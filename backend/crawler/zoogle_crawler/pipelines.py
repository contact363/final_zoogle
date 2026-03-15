"""
Scrapy item pipelines — four-stage processing.

Stage 1 – ValidationPipeline  (priority 100)
    Drop items missing machine_url or website_id.

Stage 2 – NormalizationPipeline (priority 200)
    Normalise brand / model / type strings.
    Parse price string → Decimal.

Stage 3 – ImageDownloadPipeline (priority 300)
    Download up to MAX_IMAGES per machine and store locally.
    Skips download on Render's free tier if SKIP_IMAGE_DOWNLOAD=True
    (stores URL only, no disk I/O — saves ~150 MB RAM).

Stage 4 – DatabasePipeline (priority 400)
    Upsert Machine rows into PostgreSQL via SQLAlchemy.
    Uses a shared session with batch commits (every BATCH_SIZE items)
    to cut DB round-trips and reduce connection churn.
"""

import os
import hashlib
import logging
from decimal import Decimal, InvalidOperation

import requests as _requests
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

from app.services.normalization_service import (
    normalize_brand, normalize_model, normalize_machine_type, build_content_hash
)
from app.config import settings as app_settings

logger = logging.getLogger(__name__)

# How many items to accumulate before flushing to the DB in one transaction.
# Lower = more frequent commits (safer), Higher = fewer round-trips (faster).
BATCH_SIZE = 20


# ── 1. Validation ─────────────────────────────────────────────────────────────

class ValidationPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        if not adapter.get("machine_url"):
            raise DropItem(f"Missing machine_url: {item!r}")
        if not adapter.get("website_id"):
            raise DropItem(f"Missing website_id: {item!r}")

        # Sanity: machine_url must look like a URL
        url = adapter["machine_url"]
        if not str(url).startswith("http"):
            raise DropItem(f"Invalid machine_url {url!r}")

        return item


# ── 2. Normalisation ──────────────────────────────────────────────────────────

class NormalizationPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        brand        = adapter.get("brand")
        model        = adapter.get("model")
        machine_type = adapter.get("machine_type")

        adapter["brand"]        = normalize_brand(brand)        if brand        else brand
        adapter["model"]        = normalize_model(model)        if model        else model
        adapter["machine_type"] = normalize_machine_type(machine_type) if machine_type else machine_type

        # Normalize price → Decimal (drop non-numeric chars that slipped through)
        price_raw = adapter.get("price")
        if price_raw is not None:
            try:
                price_str = (
                    str(price_raw)
                    .replace(",", "")
                    .replace(" ", "")
                    .replace("$", "")
                    .replace("€", "")
                    .replace("£", "")
                    .strip()
                )
                adapter["price"] = Decimal(price_str) if price_str else None
            except (InvalidOperation, ValueError):
                adapter["price"] = None

        return item


# ── 3. Image Download ─────────────────────────────────────────────────────────

class ImageDownloadPipeline:
    """
    Downloads machine images locally.

    Set SKIP_IMAGE_DOWNLOAD = True in spider settings (or env var
    SKIP_IMAGE_DOWNLOAD=1) to skip downloading and just store the remote URL.
    This is the recommended mode for Render's 512 MB free tier.
    """

    MAX_IMAGES = 3          # only grab the first 3 to limit disk + RAM usage
    TIMEOUT    = 8          # seconds per image request

    def open_spider(self, spider):
        skip_env = os.environ.get("SKIP_IMAGE_DOWNLOAD", "").strip().lower()
        self.skip = spider.settings.getbool("SKIP_IMAGE_DOWNLOAD", False) or skip_env in ("1", "true", "yes")
        if self.skip:
            logger.info("ImageDownloadPipeline: skipping downloads (SKIP_IMAGE_DOWNLOAD=True)")

    def process_item(self, item, spider):
        adapter     = ItemAdapter(item)
        image_urls: list = adapter.get("images") or []
        saved_paths: list = []

        if self.skip or not image_urls:
            adapter["image_paths"] = []
            return item

        website_id = adapter.get("website_id", "0")
        rel_dir    = f"machines/website_{website_id}"
        abs_dir    = os.path.join(
            getattr(app_settings, "MEDIA_DIR", "/tmp/media").replace("media/machines", "media"),
            rel_dir,
        )

        try:
            os.makedirs(abs_dir, exist_ok=True)
        except OSError as exc:
            logger.warning(f"Cannot create image dir {abs_dir}: {exc}")
            adapter["image_paths"] = []
            return item

        session = _requests.Session()
        session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"
            )
        })

        for idx, url in enumerate(image_urls[:self.MAX_IMAGES]):
            try:
                resp = session.get(url, timeout=self.TIMEOUT, stream=True)
                if resp.status_code != 200:
                    continue

                content_type = resp.headers.get("Content-Type", "")
                if "image" not in content_type and not url.lower().endswith(
                    (".jpg", ".jpeg", ".png", ".webp", ".gif")
                ):
                    continue

                ext = url.split(".")[-1].split("?")[0][:4].lower() or "jpg"
                if ext not in ("jpg", "jpeg", "png", "webp", "gif", "svg"):
                    ext = "jpg"

                hash_part = hashlib.md5(url.encode()).hexdigest()[:10]
                filename  = f"{hash_part}_{idx}.{ext}"
                abs_path  = os.path.join(abs_dir, filename)

                with open(abs_path, "wb") as f:
                    for chunk in resp.iter_content(8192):
                        f.write(chunk)

                saved_paths.append(f"{rel_dir}/{filename}")

            except Exception as exc:
                logger.debug(f"Image download failed {url}: {exc}")
                continue

        adapter["image_paths"] = saved_paths
        return item


# ── 4. Database ────────────────────────────────────────────────────────────────

class DatabasePipeline:
    """
    Upserts Machine records into PostgreSQL.

    Uses a single SQLAlchemy session per spider run with periodic batch commits
    (every BATCH_SIZE items) to reduce DB overhead.  The session is closed
    cleanly in close_spider.
    """

    def __init__(self):
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        engine = create_engine(
            app_settings.DATABASE_SYNC_URL,
            pool_pre_ping=True,
            pool_size=2,          # keep connection pool tiny
            max_overflow=1,
        )
        self.Session = sessionmaker(bind=engine)
        self._db    = None
        self._count = 0           # items processed in current batch

    def open_spider(self, spider):
        self._db    = self.Session()
        self._count = 0
        logger.info("DatabasePipeline: session opened")

    def close_spider(self, spider):
        if self._db:
            try:
                self._db.commit()
            except Exception:
                self._db.rollback()
            finally:
                self._db.close()
                self._db = None
        logger.info(f"DatabasePipeline: session closed (total={self._count})")

    def process_item(self, item, spider):
        from app.models.machine import Machine, MachineImage, MachineSpec

        adapter = ItemAdapter(item)
        db      = self._db

        try:
            brand_norm   = adapter.get("brand")
            model_norm   = adapter.get("model")
            content_hash = build_content_hash(brand_norm, model_norm, adapter["machine_url"])

            existing = db.query(Machine).filter(Machine.content_hash == content_hash).first()

            if existing:
                # Update only mutable fields on revisit
                if adapter.get("price") is not None:
                    existing.price = adapter.get("price")
                if adapter.get("description"):
                    existing.description = adapter.get("description")
                if adapter.get("location"):
                    existing.location = adapter.get("location")
                # Refresh machine_type if it was missing
                if not existing.machine_type and adapter.get("machine_type"):
                    existing.machine_type  = adapter.get("machine_type")
                    existing.type_normalized = adapter.get("machine_type")
            else:
                machine = Machine(
                    website_id       = adapter["website_id"],
                    machine_type     = adapter.get("machine_type"),
                    brand            = adapter.get("brand"),
                    model            = adapter.get("model"),
                    price            = adapter.get("price"),
                    currency         = adapter.get("currency", "USD"),
                    location         = adapter.get("location"),
                    description      = adapter.get("description"),
                    machine_url      = adapter["machine_url"],
                    website_source   = adapter.get("website_source"),
                    brand_normalized = adapter.get("brand"),
                    model_normalized = adapter.get("model"),
                    type_normalized  = adapter.get("machine_type"),
                    content_hash     = content_hash,
                )
                db.add(machine)
                db.flush()  # get machine.id without committing

                # Images
                image_paths = adapter.get("image_paths") or []
                image_urls  = adapter.get("images") or []
                padded_paths = image_paths + [None] * len(image_urls)

                for idx, url in enumerate(image_urls[:10]):
                    local = padded_paths[idx] if idx < len(padded_paths) else None
                    img = MachineImage(
                        machine_id = machine.id,
                        image_url  = url,
                        local_path = local,
                        is_primary = (idx == 0),
                    )
                    db.add(img)
                    if idx == 0:
                        machine.thumbnail_url   = url
                        machine.thumbnail_local = local

                # Specs
                for key, val in (adapter.get("specs") or {}).items():
                    db.add(MachineSpec(
                        machine_id  = machine.id,
                        spec_key    = str(key)[:100],
                        spec_value  = str(val)[:500],
                    ))

            self._count += 1

            # Batch commit to reduce round-trips
            if self._count % BATCH_SIZE == 0:
                db.commit()
                logger.debug(f"DB batch committed ({self._count} items total)")

        except Exception as exc:
            logger.error(f"DB pipeline error for {adapter.get('machine_url')}: {exc}")
            try:
                db.rollback()
            except Exception:
                # Session is broken — reopen it
                try:
                    db.close()
                except Exception:
                    pass
                self._db = self.Session()

        return item
