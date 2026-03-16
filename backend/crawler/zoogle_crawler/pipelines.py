"""
Scrapy item pipelines — six-stage processing.

Stage 1 – ValidationPipeline       (priority 100)
    Drop items missing machine_url or website_id.
    Drop items with no identity (no brand AND no model).

Stage 2 – LanguageFilterPipeline   (priority 150)  ← NEW
    Drop items whose page_lang is non-English.
    If the page declares an English hreflang canonical, redirect the
    spider to that URL instead of dropping (handled at spider level).
    This prevents duplicate German/Italian/French machine records.

Stage 3 – NormalizationPipeline    (priority 200)
    Normalise brand / model / type strings (multilingual → English).
    Extract stock_number from title + description.
    Parse price string → Decimal.

Stage 4 – ImageDownloadPipeline    (priority 300)
    Download up to MAX_IMAGES per machine and store locally.
    Skips download if SKIP_IMAGE_DOWNLOAD=True.

Stage 5 – DeduplicationPipeline    (priority 380)  ← NEW
    Pre-database duplicate check using in-memory sets to avoid hitting
    the DB for every item in a single crawl run:
      • stock_number (strongest — same stock on two language pages)
      • dedup_key = SHA-256(brand|model|stock_number)
    Drops duplicates found in the CURRENT spider run.
    Cross-run duplicates are handled in DatabasePipeline (DB lookup).

Stage 6 – DatabasePipeline         (priority 400)
    Upsert Machine rows into PostgreSQL via SQLAlchemy.
    Checks in order:
      1. dedup_key  (cross-language duplicate — update existing)
      2. content_hash (brand+model+url — same crawl re-visit — update)
      3. Insert new record
"""

import os
import re
import hashlib
import logging
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import requests as _requests
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

from app.services.normalization_service import (
    normalize_brand, normalize_model, normalize_machine_type,
    build_content_hash, build_dedup_key, extract_stock_number,
    title_similarity,
)
from app.config import settings as app_settings

logger = logging.getLogger(__name__)

BATCH_SIZE = 20


# ── 1. Validation ─────────────────────────────────────────────────────────────

class ValidationPipeline:
    """
    Drop items that are structurally invalid or clearly not machines.

    Rules:
      • machine_url required (must start with http)
      • website_id required
      • At least one identity field (brand OR model) must be present
      • images OR specs OR description must be present (content signal)
    """

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        if not adapter.get("machine_url"):
            raise DropItem(f"Missing machine_url: {adapter.get('model')!r}")

        url = adapter["machine_url"]
        if not str(url).startswith("http"):
            raise DropItem(f"Invalid machine_url {url!r}")

        if not adapter.get("website_id"):
            raise DropItem(f"Missing website_id for {url!r}")

        # Must have at least one identity field
        has_identity = bool(adapter.get("brand") or adapter.get("model"))
        if not has_identity:
            raise DropItem(f"No brand/model for {url!r} — likely not a machine page")

        # Must have at least one content signal
        has_content = bool(
            (adapter.get("images") and len(adapter["images"]) > 0)
            or (adapter.get("specs") and len(adapter["specs"]) > 0)
            or adapter.get("description")
            or adapter.get("price")
        )
        if not has_content:
            raise DropItem(f"No content (images/specs/price/desc) for {url!r}")

        return item


# ── 2. Language Filter ────────────────────────────────────────────────────────

class LanguageFilterPipeline:
    """
    Drop non-English machine pages.

    Many industrial machine sites publish every listing in German, Italian,
    French and English.  We keep ONLY the English version to avoid inserting
    duplicate records in different languages.

    Detection uses the page_lang field set by the spider (from lang_detector).
    If no language signal was detected ("unknown") the item passes through —
    we assume English by default.

    Items in non-English languages that DO have an English hreflang canonical
    are dropped here too — the spider is expected to have already scheduled
    the canonical English URL when it detected the hreflang.
    """

    # Languages we accept (English codes + "unknown" = assume English)
    ALLOWED = frozenset({"en", "unknown", None, ""})

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        lang = (adapter.get("page_lang") or "").lower().strip()

        if lang and lang not in self.ALLOWED:
            url = adapter.get("machine_url", "?")
            raise DropItem(
                f"Non-English page dropped (lang={lang!r}) url={url!r}"
            )

        return item


# ── 3. Normalisation ──────────────────────────────────────────────────────────

class NormalizationPipeline:
    """
    Normalise all text fields to clean, canonical English values.

      • Brand → canonical name via BRAND_ALIASES
      • Model → uppercase, normalised punctuation
      • machine_type → canonical English via MULTILANG_TYPE_MAP + TYPE_SYNONYMS
      • stock_number → extracted from title/description if not set by spider
      • price → Decimal (strips currency symbols, commas, spaces)
    """

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        brand        = adapter.get("brand")
        model        = adapter.get("model")
        machine_type = adapter.get("machine_type")

        adapter["brand"]        = normalize_brand(brand)        if brand        else brand
        adapter["model"]        = normalize_model(model)        if model        else model
        adapter["machine_type"] = normalize_machine_type(machine_type) if machine_type else machine_type

        # Extract stock number if not already set
        if not adapter.get("stock_number"):
            title = (adapter.get("model") or "") + " " + (adapter.get("description") or "")
            sn    = extract_stock_number(title)
            if sn:
                adapter["stock_number"] = sn

        # Normalize stock_number to UPPERCASE, strip whitespace
        sn = adapter.get("stock_number")
        if sn:
            adapter["stock_number"] = sn.strip().upper()

        # Normalize price → Decimal
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
                    .replace("¥", "")
                    .replace("₹", "")
                    .strip()
                )
                adapter["price"] = Decimal(price_str) if price_str else None
            except (InvalidOperation, ValueError):
                adapter["price"] = None

        return item


# ── 4. Image Download ─────────────────────────────────────────────────────────

class ImageDownloadPipeline:
    """
    Downloads machine images locally.

    Set SKIP_IMAGE_DOWNLOAD = True in spider settings (or env var
    SKIP_IMAGE_DOWNLOAD=1) to skip downloading and just store the remote URL.
    Recommended for Render's 512 MB free tier.
    """

    MAX_IMAGES = 3
    TIMEOUT    = 8

    def open_spider(self, spider):
        skip_env  = os.environ.get("SKIP_IMAGE_DOWNLOAD", "").strip().lower()
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


# ── 5. Deduplication ─────────────────────────────────────────────────────────

class DeduplicationPipeline:
    """
    In-memory duplicate detection for the CURRENT spider run.

    This runs BEFORE DatabasePipeline so we avoid unnecessary DB round-trips
    for duplicates discovered within a single crawl (e.g. a site that lists
    the same machine in /en/, /de/, /it/).

    Uses two keys:
      • dedup_key  = SHA-256(normalized_brand | normalized_model | stock_number)
                     Catches cross-language duplicates sharing the same stock number.
      • content_hash = SHA-256(brand | model | url)
                     Catches exact revisits of the same URL with same brand/model.

    Both sets are reset when a new spider starts (open_spider).
    """

    def open_spider(self, spider):
        self._seen_dedup_keys:    set[str] = set()
        self._seen_content_hashes: set[str] = set()
        self._stats = {"passed": 0, "dedup_key": 0, "content_hash": 0}

    def close_spider(self, spider):
        logger.info(
            f"DeduplicationPipeline: passed={self._stats['passed']} "
            f"dropped_dedup_key={self._stats['dedup_key']} "
            f"dropped_content_hash={self._stats['content_hash']}"
        )

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        brand  = adapter.get("brand")
        model  = adapter.get("model")
        sn     = adapter.get("stock_number")
        url    = adapter.get("machine_url", "")

        # ── dedup_key (cross-language, stock-number aware) ────────────────────
        if brand or model:
            dk = build_dedup_key(brand, model, sn)
            if dk in self._seen_dedup_keys:
                self._stats["dedup_key"] += 1
                raise DropItem(
                    f"Dedup (dedup_key) — same brand/model/stock as earlier item: "
                    f"{brand!r} {model!r} SN={sn!r} url={url!r}"
                )
            self._seen_dedup_keys.add(dk)

        # ── content_hash (exact URL + brand + model revisit) ─────────────────
        ch = build_content_hash(brand, model, url)
        if ch in self._seen_content_hashes:
            self._stats["content_hash"] += 1
            raise DropItem(
                f"Dedup (content_hash) — exact revisit: {url!r}"
            )
        self._seen_content_hashes.add(ch)

        self._stats["passed"] += 1
        # Store both keys on adapter so DatabasePipeline can use them without recomputing
        adapter["_dedup_key"]    = dk if (brand or model) else None
        adapter["_content_hash"] = ch

        return item


# ── 6. Database ────────────────────────────────────────────────────────────────

class DatabasePipeline:
    """
    Upserts Machine records into PostgreSQL.

    Duplicate-check order (for items that passed DeduplicationPipeline):
      1. dedup_key match  → cross-language duplicate → UPDATE existing record
      2. content_hash match → same URL re-crawled   → UPDATE existing record
      3. stock_number match (same website) → update
      4. No match → INSERT new record

    Also saves MachineImage and MachineSpec child rows on new inserts.
    """

    def __init__(self):
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        engine = create_engine(
            app_settings.DATABASE_SYNC_URL,
            pool_pre_ping=True,
            pool_size=2,
            max_overflow=1,
        )
        self.Session = sessionmaker(bind=engine)
        self._db     = None
        self._new    = 0
        self._updated = 0

    def open_spider(self, spider):
        self._db      = self.Session()
        self._new     = 0
        self._updated = 0
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
        logger.info(
            f"DatabasePipeline: closed — new={self._new} updated={self._updated}"
        )

    def process_item(self, item, spider):
        from app.models.machine import Machine, MachineImage, MachineSpec

        adapter = ItemAdapter(item)
        db      = self._db

        try:
            brand  = adapter.get("brand")
            model  = adapter.get("model")
            sn     = adapter.get("stock_number")
            url    = adapter["machine_url"]

            dedup_key    = adapter.get("_dedup_key")    or build_dedup_key(brand, model, sn)
            content_hash = adapter.get("_content_hash") or build_content_hash(brand, model, url)

            existing = None

            # ── 1. Cross-language dedup (dedup_key) ───────────────────────────
            if dedup_key:
                existing = db.query(Machine).filter(Machine.dedup_key == dedup_key).first()

            # ── 2. Same-URL revisit (content_hash) ────────────────────────────
            if not existing:
                existing = db.query(Machine).filter(Machine.content_hash == content_hash).first()

            # ── 3. Stock-number match on same website ─────────────────────────
            if not existing and sn and adapter.get("website_id"):
                existing = (
                    db.query(Machine)
                    .filter(
                        Machine.stock_number == sn,
                        Machine.website_id   == adapter["website_id"],
                    )
                    .first()
                )

            now = datetime.now(timezone.utc)

            # ── Update ────────────────────────────────────────────────────────
            if existing:
                changed = False
                if adapter.get("price") is not None:
                    existing.price = adapter.get("price"); changed = True
                if adapter.get("description"):
                    existing.description = adapter.get("description"); changed = True
                if adapter.get("location"):
                    existing.location = adapter.get("location"); changed = True
                if not existing.machine_type and adapter.get("machine_type"):
                    existing.machine_type  = adapter.get("machine_type")
                    existing.type_normalized = adapter.get("machine_type")
                    changed = True
                if not existing.stock_number and sn:
                    existing.stock_number = sn; changed = True
                # Ensure dedup_key is set even if it wasn't previously stored
                if not existing.dedup_key and dedup_key:
                    existing.dedup_key = dedup_key; changed = True
                # Always mark as seen and active in this crawl
                existing.last_crawled_at = now
                existing.is_active = True
                if changed:
                    self._updated += 1

            # ── Insert ────────────────────────────────────────────────────────
            else:
                machine = Machine(
                    website_id       = adapter["website_id"],
                    machine_type     = adapter.get("machine_type"),
                    brand            = adapter.get("brand"),
                    model            = adapter.get("model"),
                    stock_number     = sn,
                    price            = adapter.get("price"),
                    currency         = adapter.get("currency", "USD"),
                    location         = adapter.get("location"),
                    description      = adapter.get("description"),
                    machine_url      = url,
                    website_source   = adapter.get("website_source"),
                    brand_normalized = adapter.get("brand"),
                    model_normalized = adapter.get("model"),
                    type_normalized  = adapter.get("machine_type"),
                    content_hash     = content_hash,
                    dedup_key        = dedup_key,
                    last_crawled_at  = now,
                )
                db.add(machine)
                db.flush()

                # Images
                image_paths  = adapter.get("image_paths") or []
                image_urls   = adapter.get("images") or []
                padded_paths = image_paths + [None] * len(image_urls)

                for idx, img_url in enumerate(image_urls[:10]):
                    local = padded_paths[idx] if idx < len(padded_paths) else None
                    db.add(MachineImage(
                        machine_id = machine.id,
                        image_url  = img_url,
                        local_path = local,
                        is_primary = (idx == 0),
                    ))
                    if idx == 0:
                        machine.thumbnail_url   = img_url
                        machine.thumbnail_local = local

                # Specs
                for key, val in (adapter.get("specs") or {}).items():
                    db.add(MachineSpec(
                        machine_id = machine.id,
                        spec_key   = str(key)[:100],
                        spec_value = str(val)[:500],
                    ))

                self._new += 1

            db.commit()

        except DropItem:
            raise
        except Exception as exc:
            logger.error(f"DB pipeline error for {adapter.get('machine_url')}: {exc}")
            try:
                db.rollback()
            except Exception:
                try:
                    db.close()
                except Exception:
                    pass
                self._db = self.Session()

        return item
