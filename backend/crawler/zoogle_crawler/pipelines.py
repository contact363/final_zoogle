"""
Scrapy item pipelines:

  1. ValidationPipeline  – drop items missing required fields
  2. NormalizationPipeline – clean/normalize brand, model, type
  3. ImageDownloadPipeline – download machine images locally
  4. DatabasePipeline – upsert into PostgreSQL
"""
import os
import hashlib
import requests
from decimal import Decimal, InvalidOperation
from urllib.parse import urljoin

from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

from app.services.normalization_service import (
    normalize_brand, normalize_model, normalize_machine_type, build_content_hash
)
from app.config import settings


# ── 1. Validation ─────────────────────────────────────────────────────────────

class ValidationPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        if not adapter.get("machine_url"):
            raise DropItem(f"Missing machine_url in {item!r}")
        if not adapter.get("website_id"):
            raise DropItem(f"Missing website_id in {item!r}")
        return item


# ── 2. Normalization ──────────────────────────────────────────────────────────

class NormalizationPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        brand = adapter.get("brand")
        model = adapter.get("model")
        machine_type = adapter.get("machine_type")

        adapter["brand"] = normalize_brand(brand) if brand else brand
        adapter["model"] = normalize_model(model) if model else model
        adapter["machine_type"] = normalize_machine_type(machine_type) if machine_type else machine_type

        # Normalize price
        price_raw = adapter.get("price")
        if price_raw:
            try:
                price_str = str(price_raw).replace(",", "").replace(" ", "").replace("$", "").replace("€", "").replace("£", "")
                adapter["price"] = Decimal(price_str)
            except (InvalidOperation, ValueError):
                adapter["price"] = None

        return item


# ── 3. Image Download ─────────────────────────────────────────────────────────

class ImageDownloadPipeline:
    MAX_IMAGES = 5

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        image_urls: list = adapter.get("images") or []
        saved_paths = []

        for idx, url in enumerate(image_urls[:self.MAX_IMAGES]):
            try:
                resp = requests.get(url, timeout=10, stream=True)
                if resp.status_code != 200:
                    continue

                ext = url.split(".")[-1].split("?")[0][:4] or "jpg"
                hash_part = hashlib.md5(url.encode()).hexdigest()[:8]
                rel_dir = f"machines/website_{adapter.get('website_id')}"
                abs_dir = os.path.join(settings.MEDIA_DIR.replace("media/machines", "media"), rel_dir)
                os.makedirs(abs_dir, exist_ok=True)

                filename = f"{hash_part}_{idx}.{ext}"
                abs_path = os.path.join(abs_dir, filename)

                with open(abs_path, "wb") as f:
                    for chunk in resp.iter_content(8192):
                        f.write(chunk)

                saved_paths.append(f"{rel_dir}/{filename}")
            except Exception as e:
                spider.logger.warning(f"Image download failed {url}: {e}")

        adapter["image_paths"] = saved_paths
        return item


# ── 4. Database ───────────────────────────────────────────────────────────────

class DatabasePipeline:
    def __init__(self):
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        engine = create_engine(settings.DATABASE_SYNC_URL, pool_pre_ping=True)
        self.Session = sessionmaker(bind=engine)

    def process_item(self, item, spider):
        from app.models.machine import Machine, MachineImage, MachineSpec
        from app.services.normalization_service import build_content_hash

        adapter = ItemAdapter(item)
        db = self.Session()

        try:
            brand_norm = adapter.get("brand")
            model_norm = adapter.get("model")
            content_hash = build_content_hash(
                brand_norm, model_norm, adapter["machine_url"]
            )

            existing = db.query(Machine).filter(Machine.content_hash == content_hash).first()

            if existing:
                # Update mutable fields
                existing.price = adapter.get("price")
                existing.description = adapter.get("description")
                existing.location = adapter.get("location")
                db.commit()
                return item

            machine = Machine(
                website_id=adapter["website_id"],
                machine_type=adapter.get("machine_type"),
                brand=adapter.get("brand"),
                model=adapter.get("model"),
                price=adapter.get("price"),
                currency=adapter.get("currency", "USD"),
                location=adapter.get("location"),
                description=adapter.get("description"),
                machine_url=adapter["machine_url"],
                website_source=adapter.get("website_source"),
                brand_normalized=adapter.get("brand"),
                model_normalized=adapter.get("model"),
                type_normalized=adapter.get("machine_type"),
                content_hash=content_hash,
            )
            db.add(machine)
            db.flush()

            # Images
            image_paths = adapter.get("image_paths") or []
            image_urls = adapter.get("images") or []
            for idx, (url, local_path) in enumerate(zip(image_urls, image_paths + [None] * len(image_urls))):
                img = MachineImage(
                    machine_id=machine.id,
                    image_url=url,
                    local_path=local_path,
                    is_primary=(idx == 0),
                )
                db.add(img)
                if idx == 0:
                    machine.thumbnail_url = url
                    machine.thumbnail_local = local_path

            # Specs
            for key, value in (adapter.get("specs") or {}).items():
                spec = MachineSpec(
                    machine_id=machine.id,
                    spec_key=str(key),
                    spec_value=str(value),
                )
                db.add(spec)

            db.commit()

        except Exception as e:
            db.rollback()
            spider.logger.error(f"DB pipeline error: {e}")
        finally:
            db.close()

        return item
