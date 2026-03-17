"""
PostgreSQL Storage Pipeline — runs inside the Scrapy subprocess.

Uses psycopg2 (synchronous) to store MachineItems.
Handles deduplication via content_hash and dedup_key.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse
from unidecode import unidecode
import re

import psycopg2
import psycopg2.extras
from scrapy import signals
from scrapy.exceptions import DropItem

from crawler.items import MachineItem

logger = logging.getLogger(__name__)


def _normalize(text: str) -> str:
    """Lowercase + ASCII-fold + strip punctuation for dedup key generation."""
    if not text:
        return ""
    return re.sub(r"[^a-z0-9]", "", unidecode(str(text)).lower())


def _dedup_key(brand: str, model: str, stock_number: str) -> str:
    raw = f"{_normalize(brand)}|{_normalize(model)}|{_normalize(stock_number)}"
    return hashlib.sha256(raw.encode()).hexdigest()


def _content_hash(brand: str, model: str, url: str) -> str:
    raw = f"{_normalize(brand)}|{_normalize(model)}|{url}"
    return hashlib.sha256(raw.encode()).hexdigest()


class PostgreSQLPipeline:
    """Stores MachineItem objects to PostgreSQL with upsert logic."""

    def __init__(self, db_url: str):
        self._db_url = db_url
        self._conn: Optional[psycopg2.extensions.connection] = None
        self._machines_new = 0
        self._machines_updated = 0
        self._machines_skipped = 0

    @classmethod
    def from_crawler(cls, crawler):
        db_url = (
            crawler.settings.get("DATABASE_SYNC_URL")
            or os.environ.get("DATABASE_SYNC_URL")
            or os.environ.get("DATABASE_URL", "").replace(
                "postgresql+asyncpg://", "postgresql://"
            )
        )
        return cls(db_url=db_url)

    def open_spider(self, spider):
        self._conn = psycopg2.connect(self._db_url)
        self._conn.autocommit = False
        logger.info("PostgreSQLPipeline: connected to database")

    def close_spider(self, spider):
        if self._conn:
            try:
                self._conn.commit()
            except Exception:
                pass
            self._conn.close()
        logger.info(
            "PostgreSQLPipeline closed. new=%d updated=%d skipped=%d",
            self._machines_new,
            self._machines_updated,
            self._machines_skipped,
        )
        # Write stats for task to read
        stats_file = f"/tmp/pipeline_stats_{spider.website_id}.json"
        try:
            with open(stats_file, "w") as f:
                json.dump({
                    "new": self._machines_new,
                    "updated": self._machines_updated,
                    "skipped": self._machines_skipped,
                }, f)
        except Exception:
            pass

    def process_item(self, item: MachineItem, spider):
        if not isinstance(item, MachineItem):
            return item

        # Require at least a URL
        source_url = item.get("source_url") or ""
        if not source_url:
            raise DropItem("Missing source_url")

        brand        = str(item.get("brand") or "")
        model        = str(item.get("model") or "")
        machine_name = str(item.get("machine_name") or "")
        stock_number = str(item.get("stock_number") or "")
        website_id   = int(item.get("website_id") or 0)

        # Use machine_name as fallback for model
        if not model and machine_name:
            model = machine_name

        c_hash   = item.get("content_hash") or _content_hash(brand, model, source_url)
        d_key    = _dedup_key(brand, model, stock_number)
        now      = datetime.now(timezone.utc)

        try:
            with self._conn.cursor() as cur:
                # Check for existing machine by content_hash
                cur.execute(
                    "SELECT id FROM machines WHERE content_hash = %s",
                    (c_hash,),
                )
                existing = cur.fetchone()

                if existing:
                    # Update last_crawled_at and is_active
                    cur.execute(
                        """
                        UPDATE machines
                        SET last_crawled_at = %s,
                            is_active = TRUE,
                            price = COALESCE(%s, price),
                            description = COALESCE(NULLIF(%s, ''), description)
                        WHERE id = %s
                        """,
                        (now, item.get("price"), item.get("description") or "", existing[0]),
                    )
                    self._machines_updated += 1
                else:
                    # Insert new machine
                    cur.execute(
                        """
                        INSERT INTO machines (
                            website_id, machine_type, brand, model,
                            stock_number, price, currency, description,
                            machine_url, website_source,
                            brand_normalized, model_normalized, type_normalized,
                            content_hash, dedup_key,
                            thumbnail_url, is_active, last_crawled_at
                        ) VALUES (
                            %s, %s, %s, %s,
                            %s, %s, %s, %s,
                            %s, %s,
                            %s, %s, %s,
                            %s, %s,
                            %s, TRUE, %s
                        )
                        ON CONFLICT (content_hash) DO UPDATE SET
                            last_crawled_at = EXCLUDED.last_crawled_at,
                            is_active = TRUE
                        RETURNING id
                        """,
                        (
                            website_id,
                            item.get("machine_type") or "",
                            brand,
                            model,
                            stock_number,
                            item.get("price"),
                            item.get("currency") or "USD",
                            (item.get("description") or "")[:5000],
                            source_url,
                            urlparse(source_url).netloc,
                            _normalize(brand),
                            _normalize(model),
                            _normalize(item.get("machine_type") or ""),
                            c_hash,
                            d_key,
                            (item.get("images") or [None])[0],
                            now,
                        ),
                    )
                    row = cur.fetchone()
                    machine_id = row[0] if row else None
                    self._machines_new += 1

                    if machine_id:
                        # Insert images
                        images = item.get("images") or []
                        for idx, img_url in enumerate(images[:5]):
                            if img_url:
                                cur.execute(
                                    """
                                    INSERT INTO machine_images (machine_id, image_url, is_primary)
                                    VALUES (%s, %s, %s)
                                    ON CONFLICT DO NOTHING
                                    """,
                                    (machine_id, img_url, idx == 0),
                                )

                        # Insert specs
                        specs = item.get("specifications") or {}
                        for spec_key, spec_value in list(specs.items())[:50]:
                            if spec_key and spec_value:
                                cur.execute(
                                    """
                                    INSERT INTO machine_specs (machine_id, spec_key, spec_value)
                                    VALUES (%s, %s, %s)
                                    ON CONFLICT DO NOTHING
                                    """,
                                    (machine_id, str(spec_key)[:100], str(spec_value)[:500]),
                                )

            # Commit every 100 items for safety
            total = self._machines_new + self._machines_updated
            if total % 100 == 0:
                self._conn.commit()
                # Update website machine_count
                self._update_website_count(website_id)

        except Exception as exc:
            self._conn.rollback()
            self._machines_skipped += 1
            logger.error("DB error for %s: %s", source_url, exc)
            raise DropItem(f"DB error: {exc}")

        return item

    def _update_website_count(self, website_id: int) -> None:
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE websites
                    SET machine_count = (
                        SELECT COUNT(*) FROM machines
                        WHERE website_id = %s AND is_active = TRUE
                    )
                    WHERE id = %s
                    """,
                    (website_id, website_id),
                )
            self._conn.commit()
        except Exception as exc:
            logger.warning("Failed to update machine_count: %s", exc)
            self._conn.rollback()
