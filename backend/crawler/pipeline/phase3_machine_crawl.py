"""
Phase 3 — Machine Crawling

Runs the MachineSpider as a subprocess (scrapy crawl machine_spider).
The spider reads product URLs from the Redis queue, extracts machine data,
and stores it in PostgreSQL via the storage pipeline.

Also handles the API path — when Phase 1 found a direct API, we skip
Scrapy entirely and store machines directly from the API response.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import psycopg2

from crawler.extractors.api_extractor import (
    APIConfig,
    fetch_all_machines,
    normalize_api_item,
)

logger = logging.getLogger(__name__)


@dataclass
class CrawlResult:
    machines_new: int = 0
    machines_updated: int = 0
    machines_skipped: int = 0
    errors: int = 0


# ── API path ──────────────────────────────────────────────────────────────────

def run_api_crawl(
    website_id: int,
    api_config: APIConfig,
    db_sync_url: str,
) -> CrawlResult:
    """
    Directly fetch machines from an API and store them in PostgreSQL.
    No Scrapy needed for this path.
    """
    import hashlib
    import re
    from datetime import datetime, timezone
    from urllib.parse import urlparse
    from unidecode import unidecode

    def _norm(t):
        return re.sub(r"[^a-z0-9]", "", unidecode(str(t or "")).lower())

    def _chash(brand, model, url):
        return hashlib.sha256(f"{_norm(brand)}|{_norm(model)}|{url}".encode()).hexdigest()

    def _dkey(brand, model, stock):
        return hashlib.sha256(f"{_norm(brand)}|{_norm(model)}|{_norm(stock)}".encode()).hexdigest()

    result = CrawlResult()
    conn = psycopg2.connect(db_sync_url)
    conn.autocommit = False
    now = datetime.now(timezone.utc)

    try:
        cur = conn.cursor()
        batch = []

        for raw in fetch_all_machines(api_config):
            item = normalize_api_item(raw, api_config.field_map, api_config.base_url)
            brand  = str(item.get("brand") or "")
            model  = str(item.get("model") or item.get("machine_name") or "")
            stock  = str(item.get("stock_number") or "")
            src    = str(item.get("source_url") or "")
            price  = item.get("price")
            desc   = str(item.get("description") or "")[:5000]
            images = item.get("images") or []

            if not model and not brand:
                result.machines_skipped += 1
                continue

            c_hash = _chash(brand, model, src)
            d_key  = _dkey(brand, model, stock)

            try:
                cur.execute("SELECT id FROM machines WHERE content_hash=%s", (c_hash,))
                existing = cur.fetchone()
                if existing:
                    cur.execute(
                        "UPDATE machines SET last_crawled_at=%s, is_active=TRUE WHERE id=%s",
                        (now, existing[0]),
                    )
                    result.machines_updated += 1
                else:
                    cur.execute(
                        """
                        INSERT INTO machines (
                            website_id, brand, model, stock_number, price, currency,
                            description, machine_url, website_source,
                            brand_normalized, model_normalized,
                            content_hash, dedup_key,
                            thumbnail_url, is_active, last_crawled_at
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,TRUE,%s)
                        ON CONFLICT (content_hash) DO UPDATE SET
                            last_crawled_at=EXCLUDED.last_crawled_at, is_active=TRUE
                        RETURNING id
                        """,
                        (
                            website_id, brand, model, stock,
                            price, item.get("currency") or "USD",
                            desc, src,
                            src[:50] if src else "",
                            _norm(brand), _norm(model),
                            c_hash, d_key,
                            images[0] if images else None,
                            now,
                        ),
                    )
                    row = cur.fetchone()
                    if row:
                        mid = row[0]
                        for idx, img_url in enumerate(images[:5]):
                            if img_url:
                                cur.execute(
                                    """INSERT INTO machine_images (machine_id, image_url, is_primary)
                                       VALUES (%s,%s,%s) ON CONFLICT DO NOTHING""",
                                    (mid, img_url, idx == 0),
                                )
                        specs = item.get("specifications") or {}
                        for k, v in list(specs.items())[:50]:
                            if k and v:
                                cur.execute(
                                    """INSERT INTO machine_specs (machine_id, spec_key, spec_value)
                                       VALUES (%s,%s,%s) ON CONFLICT DO NOTHING""",
                                    (mid, str(k)[:100], str(v)[:500]),
                                )
                    result.machines_new += 1

                # Batch commit every 500 rows
                total = result.machines_new + result.machines_updated
                if total % 500 == 0:
                    conn.commit()

            except Exception as exc:
                conn.rollback()
                result.machines_skipped += 1
                result.errors += 1
                logger.warning("API insert error: %s", exc)

        conn.commit()

        # Update website machine count
        cur.execute(
            "UPDATE websites SET machine_count=(SELECT COUNT(*) FROM machines WHERE website_id=%s AND is_active=TRUE) WHERE id=%s",
            (website_id, website_id),
        )
        conn.commit()
        cur.close()

    finally:
        conn.close()

    logger.info(
        "[Phase3/API] website_id=%d new=%d updated=%d skipped=%d",
        website_id, result.machines_new, result.machines_updated, result.machines_skipped,
    )
    return result


# ── HTML/Scrapy path ──────────────────────────────────────────────────────────

def run_machine_crawl(
    website_id: int,
    redis_url: str,
    db_sync_url: str,
    backend_dir: str,
    request_delay: float = 1.0,
    timeout: int = 7200,
) -> CrawlResult:
    """
    Launch the machine_spider subprocess to drain the Redis URL queue
    and store machines in PostgreSQL.
    """
    # FIX 3: Use tempfile.gettempdir() — works on Windows AND Linux/Mac
    _tmp = tempfile.gettempdir()
    stats_file = str(Path(_tmp) / f"pipeline_stats_{website_id}.json")
    try:
        os.remove(stats_file)
    except FileNotFoundError:
        pass

    cmd = [
        sys.executable, "-m", "scrapy", "crawl", "machine_spider",
        "-a", f"website_id={website_id}",
        "-a", f"redis_url={redis_url}",
        "-a", f"request_delay={request_delay}",
        "-s", f"DATABASE_SYNC_URL={db_sync_url}",
        "--logfile", str(Path(_tmp) / f"machine_spider_{website_id}.log"),
        "--loglevel", "INFO",
    ]

    logger.info("[Phase3] Starting machine_spider for website_id=%d", website_id)

    try:
        proc = subprocess.run(
            cmd,
            cwd=backend_dir,
            timeout=timeout,
            capture_output=False,
            env={**os.environ,
                 "PYTHONPATH": backend_dir,
                 "DATABASE_SYNC_URL": db_sync_url},
        )
        if proc.returncode != 0:
            logger.error(
                "[Phase3] machine_spider exited with code %d for website_id=%d",
                proc.returncode, website_id,
            )
    except subprocess.TimeoutExpired:
        logger.error("[Phase3] machine_spider timed out for website_id=%d", website_id)
    except Exception as exc:
        logger.error("[Phase3] machine_spider error: %s", exc)

    # Read pipeline stats
    try:
        with open(stats_file, "r") as f:
            stats = json.load(f)
        return CrawlResult(
            machines_new=stats.get("new", 0),
            machines_updated=stats.get("updated", 0),
            machines_skipped=stats.get("skipped", 0),
        )
    except Exception:
        logger.warning("[Phase3] Could not read pipeline stats for website_id=%d", website_id)
        return CrawlResult()


# ── Playwright / lightweight_crawler path ─────────────────────────────────────

def run_lightweight_crawl(
    website_id: int,
    website_url: str,
    db_sync_url: str,
    max_requests: int = 500,
    request_delay: float = 0.5,
) -> CrawlResult:
    """
    Full Playwright-aware crawl using lightweight_crawler.
    Used when the site is JS-rendered and Scrapy cannot find any URLs.

    Calls lightweight_crawler.crawl() which handles:
      • Playwright rendering for React/Vue/Angular/Next.js
      • Shopify / WooCommerce / GraphQL API fast-paths
      • Sitemap parsing
      • Pagination following
      • Full machine data extraction

    Stores results directly to PostgreSQL (same schema as run_api_crawl).
    """
    import hashlib
    import re
    from datetime import datetime, timezone
    from unidecode import unidecode

    def _norm(t):
        return re.sub(r"[^a-z0-9]", "", unidecode(str(t or "")).lower())

    def _chash(brand, model, url):
        return hashlib.sha256(f"{_norm(brand)}|{_norm(model)}|{url}".encode()).hexdigest()

    def _dkey(brand, model, stock):
        return hashlib.sha256(f"{_norm(brand)}|{_norm(model)}|{_norm(stock)}".encode()).hexdigest()

    logger.info(
        "[Phase3/LW] Starting lightweight_crawler for website_id=%d url=%s",
        website_id, website_url,
    )

    # ── Run the full crawler ───────────────────────────────────────────────
    try:
        from crawler.lightweight_crawler import crawl as lw_crawl
        machines = lw_crawl(
            website_url,
            max_requests=max_requests,
            request_delay=request_delay,
            use_playwright=True,
        )
    except Exception as exc:
        logger.error("[Phase3/LW] lightweight_crawler error: %s", exc)
        return CrawlResult(errors=1)

    if not machines:
        logger.warning("[Phase3/LW] No machines found for website_id=%d", website_id)
        return CrawlResult()

    logger.info("[Phase3/LW] Extracted %d machines — storing to DB", len(machines))

    # ── Store results to PostgreSQL ────────────────────────────────────────
    result = CrawlResult()
    conn = psycopg2.connect(db_sync_url)
    conn.autocommit = False
    now = datetime.now(timezone.utc)

    try:
        cur = conn.cursor()

        for item in machines:
            brand   = str(item.get("brand") or "")
            model   = str(item.get("model") or item.get("machine_name") or "")
            stock   = str(item.get("stock_number") or "")
            src     = str(item.get("source_url") or "")
            price   = item.get("price")
            desc    = str(item.get("description") or "")[:5000]
            images  = item.get("images") or []
            specs   = item.get("specifications") or {}
            name    = str(item.get("machine_name") or model or brand)
            mtype   = str(item.get("machine_type") or "")
            year    = item.get("year")
            cond    = str(item.get("condition") or "")
            currency = str(item.get("currency") or "USD")

            if not model and not brand:
                result.machines_skipped += 1
                continue

            c_hash = _chash(brand, model, src)
            d_key  = _dkey(brand, model, stock)

            try:
                cur.execute("SELECT id FROM machines WHERE content_hash=%s", (c_hash,))
                existing = cur.fetchone()

                if existing:
                    cur.execute(
                        "UPDATE machines SET last_crawled_at=%s, is_active=TRUE WHERE id=%s",
                        (now, existing[0]),
                    )
                    result.machines_updated += 1
                else:
                    cur.execute(
                        """
                        INSERT INTO machines (
                            website_id, machine_name, brand, model, machine_type,
                            condition, year, stock_number, price, currency,
                            description, machine_url, website_source,
                            brand_normalized, model_normalized,
                            content_hash, dedup_key,
                            thumbnail_url, is_active, last_crawled_at
                        ) VALUES (
                            %s,%s,%s,%s,%s,
                            %s,%s,%s,%s,%s,
                            %s,%s,%s,
                            %s,%s,
                            %s,%s,
                            %s,TRUE,%s
                        )
                        ON CONFLICT (content_hash) DO UPDATE SET
                            last_crawled_at=EXCLUDED.last_crawled_at, is_active=TRUE
                        RETURNING id
                        """,
                        (
                            website_id, name, brand, model, mtype,
                            cond, year, stock, price, currency,
                            desc, src, src[:50] if src else "",
                            _norm(brand), _norm(model),
                            c_hash, d_key,
                            images[0] if images else None,
                            now,
                        ),
                    )
                    row = cur.fetchone()
                    if row:
                        mid = row[0]
                        for idx, img_url in enumerate(images[:5]):
                            if img_url:
                                cur.execute(
                                    """INSERT INTO machine_images (machine_id, image_url, is_primary)
                                       VALUES (%s,%s,%s) ON CONFLICT DO NOTHING""",
                                    (mid, img_url, idx == 0),
                                )
                        for k, v in list(specs.items())[:50]:
                            if k and v:
                                cur.execute(
                                    """INSERT INTO machine_specs (machine_id, spec_key, spec_value)
                                       VALUES (%s,%s,%s) ON CONFLICT DO NOTHING""",
                                    (mid, str(k)[:100], str(v)[:500]),
                                )
                    result.machines_new += 1

                # Batch commit every 200 rows
                total = result.machines_new + result.machines_updated
                if total % 200 == 0:
                    conn.commit()

            except Exception as exc:
                conn.rollback()
                result.machines_skipped += 1
                result.errors += 1
                logger.warning("[Phase3/LW] DB insert error: %s", exc)

        conn.commit()

        # Update website machine count
        cur.execute(
            "UPDATE websites SET machine_count="
            "(SELECT COUNT(*) FROM machines WHERE website_id=%s AND is_active=TRUE) "
            "WHERE id=%s",
            (website_id, website_id),
        )
        conn.commit()
        cur.close()

    except Exception as exc:
        logger.error("[Phase3/LW] DB error for website_id=%d: %s", website_id, exc)
        result.errors += 1
    finally:
        conn.close()

    logger.info(
        "[Phase3/LW] website_id=%d new=%d updated=%d skipped=%d errors=%d",
        website_id, result.machines_new, result.machines_updated,
        result.machines_skipped, result.errors,
    )
    return result
