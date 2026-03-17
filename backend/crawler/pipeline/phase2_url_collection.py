"""
Phase 2 — URL Collection

Runs the UrlCollectorSpider as a subprocess (scrapy crawl url_collector).
The spider crawls category/listing pages and pushes all product URLs
into the Redis queue.

Returns the total number of product URLs collected.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import tempfile
from typing import List, Optional

logger = logging.getLogger(__name__)


def run_url_collection(
    website_id: int,
    category_urls: List[str],
    redis_url: str,
    backend_dir: str,
    product_link_pattern: str = "",
    request_delay: float = 1.0,
    timeout: int = 3600,
) -> int:
    """
    Launch the url_collector Scrapy spider and wait for it to finish.
    Returns the count of URLs pushed to Redis.

    category_urls  — listing/category pages to crawl
    redis_url      — Redis URL for the URLQueue
    backend_dir    — path to the backend/ directory (where scrapy.cfg lives)
    """
    if not category_urls:
        logger.warning("[Phase2] No category URLs for website_id=%d", website_id)
        return 0

    # Write start URLs to a temp file
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".txt", delete=False, encoding="utf-8"
    ) as f:
        f.write("\n".join(category_urls))
        urls_file = f.name

    result_file = f"/tmp/url_count_{website_id}.txt"
    # Clean up previous result
    try:
        os.remove(result_file)
    except FileNotFoundError:
        pass

    cmd = [
        sys.executable, "-m", "scrapy", "crawl", "url_collector",
        "-a", f"website_id={website_id}",
        "-a", f"start_urls_file={urls_file}",
        "-a", f"redis_url={redis_url}",
        "-a", f"product_link_pattern={product_link_pattern}",
        "-a", f"request_delay={request_delay}",
        "--logfile", f"/tmp/url_collector_{website_id}.log",
        "--loglevel", "INFO",
    ]

    logger.info(
        "[Phase2] Starting url_collector for website_id=%d with %d category URLs",
        website_id, len(category_urls),
    )

    try:
        proc = subprocess.run(
            cmd,
            cwd=backend_dir,
            timeout=timeout,
            capture_output=False,
            env={**os.environ, "PYTHONPATH": backend_dir},
        )
        if proc.returncode != 0:
            logger.error(
                "[Phase2] url_collector exited with code %d for website_id=%d",
                proc.returncode, website_id,
            )
    except subprocess.TimeoutExpired:
        logger.error("[Phase2] url_collector timed out for website_id=%d", website_id)
    except Exception as exc:
        logger.error("[Phase2] url_collector error: %s", exc)
    finally:
        try:
            os.remove(urls_file)
        except Exception:
            pass

    # Read result
    try:
        with open(result_file, "r") as f:
            count = int(f.read().strip())
        logger.info("[Phase2] Collected %d URLs for website_id=%d", count, website_id)
        return count
    except Exception:
        logger.warning("[Phase2] Could not read url_count for website_id=%d", website_id)
        return 0
