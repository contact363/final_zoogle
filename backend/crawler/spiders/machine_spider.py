"""
Phase 3 — Machine Spider

Reads product URLs from the Redis queue and extracts structured
machine data from each page. Yields MachineItem objects which
are stored by PostgreSQLPipeline.

Spider arguments (pass via -a):
  website_id      int   — website ID
  redis_url       str   — Redis connection URL
  db_url          str   — PostgreSQL sync connection URL
  request_delay   float (optional)
  batch_size      int   (optional, default 50) — URLs popped per batch
"""
from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path
from typing import Iterator

import scrapy

from crawler.spiders.base_spider import BaseSpider
from crawler.items import MachineItem
from crawler.extractors.html_extractor import extract_machine_data
from crawler.queue.url_queue import URLQueue

logger = logging.getLogger(__name__)


class MachineSpider(BaseSpider):
    name = "machine_spider"

    custom_settings = {
        **BaseSpider.custom_settings,
        "CONCURRENT_REQUESTS": 8,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
    }

    def __init__(
        self,
        website_id: str = "",
        redis_url: str = "",
        db_url: str = "",
        request_delay: str = "1.0",
        batch_size: str = "50",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.website_id   = int(website_id)
        self.batch_size   = int(batch_size)
        self._db_url      = db_url
        self._queue       = URLQueue(redis_url, self.website_id)
        self._machines_extracted = 0
        self._errors = 0

        delay = float(request_delay)
        self.custom_settings["DOWNLOAD_DELAY"] = delay

    def start_requests(self):
        """
        Pop URLs from Redis in batches and schedule requests.
        Uses a recursive callback to keep the queue draining.
        """
        urls = self._queue.pop_many(self.batch_size)
        if not urls:
            logger.warning("Redis queue is empty for website_id=%d", self.website_id)
            return

        for url in urls:
            yield scrapy.Request(
                url,
                callback=self.parse_machine,
                errback=self.errback,
                meta={"handle_httpstatus_list": [404, 410]},
            )

    def parse_machine(self, response):
        # Skip 404/410 — product no longer exists
        if response.status in (404, 410):
            return

        try:
            data = extract_machine_data(response.text, response.url)
        except Exception as exc:
            self._errors += 1
            logger.warning("Extraction failed for %s: %s", response.url, exc)
            data = {}

        item = MachineItem()
        item["website_id"]   = self.website_id
        item["source_url"]   = response.url
        item["machine_name"] = data.get("machine_name") or ""
        item["brand"]        = data.get("brand") or ""
        item["model"]        = data.get("model") or ""
        item["machine_type"] = data.get("machine_type") or ""
        item["condition"]    = data.get("condition") or ""
        item["year"]         = data.get("year")
        item["price"]        = data.get("price")
        item["currency"]     = data.get("currency") or "USD"
        item["stock_number"] = data.get("stock_number") or ""
        item["description"]  = data.get("description") or ""
        item["specifications"] = data.get("specifications") or {}
        item["images"]       = data.get("images") or []
        item["content_hash"] = data.get("content_hash") or ""

        self._machines_extracted += 1

        # Drain more URLs from queue after each parse to keep spider alive
        remaining = self._queue.pop_many(10)
        for url in remaining:
            yield scrapy.Request(
                url,
                callback=self.parse_machine,
                errback=self.errback,
                meta={"handle_httpstatus_list": [404, 410]},
            )

        yield item

    def closed(self, reason):
        logger.info(
            "MachineSpider finished. Extracted: %d | Errors: %d | Reason: %s",
            self._machines_extracted,
            self._errors,
            reason,
        )
        # FIX 3: Write result using cross-platform temp dir (not hard-coded /tmp)
        result_file = str(Path(tempfile.gettempdir()) / f"machine_count_{self.website_id}.txt")
        try:
            with open(result_file, "w") as f:
                f.write(f"{self._machines_extracted},{self._errors}")
        except Exception:
            pass
