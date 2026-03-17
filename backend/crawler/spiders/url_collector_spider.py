"""
Phase 2 — URL Collector Spider

Crawls category/listing pages and pushes all discovered product URLs
into the Redis queue for Phase 3.

Spider arguments (pass via -a):
  website_id        int   — website ID
  start_urls_file   str   — path to a newline-separated file of category URLs
  redis_url         str   — Redis connection URL
  product_link_pattern  str (optional) — regex for product URL detection
  request_delay     float (optional) — per-domain delay override
"""
from __future__ import annotations

import json
import os
import logging
from typing import Iterator
from urllib.parse import urlparse

import scrapy

from crawler.spiders.base_spider import BaseSpider
from crawler.extractors.html_extractor import (
    find_product_urls,
    find_next_page_url,
)
from crawler.queue.url_queue import URLQueue

logger = logging.getLogger(__name__)


class UrlCollectorSpider(BaseSpider):
    name = "url_collector"

    # No storage pipeline needed for this spider
    custom_settings = {
        **BaseSpider.custom_settings,
        "ITEM_PIPELINES": {},
        "CLOSESPIDER_PAGECOUNT": 5000,
    }

    def __init__(
        self,
        website_id: str = "",
        start_urls_file: str = "",
        redis_url: str = "",
        product_link_pattern: str = "",
        request_delay: str = "1.0",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.website_id = int(website_id)
        self.product_link_pattern = product_link_pattern

        # Per-domain delay
        delay = float(request_delay)
        self.custom_settings["DOWNLOAD_DELAY"] = delay

        # Redis queue
        self._queue = URLQueue(redis_url, self.website_id)
        self._queue.clear()   # start fresh for this crawl

        # Load start URLs from file
        self.start_urls = []
        if start_urls_file and os.path.exists(start_urls_file):
            with open(start_urls_file, "r", encoding="utf-8") as f:
                self.start_urls = [line.strip() for line in f if line.strip()]

        if not self.start_urls:
            logger.error("No start URLs provided for UrlCollectorSpider")

        self._urls_pushed = 0
        self._pages_crawled = 0

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse_listing, errback=self.errback)

    def parse_listing(self, response):
        self._pages_crawled += 1

        # Extract product URLs from this listing page
        product_urls = find_product_urls(
            response.text,
            response.url,
            self.product_link_pattern,
        )

        new_count = self._queue.push_many(product_urls)
        self._urls_pushed += new_count

        logger.info(
            "[%s] Page %d: found %d product URLs (%d new) — queue size: %d",
            response.url,
            self._pages_crawled,
            len(product_urls),
            new_count,
            self._queue.size(),
        )

        # Follow pagination
        next_url = find_next_page_url(response.text, response.url)
        if next_url and next_url != response.url:
            yield scrapy.Request(next_url, callback=self.parse_listing, errback=self.errback)

    def closed(self, reason):
        total = self._queue.seen_count()
        logger.info(
            "UrlCollectorSpider finished. Pages crawled: %d | URLs queued: %d | Reason: %s",
            self._pages_crawled,
            total,
            reason,
        )
        # Write final count to a result file so the task can read it
        result_file = f"/tmp/url_count_{self.website_id}.txt"
        try:
            with open(result_file, "w") as f:
                f.write(str(total))
        except Exception:
            pass
