"""
BaseSpider — shared configuration for all Zoogle spiders.

All spiders inherit from this class to get:
  • Standard error handling
  • Domain delay override from training rules
  • Logging helpers
"""
from __future__ import annotations

import logging
from typing import Optional

import scrapy


class BaseSpider(scrapy.Spider):
    name = "base"

    # Subclasses may set this to override per-domain download delay
    domain_delays: dict = {}

    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "COOKIES_ENABLED": True,
        "DOWNLOAD_TIMEOUT": 30,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)

    def errback(self, failure):
        """Default errback — log and continue."""
        self.logger.warning(
            "Request failed: %s — %s",
            failure.request.url,
            repr(failure.value),
        )
