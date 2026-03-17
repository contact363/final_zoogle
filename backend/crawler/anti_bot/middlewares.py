"""
Anti-bot Scrapy downloader middlewares.

RotateUserAgentMiddleware  — injects realistic browser headers on every request
RetryWithBackoffMiddleware — retries 429/403/5xx with exponential back-off
RateLimiterMiddleware      — enforces per-domain minimum delay
"""
from __future__ import annotations

import random
import time
import logging
from collections import defaultdict

from scrapy import signals
from scrapy.exceptions import NotConfigured
from scrapy.http import Response

from crawler.anti_bot.user_agents import USER_AGENTS, ACCEPT_LANGUAGES

logger = logging.getLogger(__name__)


class RotateUserAgentMiddleware:
    """Attach a random user-agent + realistic browser headers to every request."""

    def process_request(self, request, spider):
        ua = random.choice(USER_AGENTS)
        lang = random.choice(ACCEPT_LANGUAGES)
        request.headers["User-Agent"] = ua
        request.headers["Accept-Language"] = lang
        request.headers["Accept"] = (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "image/avif,image/webp,*/*;q=0.8"
        )
        request.headers["Accept-Encoding"] = "gzip, deflate, br"
        request.headers["DNT"] = "1"
        request.headers["Upgrade-Insecure-Requests"] = "1"
        request.headers["Sec-Fetch-Dest"] = "document"
        request.headers["Sec-Fetch-Mode"] = "navigate"
        request.headers["Sec-Fetch-Site"] = "none"


class RetryWithBackoffMiddleware:
    """
    Retry failed requests with exponential back-off.
    Handles 429 (Too Many Requests) and 5xx server errors.
    """

    MAX_RETRIES = 3
    BASE_DELAY  = 2.0   # seconds
    MAX_DELAY   = 60.0  # cap

    RETRY_CODES = {429, 403, 500, 502, 503, 504}

    def process_response(self, request, response, spider):
        if response.status not in self.RETRY_CODES:
            return response

        retry_count = request.meta.get("retry_count", 0)
        if retry_count >= self.MAX_RETRIES:
            logger.warning(
                "Giving up on %s after %d retries (status %d)",
                request.url, retry_count, response.status,
            )
            return response

        # Honour Retry-After header if present
        retry_after = response.headers.get("Retry-After", b"").decode("utf-8", errors="ignore")
        if retry_after.isdigit():
            delay = min(float(retry_after), self.MAX_DELAY)
        else:
            delay = min(self.BASE_DELAY * (2 ** retry_count), self.MAX_DELAY)

        delay += random.uniform(0, 1)
        logger.info(
            "Retrying %s (attempt %d, delay %.1fs, status %d)",
            request.url, retry_count + 1, delay, response.status,
        )
        time.sleep(delay)

        new_request = request.copy()
        new_request.meta["retry_count"] = retry_count + 1
        new_request.dont_filter = True
        return new_request

    def process_exception(self, request, exception, spider):
        retry_count = request.meta.get("retry_count", 0)
        if retry_count >= self.MAX_RETRIES:
            return None

        delay = min(self.BASE_DELAY * (2 ** retry_count), self.MAX_DELAY)
        logger.info(
            "Retrying %s after exception %s (attempt %d)",
            request.url, type(exception).__name__, retry_count + 1,
        )
        time.sleep(delay)

        new_request = request.copy()
        new_request.meta["retry_count"] = retry_count + 1
        new_request.dont_filter = True
        return new_request


class RateLimiterMiddleware:
    """
    Enforce a minimum gap between requests to the same domain.
    Reads per-domain delay from spider.domain_delays dict (optional).
    Falls back to DOWNLOAD_DELAY setting.
    """

    def __init__(self, default_delay: float):
        self._last_request: dict[str, float] = defaultdict(float)
        self._default_delay = default_delay

    @classmethod
    def from_crawler(cls, crawler):
        delay = crawler.settings.getfloat("DOWNLOAD_DELAY", 1.0)
        return cls(default_delay=delay)

    def process_request(self, request, spider):
        from urllib.parse import urlparse
        domain = urlparse(request.url).netloc

        # Allow spider to specify per-domain delays
        domain_delays: dict = getattr(spider, "domain_delays", {})
        delay = domain_delays.get(domain, self._default_delay)

        elapsed = time.time() - self._last_request[domain]
        if elapsed < delay:
            sleep_for = delay - elapsed + random.uniform(0.1, 0.5)
            time.sleep(sleep_for)

        self._last_request[domain] = time.time()
