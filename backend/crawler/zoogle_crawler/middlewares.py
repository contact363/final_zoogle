"""
Zoogle Downloader Middlewares
─────────────────────────────
1. SmartHeadersMiddleware  – rotates 30+ UAs + full browser-like header fingerprints
2. RetryWithBackoffMiddleware – exponential back-off + Retry-After support
3. BotDetectionMiddleware  – detects Cloudflare / captcha and logs (doesn't crash)
4. RateLimiterMiddleware   – per-domain cooldown so we don't hammer one host
5. ProxyMiddleware         – optional proxy rotation (set PROXY_LIST in settings)
"""
import random
import time
import logging
from urllib.parse import urlparse

from scrapy import signals
from scrapy.exceptions import IgnoreRequest
from scrapy.downloadermiddlewares.retry import RetryMiddleware
from scrapy.utils.response import response_status_message

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# 30+ realistic user agents across Chrome / Firefox / Safari / Edge
# Keep these up-to-date; stale UAs are an easy bot signal.
# ─────────────────────────────────────────────────────────────────────────────
USER_AGENTS = [
    # Chrome Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    # Chrome macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    # Chrome Linux
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    # Firefox Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    # Firefox macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13.6; rv:124.0) Gecko/20100101 Firefox/124.0",
    # Firefox Linux
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
    # Safari macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
    # Edge Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
    # Chrome mobile
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.82 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 13; SM-G998B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Mobile Safari/537.36",
    # Safari mobile (iOS)
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPad; CPU OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
    # Opera
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 OPR/110.0.0.0",
]

# Matching Accept headers per browser engine
_CHROME_ACCEPT  = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"
_FIREFOX_ACCEPT = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"
_SAFARI_ACCEPT  = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"

def _accept_for_ua(ua: str) -> str:
    if "Firefox" in ua:
        return _FIREFOX_ACCEPT
    if "Safari" in ua and "Chrome" not in ua:
        return _SAFARI_ACCEPT
    return _CHROME_ACCEPT

def _sec_ch_ua_for_ua(ua: str) -> dict:
    """Add Sec-CH-UA headers for Chromium-based UAs."""
    if "Chrome/124" in ua:
        return {
            "Sec-CH-UA": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
            "Sec-CH-UA-Mobile": "?0",
            "Sec-CH-UA-Platform": '"Windows"' if "Windows" in ua else '"macOS"' if "Mac" in ua else '"Linux"',
        }
    if "Edg/" in ua:
        return {
            "Sec-CH-UA": '"Chromium";v="124", "Microsoft Edge";v="124", "Not-A.Brand";v="99"',
            "Sec-CH-UA-Mobile": "?0",
            "Sec-CH-UA-Platform": '"Windows"',
        }
    return {}

# Common referrers to use for cold requests
REFERRERS = [
    "https://www.google.com/",
    "https://www.bing.com/",
    "https://duckduckgo.com/",
    None,  # no referrer sometimes
]


# ─────────────────────────────────────────────────────────────────────────────
# 1. SmartHeadersMiddleware
# ─────────────────────────────────────────────────────────────────────────────

class SmartHeadersMiddleware:
    """
    Injects a randomly chosen User-Agent with a matching full browser header
    fingerprint. Also manages Referer chaining (use prev response URL).
    """

    def process_request(self, request, spider):
        ua = random.choice(USER_AGENTS)
        request.headers["User-Agent"] = ua
        request.headers["Accept"] = _accept_for_ua(ua)
        request.headers["Accept-Language"] = random.choice([
            "en-US,en;q=0.9",
            "en-GB,en;q=0.8",
            "en-US,en;q=0.9,de;q=0.7",
            "en-US,en;q=0.8,fr;q=0.5",
        ])
        request.headers["Accept-Encoding"] = "gzip, deflate, br"
        request.headers["Connection"] = "keep-alive"
        request.headers["Upgrade-Insecure-Requests"] = "1"
        request.headers["Cache-Control"] = random.choice(["no-cache", "max-age=0"])
        request.headers["Pragma"] = "no-cache"

        # Sec-Fetch headers (Chrome-like)
        if "Firefox" not in ua and "Safari" not in ua.split("Safari")[0]:
            request.headers["Sec-Fetch-Dest"] = "document"
            request.headers["Sec-Fetch-Mode"] = "navigate"
            request.headers["Sec-Fetch-Site"] = "none" if not request.headers.get("Referer") else "same-site"
            request.headers["Sec-Fetch-User"] = "?1"
            for k, v in _sec_ch_ua_for_ua(ua).items():
                request.headers[k] = v

        # Referer: use the parent URL stored in meta, or a random search engine
        if "referer" in request.meta:
            request.headers["Referer"] = request.meta["referer"]
        elif random.random() < 0.4:
            ref = random.choice(REFERRERS)
            if ref:
                request.headers["Referer"] = ref

    def process_response(self, request, response, spider):
        # Store this URL in meta for child requests (Referer chaining).
        # Use request.meta (already available as a parameter) instead of
        # response.meta — response.meta is a property that accesses
        # response.request.meta and raises AttributeError on synthetic responses
        # that are not tied to any request object.
        request.meta["referer"] = response.url
        return response


# ─────────────────────────────────────────────────────────────────────────────
# 2. RetryWithBackoffMiddleware
# ─────────────────────────────────────────────────────────────────────────────

class RetryWithBackoffMiddleware(RetryMiddleware):
    """
    Extends Scrapy's built-in RetryMiddleware with:
    - Exponential back-off with jitter
    - Honour Retry-After header on 429
    - Log which retry attempt we're on
    """

    def process_response(self, request, response, spider):
        if response.status == 429:
            retry_after = response.headers.get("Retry-After", b"").decode("utf-8", errors="ignore").strip()
            try:
                wait = float(retry_after) if retry_after else None
            except ValueError:
                wait = None
            wait = min(wait or 30.0, 120.0)  # cap at 2 min
            logger.warning(f"429 on {response.url} — sleeping {wait}s")
            time.sleep(wait)
            return self._retry(request, f"429 Too Many Requests", spider) or response

        if response.status in (403, 503):
            attempt = request.meta.get("retry_times", 0)
            # Exponential backoff: 2^attempt seconds + jitter (capped at 60s)
            wait = min(2 ** attempt + random.uniform(0, 1), 60.0)
            logger.warning(f"HTTP {response.status} on {response.url} — backoff {wait:.1f}s (attempt {attempt})")
            time.sleep(wait)
            return self._retry(request, response_status_message(response.status), spider) or response

        return super().process_response(request, response, spider)

    def process_exception(self, request, exception, spider):
        attempt = request.meta.get("retry_times", 0)
        wait = min(2 ** attempt + random.uniform(0, 0.5), 30.0)
        logger.warning(f"Request exception {type(exception).__name__} on {request.url} — backoff {wait:.1f}s")
        time.sleep(wait)
        return super().process_exception(request, exception, spider)


# ─────────────────────────────────────────────────────────────────────────────
# 3. BotDetectionMiddleware
# ─────────────────────────────────────────────────────────────────────────────

_BOT_DETECTION_SIGNALS = [
    "cloudflare", "cf-ray", "just a moment", "ddos-guard",
    "enable javascript", "checking your browser", "captcha",
    "access denied", "403 forbidden", "bot detection",
    "please verify you are human", "security check",
    "distil networks", "imperva", "incapsula",
]

class BotDetectionMiddleware:
    """
    Logs when a bot-detection page is returned.
    Marks the request for JS rendering if Playwright is available.
    Does NOT crash the spider — just logs and skips the item.
    """

    def process_response(self, request, response, spider):
        if response.status in (200, 206):
            text_sample = response.text[:3000].lower()
            for signal in _BOT_DETECTION_SIGNALS:
                if signal in text_sample:
                    logger.warning(
                        f"Bot-detection page detected ({signal!r}) at {response.url}. "
                        f"Will attempt JS fallback if enabled."
                    )
                    # Use request.meta directly (same fix as SmartHeadersMiddleware —
                    # response.meta raises AttributeError on synthetic responses).
                    request.meta["bot_detected"] = True
                    request.meta["bot_signal"] = signal
                    return response
        return response


# ─────────────────────────────────────────────────────────────────────────────
# 4. RateLimiterMiddleware
# ─────────────────────────────────────────────────────────────────────────────

class RateLimiterMiddleware:
    """
    Enforces a per-domain minimum delay to avoid hammering any single host.
    Default: 0.5s between requests to the same domain.
    Configurable via RATE_LIMIT_DELAY in spider settings.
    """

    def __init__(self):
        self._last_request: dict[str, float] = {}

    @classmethod
    def from_crawler(cls, crawler):
        instance = cls()
        instance.min_delay = crawler.settings.getfloat("RATE_LIMIT_DELAY", 0.5)
        return instance

    def process_request(self, request, spider):
        domain = urlparse(request.url).netloc
        now = time.time()
        last = self._last_request.get(domain, 0)
        gap = now - last
        if gap < self.min_delay:
            sleep_for = self.min_delay - gap + random.uniform(0, 0.2)
            time.sleep(sleep_for)
        self._last_request[domain] = time.time()


# ─────────────────────────────────────────────────────────────────────────────
# 5. ProxyMiddleware
# ─────────────────────────────────────────────────────────────────────────────

class ProxyMiddleware:
    """
    Optional proxy rotation.
    Set PROXY_LIST = ['http://host:port', ...] in settings or spider custom_settings.
    """

    def process_request(self, request, spider):
        proxies = spider.settings.getlist("PROXY_LIST", [])
        if proxies:
            request.meta["proxy"] = random.choice(proxies)
