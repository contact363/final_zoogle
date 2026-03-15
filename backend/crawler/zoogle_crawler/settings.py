"""
Scrapy settings — memory-optimised for Render's 512 MB free tier.

Key differences from a default Scrapy project
──────────────────────────────────────────────
• CONCURRENT_REQUESTS = 8   (was 32 — too high for 512 MB)
• CONCURRENT_REQUESTS_PER_DOMAIN = 4  (was 8)
• AUTOTHROTTLE_TARGET_CONCURRENCY = 3 (was 6)
• DOWNLOAD_MAXSIZE = 10 MB  (guard against huge HTML blobs)
• HTTPCACHE_ENABLED = False (disk cache eats RAM when pages are hot)
• MEMUSAGE_ENABLED = True   (log memory usage; warn at 400 MB, stop at 480 MB)
"""

import sys
import os

# Make the backend app importable from the crawler subprocess
_backend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if _backend_dir not in sys.path:
    sys.path.insert(0, _backend_dir)

# ── Identity ──────────────────────────────────────────────────────────────────
BOT_NAME        = "zoogle_crawler"
SPIDER_MODULES  = ["zoogle_crawler.spiders"]
NEWSPIDER_MODULE = "zoogle_crawler.spiders"

# ── Extensions ────────────────────────────────────────────────────────────────
# Disable the Telnet console — it causes "AlreadyNegotiating" CRITICAL errors
# on Render and other cloud environments where the telnet port is unavailable.
EXTENSIONS = {
    "scrapy.extensions.telnet.TelnetConsole": None,
}

# ── Crawl behaviour ───────────────────────────────────────────────────────────
ROBOTSTXT_OBEY = False          # Many machine sites block all bots in robots.txt

# Reduced concurrency — critical for 512 MB Render instances
CONCURRENT_REQUESTS            = 8
CONCURRENT_REQUESTS_PER_DOMAIN = 4

DOWNLOAD_DELAY         = 0.5   # seconds between requests (base)
RANDOMIZE_DOWNLOAD_DELAY = True  # actual delay = 0.5×DOWNLOAD_DELAY … 1.5×DOWNLOAD_DELAY

# ── AutoThrottle — adapts to server response times ────────────────────────────
AUTOTHROTTLE_ENABLED            = True
AUTOTHROTTLE_START_DELAY        = 1.0   # initial delay (seconds)
AUTOTHROTTLE_MAX_DELAY          = 30.0  # max delay under high load
AUTOTHROTTLE_TARGET_CONCURRENCY = 3.0   # target concurrent requests per server
AUTOTHROTTLE_DEBUG              = False

# ── Retry ─────────────────────────────────────────────────────────────────────
# The default RetryMiddleware is REPLACED by RetryWithBackoffMiddleware in
# the spider's custom_settings. These are kept as fallback defaults.
RETRY_ENABLED    = True
RETRY_TIMES      = 4
RETRY_HTTP_CODES = [500, 502, 503, 504, 408, 429, 403, 520, 521, 522, 524]

# ── Timeouts & size limits ────────────────────────────────────────────────────
DOWNLOAD_TIMEOUT = 30             # seconds per request
DOWNLOAD_MAXSIZE = 10 * 1024 * 1024  # 10 MB max response body

# ── Memory protection ─────────────────────────────────────────────────────────
MEMUSAGE_ENABLED        = True
MEMUSAGE_WARNING_MB     = 400    # log a warning
MEMUSAGE_LIMIT_MB       = 480    # stop the spider gracefully

# ── Default headers (overridden per-request by SmartHeadersMiddleware) ────────
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
DEFAULT_REQUEST_HEADERS = {
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language":  "en-US,en;q=0.9",
    "Accept-Encoding":  "gzip, deflate, br",
    "Connection":       "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

# ── Downloader middlewares ────────────────────────────────────────────────────
# The spider's custom_settings can override these per-run.
DOWNLOADER_MIDDLEWARES = {
    # Disable Scrapy's built-in retry — we use our enhanced version
    "scrapy.downloadermiddlewares.retry.RetryMiddleware": None,
    # Our middlewares in priority order
    "zoogle_crawler.middlewares.RetryWithBackoffMiddleware": 350,
    "zoogle_crawler.middlewares.SmartHeadersMiddleware":     400,
    "zoogle_crawler.middlewares.BotDetectionMiddleware":     410,
    "zoogle_crawler.middlewares.RateLimiterMiddleware":      420,
    "zoogle_crawler.middlewares.ProxyMiddleware":            430,
}

# ── Item pipelines (priority order) ──────────────────────────────────────────
#
#  100  ValidationPipeline    — drop structurally invalid items (no brand/model/content)
#  150  LanguageFilterPipeline — drop non-English pages (German/Italian/French/etc.)
#  200  NormalizationPipeline  — multilingual type map, stock number extraction, price parse
#  300  ImageDownloadPipeline  — download/store images
#  380  DeduplicationPipeline  — in-run dedup (dedup_key + content_hash)
#  400  DatabasePipeline       — upsert to PostgreSQL with cross-language dedup
#
ITEM_PIPELINES = {
    "zoogle_crawler.pipelines.ValidationPipeline":      100,
    "zoogle_crawler.pipelines.LanguageFilterPipeline":  150,
    "zoogle_crawler.pipelines.NormalizationPipeline":   200,
    "zoogle_crawler.pipelines.ImageDownloadPipeline":   300,
    "zoogle_crawler.pipelines.DeduplicationPipeline":   380,
    "zoogle_crawler.pipelines.DatabasePipeline":        400,
}

# ── Image storage ─────────────────────────────────────────────────────────────
IMAGES_STORE = os.path.join(os.path.dirname(__file__), "../../../media/machines")

# ── Cache — DISABLED to save disk & memory on Render ─────────────────────────
HTTPCACHE_ENABLED = False

# ── Depth & item cap (spider's custom_settings override these per-run) ────────
DEPTH_LIMIT             = 8
CLOSESPIDER_ITEMCOUNT   = 5000

# ── Dedup filter ──────────────────────────────────────────────────────────────
DUPEFILTER_CLASS = "scrapy.dupefilters.RFPDupeFilter"

# ── Feed encoding ────────────────────────────────────────────────────────────
FEED_EXPORT_ENCODING = "utf-8"

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_LEVEL  = "INFO"
LOG_FORMAT = "%(asctime)s [%(name)s] %(levelname)s: %(message)s"

# ── Playwright integration (scrapy-playwright, optional) ──────────────────────
# Uncomment the lines below if you install scrapy-playwright and want native
# integration instead of the subprocess-based JSRenderer.
#
# DOWNLOAD_HANDLERS = {
#     "http":  "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
#     "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
# }
# PLAYWRIGHT_BROWSER_TYPE = "chromium"
# PLAYWRIGHT_LAUNCH_OPTIONS = {
#     "headless": True,
#     "args": [
#         "--no-sandbox", "--disable-dev-shm-usage",
#         "--disable-gpu", "--single-process",
#     ],
# }
# PLAYWRIGHT_MAX_PAGES_PER_CONTEXT = 1   # memory safety on 512 MB
