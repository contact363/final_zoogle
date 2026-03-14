import sys, os

# Make app importable from crawler.
# __file__ = backend/crawler/zoogle_crawler/settings.py
# ../.. from there = backend/   (where app/ lives)
_backend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if _backend_dir not in sys.path:
    sys.path.insert(0, _backend_dir)

BOT_NAME = "zoogle_crawler"
SPIDER_MODULES = ["zoogle_crawler.spiders"]
NEWSPIDER_MODULE = "zoogle_crawler.spiders"

# Crawl responsibly
ROBOTSTXT_OBEY = True
CONCURRENT_REQUESTS = 16
CONCURRENT_REQUESTS_PER_DOMAIN = 4
DOWNLOAD_DELAY = 1.5
RANDOMIZE_DOWNLOAD_DELAY = True

# AutoThrottle
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1
AUTOTHROTTLE_MAX_DELAY = 10
AUTOTHROTTLE_TARGET_CONCURRENCY = 4

# Retry
RETRY_ENABLED = True
RETRY_TIMES = 3
RETRY_HTTP_CODES = [500, 502, 503, 504, 408, 429]

# Timeouts
DOWNLOAD_TIMEOUT = 30

# User agent rotation
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Item pipelines (order matters)
ITEM_PIPELINES = {
    "zoogle_crawler.pipelines.ValidationPipeline": 100,
    "zoogle_crawler.pipelines.NormalizationPipeline": 200,
    "zoogle_crawler.pipelines.ImageDownloadPipeline": 300,
    "zoogle_crawler.pipelines.DatabasePipeline": 400,
}

# Image download
IMAGES_STORE = os.path.join(os.path.dirname(__file__), "../../../media/machines")

# Downloader middlewares
DOWNLOADER_MIDDLEWARES = {
    "zoogle_crawler.middlewares.RotateUserAgentMiddleware": 400,
    "zoogle_crawler.middlewares.ProxyMiddleware": 410,
}

# Feed settings
FEED_EXPORT_ENCODING = "utf-8"

# Logging
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s [%(name)s] %(levelname)s: %(message)s"
