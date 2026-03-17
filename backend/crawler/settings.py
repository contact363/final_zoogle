BOT_NAME = "zoogle"
SPIDER_MODULES = ["crawler.spiders"]
NEWSPIDER_MODULE = "crawler.spiders"

# Polite crawling
ROBOTSTXT_OBEY = False
COOKIES_ENABLED = True

# Concurrency
CONCURRENT_REQUESTS = 16
CONCURRENT_REQUESTS_PER_DOMAIN = 4
DOWNLOAD_DELAY = 1.0
RANDOMIZE_DOWNLOAD_DELAY = True

# AutoThrottle — backs off when server is slow
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1.0
AUTOTHROTTLE_MAX_DELAY = 15.0
AUTOTHROTTLE_TARGET_CONCURRENCY = 2.0

# Timeouts & retries
DOWNLOAD_TIMEOUT = 30
RETRY_TIMES = 3
RETRY_HTTP_CODES = [429, 500, 502, 503, 504]

# Do not filter duplicate requests — the URL queue handles dedup
DUPEFILTER_CLASS = "scrapy.dupefilters.BaseDupeFilter"

# Disable Telnet console
TELNETCONSOLE_ENABLED = False

# ── Downloader Middlewares ─────────────────────────────────────────────────────
# Disable Scrapy's built-in RetryMiddleware; use ours
DOWNLOADER_MIDDLEWARES = {
    "scrapy.downloadermiddlewares.retry.RetryMiddleware": None,
    "crawler.anti_bot.middlewares.RotateUserAgentMiddleware": 100,
    "crawler.anti_bot.middlewares.RetryWithBackoffMiddleware": 200,
    "crawler.anti_bot.middlewares.RateLimiterMiddleware": 300,
    "scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware": 810,
}

# ── Item Pipelines ────────────────────────────────────────────────────────────
# The machine_spider enables this; url_collector_spider overrides to empty.
ITEM_PIPELINES = {
    "crawler.pipelines.storage_pipeline.PostgreSQLPipeline": 300,
}

# Logging
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s [%(name)s] %(levelname)s: %(message)s"
