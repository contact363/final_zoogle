import scrapy


class MachineItem(scrapy.Item):
    # ── Required ──────────────────────────────────────────────────────────────
    machine_url    = scrapy.Field()   # canonical URL of the listing
    website_id     = scrapy.Field()   # FK → websites.id
    website_source = scrapy.Field()   # domain string (e.g. "machinio.com")

    # ── Core machine data ─────────────────────────────────────────────────────
    machine_type = scrapy.Field()     # normalised type ("CNC Lathe", "VMC", …)
    brand        = scrapy.Field()     # manufacturer name
    model        = scrapy.Field()     # model / part number
    price        = scrapy.Field()     # Decimal or None
    currency     = scrapy.Field()     # "USD", "EUR", "GBP", …
    location     = scrapy.Field()     # city / country string
    description  = scrapy.Field()     # full text description (max 2000 chars)

    # ── Classification ────────────────────────────────────────────────────────
    category     = scrapy.Field()     # category discovered from nav/breadcrumb

    # ── Media ────────────────────────────────────────────────────────────────
    images       = scrapy.Field()     # list[str] of remote image URLs
    image_paths  = scrapy.Field()     # list[str] filled by ImageDownloadPipeline

    # ── Structured specifications ─────────────────────────────────────────────
    specs        = scrapy.Field()     # dict[str, str] of spec key→value pairs
