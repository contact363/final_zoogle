import scrapy


class MachineItem(scrapy.Item):
    # ── Required ──────────────────────────────────────────────────────────────
    machine_url    = scrapy.Field()   # canonical URL of the listing
    website_id     = scrapy.Field()   # FK → websites.id
    website_source = scrapy.Field()   # domain string (e.g. "machinio.com")

    # ── Core machine data ─────────────────────────────────────────────────────
    machine_type = scrapy.Field()     # normalised English type ("CNC Lathe", "VMC", …)
    brand        = scrapy.Field()     # manufacturer name (language-independent)
    model        = scrapy.Field()     # model / part number
    stock_number = scrapy.Field()     # dealer's stock / reference number (e.g. "ST-1234")
    price        = scrapy.Field()     # Decimal or None
    currency     = scrapy.Field()     # "USD", "EUR", "GBP", …
    location     = scrapy.Field()     # city / country string
    description  = scrapy.Field()     # full text description (max 2000 chars)

    # ── Classification ────────────────────────────────────────────────────────
    category     = scrapy.Field()     # category discovered from nav/breadcrumb

    # ── Language metadata ─────────────────────────────────────────────────────
    page_lang    = scrapy.Field()     # detected language code, e.g. "en", "de"
    canonical_url = scrapy.Field()    # English hreflang canonical if found

    # ── Media ─────────────────────────────────────────────────────────────────
    images       = scrapy.Field()     # list[str] of remote image URLs
    image_paths  = scrapy.Field()     # list[str] filled by ImageDownloadPipeline

    # ── Structured specifications ─────────────────────────────────────────────
    specs        = scrapy.Field()     # dict[str, str] of spec key→value pairs

    # ── Internal pipeline fields (not stored in DB) ───────────────────────────
    # Passed from DeduplicationPipeline → DatabasePipeline to avoid recomputing
    _dedup_key    = scrapy.Field()
    _content_hash = scrapy.Field()
