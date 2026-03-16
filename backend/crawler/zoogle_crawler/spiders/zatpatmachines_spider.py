"""
ZatPatMachinesSpider — Dedicated Scrapy spider for zatpatmachines.com
======================================================================

Site: ZatPat Machines (India) — Vite / React SPA backed by Supabase PostgREST
URL:  https://zatpatmachines.com

Why a dedicated spider?
───────────────────────
The generic spider correctly discovers the Supabase project URL and anon key
from the JS bundle, but its _json_to_machine_item() cannot map the zatpat
data shape:
  • field is "model_name" not "title" / "name" / "model"
  • brand, machine_type, category are nested: brands{name}, machine_types{name}
  • images come from machine_images sub-table (not a flat "image" field)
  • machine URL: https://zatpatmachines.com/machine/{sku_number}
This spider handles all of the above correctly.

Crawl flow
──────────
Phase 1 – Credential discovery
  • Fetch homepage → find <script src="/assets/index-*.js">
  • Download JS bundle → extract Supabase project URL and anon JWT via regex
  • Fallback: use known credentials if bundle detection fails

Phase 2 – Supabase REST API pagination
  • GET /rest/v1/machines?select=...&status=eq.active&limit=1000&offset=N
  • Fields fetched: id, sku_number, model_name, year, condition, price,
    currency, location_city, location_country, main_image_url, description,
    brands(name), machine_types(name), categories(name),
    machine_images(image_url,display_order)
  • Pages until fewer rows than the limit are returned

Phase 3 – Item assembly
  • machine_url  = https://zatpatmachines.com/machine/{sku_number}
  • title        = "{brand} {model_name}"  (e.g. "Hurco VM2")
  • images       = machine_images sorted by display_order  (main_image first)
  • specs        = {Year, Condition, SKU, Spec values if present}
  • Duplicate guard: sku_number is unique — machine_url is the dedup key

Notes
─────
  • 5 003 active listings as of 2026-03-15 (confirmed via count=exact header)
  • Supabase anon key is a PUBLIC credential embedded in the site's JS bundle
  • RLS policy only exposes status='active' rows through the public anon role
"""

import json
import logging
import re
from urllib.parse import urljoin

import scrapy

from zoogle_crawler.items import MachineItem
from zoogle_crawler.spiders.base_spider import BaseZoogleSpider

logger = logging.getLogger(__name__)

SPIDER_VERSION = "2026-03-15-v1"
SITE_URL       = "https://zatpatmachines.com"

# ── Known Supabase project (discovered from JS bundle on 2026-03-15) ─────────
# These are the public anon credentials embedded in the site's own JavaScript.
# Used as a fallback if dynamic bundle detection fails.
_SUPABASE_URL = "https://aqhgorgilxwrhzleztby.supabase.co"
_SUPABASE_KEY = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
    ".eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImFxaGdvcmdpbHh3cmh6bGV6dGJ5Iiw"
    "icm9sZSI6ImFub24iLCJpYXQiOjE3NjU0MjMxNDEsImV4cCI6MjA4MDk5OTE0MX0"
    ".GD_HVD-98oUUM9RteG_DxPD3Deg8lyqLpq9d8tgYA5A"
)

# Supabase page size — PostgREST default max is 1000
_PAGE_SIZE = 1000

# PostgREST select expression with embedded joins
_SELECT = ",".join([
    "id",
    "sku_number",
    "id_prefix",
    "model_name",
    "year",
    "condition",
    "price",
    "currency",
    "location_city",
    "location_country",
    "main_image_url",
    "description",
    "status",
    "brands(name)",
    "machine_types(name)",
    "categories(name)",
    "machine_images(image_url,display_order)",
])


class ZatPatMachinesSpider(BaseZoogleSpider):
    """
    Dedicated spider for zatpatmachines.com.
    Can be invoked as:
        scrapy crawl zatpatmachines -a website_id=<id>
    """

    name = "zatpatmachines"
    allowed_domains = ["zatpatmachines.com", "aqhgorgilxwrhzleztby.supabase.co"]

    custom_settings = {
        "DEPTH_LIMIT":                     3,
        "CLOSESPIDER_ITEMCOUNT":           10000,
        "DOWNLOAD_DELAY":                  0.3,
        "RANDOMIZE_DOWNLOAD_DELAY":        True,
        "CONCURRENT_REQUESTS_PER_DOMAIN":  4,
        "ROBOTSTXT_OBEY":                  False,
        "RETRY_TIMES":                     3,
        "RETRY_HTTP_CODES":                [500, 502, 503, 504, 408, 429],
        "DOWNLOAD_TIMEOUT":                30,
        "HTTPERROR_ALLOWED_CODES":         [404, 406],
        "REDIRECT_ENABLED":                True,
        # SmartHeadersMiddleware is disabled — it overwrites Accept: application/json
        # on API requests which breaks PostgREST responses.
        "DOWNLOADER_MIDDLEWARES": {
            "scrapy.downloadermiddlewares.retry.RetryMiddleware":    None,
            "zoogle_crawler.middlewares.RetryWithBackoffMiddleware":  350,
            "zoogle_crawler.middlewares.SmartHeadersMiddleware":      None,
            "zoogle_crawler.middlewares.BotDetectionMiddleware":      410,
            "zoogle_crawler.middlewares.RateLimiterMiddleware":       420,
            "zoogle_crawler.middlewares.ProxyMiddleware":             430,
        },
        "DEFAULT_REQUEST_HEADERS": {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            # No "br" — Scrapy cannot decompress Brotli
            "Accept-Encoding": "gzip, deflate",
        },
    }

    def __init__(self, website_id=None, start_url=None, crawl_log_id=None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.website_id   = int(website_id)   if website_id   else None
        self.crawl_log_id = int(crawl_log_id) if crawl_log_id else None
        self.start_urls   = [start_url or SITE_URL]
        self._supabase_url: str = ""
        self._supabase_key: str = ""
        self._seen_skus:    set = set()
        logger.info(
            f"[ZatPatMachinesSpider v={SPIDER_VERSION}] init — "
            f"website_id={self.website_id}"
        )

    # =========================================================================
    # Phase 1 — Entry point: discover Supabase credentials from JS bundle
    # =========================================================================

    def start_requests(self):
        logger.info(f"[ZatPatMachinesSpider v={SPIDER_VERSION}] start_requests")
        # Fetch the homepage to discover the JS bundle URL dynamically.
        # If that fails, we fall back to known hardcoded credentials.
        yield scrapy.Request(
            SITE_URL,
            callback=self._parse_homepage,
            errback=self._homepage_errback,
        )

    def _parse_homepage(self, response):
        """
        Find the Vite JS bundle URL from the homepage HTML, then schedule
        a bundle download to extract Supabase credentials.
        Falls back to known credentials if no bundle is found.
        """
        bundle_url = None

        # Vite output: <script type="module" crossorigin src="/assets/index-*.js">
        for src in response.css("script[src]::attr(src)").getall():
            if re.search(r"/assets/index[^/]*\.js$", src):
                bundle_url = urljoin(SITE_URL, src)
                break

        # Broader fallback: any /assets/*.js or /static/js/*.js
        if not bundle_url:
            for src in response.css("script[src]::attr(src)").getall():
                if re.search(r"/assets/[^/]+\.js$|/static/js/[^/]+\.js$", src):
                    bundle_url = urljoin(SITE_URL, src)
                    break

        if bundle_url:
            logger.info(f"JS bundle found: {bundle_url}")
            yield scrapy.Request(
                bundle_url,
                callback=self._parse_js_bundle,
                errback=self._homepage_errback,
                # Brotli-safe Accept-Encoding already in DEFAULT_REQUEST_HEADERS
            )
        else:
            logger.warning("No JS bundle found in homepage — using hardcoded credentials")
            yield from self._start_api_crawl(_SUPABASE_URL, _SUPABASE_KEY)

    def _parse_js_bundle(self, response):
        """
        Extract Supabase project URL and anon JWT from the minified JS bundle.
        Falls back to hardcoded credentials if extraction fails.
        """
        if response.status != 200:
            logger.warning(f"Bundle fetch failed ({response.status}) — using hardcoded credentials")
            yield from self._start_api_crawl(_SUPABASE_URL, _SUPABASE_KEY)
            return

        text = response.text
        logger.info(f"Scanning JS bundle ({len(text):,} chars) for Supabase credentials")

        url_m = re.search(r"https://([a-z0-9]{10,30})\.supabase\.co", text)
        key_m = re.search(
            r'["\x27`]?(eyJ[A-Za-z0-9_+/\-]{10,}'
            r'\.[A-Za-z0-9_+/\-]{40,}'
            r'\.[A-Za-z0-9_+/\-]{10,})["\x27`]?',
            text,
        )

        if url_m and key_m:
            supabase_url = f"https://{url_m.group(1)}.supabase.co"
            anon_key     = key_m.group(1)
            logger.info(
                f"Supabase credentials extracted from bundle: "
                f"url={supabase_url} key_len={len(anon_key)}"
            )
            yield from self._start_api_crawl(supabase_url, anon_key)
        else:
            logger.warning(
                "Supabase credentials NOT found in bundle "
                f"(url={'yes' if url_m else 'no'} key={'yes' if key_m else 'no'}) "
                "— using hardcoded credentials"
            )
            yield from self._start_api_crawl(_SUPABASE_URL, _SUPABASE_KEY)

    def _homepage_errback(self, failure):
        """Homepage or bundle fetch failed — go straight to known credentials."""
        logger.warning(f"Homepage/bundle fetch error: {repr(failure.value)[:100]} — using hardcoded credentials")
        yield from self._start_api_crawl(_SUPABASE_URL, _SUPABASE_KEY)

    # =========================================================================
    # Phase 2 — Supabase API pagination
    # =========================================================================

    def _start_api_crawl(self, supabase_url: str, anon_key: str):
        """Schedule the first page of the machines API."""
        self._supabase_url = supabase_url
        self._supabase_key = anon_key
        yield from self._fetch_page(offset=0)

    def _fetch_page(self, offset: int):
        """Build and schedule a single paginated API request."""
        url = (
            f"{self._supabase_url}/rest/v1/machines"
            f"?select={_SELECT}"
            f"&status=eq.active"
            f"&order=created_at.asc"
            f"&limit={_PAGE_SIZE}"
            f"&offset={offset}"
        )
        logger.info(f"Fetching Supabase page offset={offset}: {url[:120]}")
        yield scrapy.Request(
            url,
            callback=self._parse_machines_page,
            errback=self._errback,
            headers={
                "apikey":        self._supabase_key,
                "Authorization": f"Bearer {self._supabase_key}",
                "Accept":        "application/json",
            },
            meta={"offset": offset},
            dont_filter=True,
        )

    def _parse_machines_page(self, response):
        """
        Parse one page of machine records from the Supabase REST API.
        Yields MachineItems and schedules the next page if more rows exist.
        """
        offset = response.meta.get("offset", 0)

        if response.status not in (200, 206):
            logger.error(
                f"Supabase API error: status={response.status} "
                f"body={response.text[:200]!r}"
            )
            return

        try:
            rows = json.loads(response.text)
        except Exception as exc:
            logger.error(f"Supabase JSON parse error (offset={offset}): {exc}")
            return

        if not isinstance(rows, list):
            # PostgREST error envelope: {"code": "...", "message": "..."}
            if isinstance(rows, dict) and rows.get("message"):
                logger.error(f"Supabase API error: {rows['message']}")
            return

        logger.info(f"Supabase page offset={offset}: {len(rows)} rows received")

        yielded = 0
        for row in rows:
            item = self._row_to_item(row)
            if item:
                yielded += 1
                yield item

        logger.info(f"Supabase page offset={offset}: {yielded}/{len(rows)} items yielded")

        # If a full page was returned, there may be more
        if len(rows) == _PAGE_SIZE:
            yield from self._fetch_page(offset + _PAGE_SIZE)

    # =========================================================================
    # Phase 3 — Row → MachineItem
    # =========================================================================

    def _row_to_item(self, row: dict):
        """
        Convert one Supabase machines row (with embedded joins) to a MachineItem.

        Expected shape:
          {id, sku_number, id_prefix, model_name, year, condition,
           price, currency, location_city, location_country,
           main_image_url, description, status,
           brands:{name}, machine_types:{name}, categories:{name},
           machine_images:[{image_url, display_order}]}
        """
        if not isinstance(row, dict):
            return None

        # Skip duplicates (sku_number is unique)
        sku = (row.get("sku_number") or "").strip()
        if not sku or sku in self._seen_skus:
            return None
        self._seen_skus.add(sku)

        # ── Brand and model ───────────────────────────────────────────────────
        brand_obj    = row.get("brands") or {}
        brand        = (brand_obj.get("name") if isinstance(brand_obj, dict) else "") or ""
        model        = (row.get("model_name") or "").strip()

        # Title = "Brand Model" — fall back to just model if brand missing
        title = f"{brand} {model}".strip() if brand else model
        if not title:
            return None

        # ── Machine type and category ─────────────────────────────────────────
        type_obj     = row.get("machine_types") or {}
        machine_type = (type_obj.get("name") if isinstance(type_obj, dict) else "") or None

        cat_obj      = row.get("categories") or {}
        category     = (cat_obj.get("name") if isinstance(cat_obj, dict) else "") or None

        # ── Machine URL (unique per SKU) ──────────────────────────────────────
        machine_url = f"{SITE_URL}/machine/{sku}"

        # ── Images — sorted by display_order; main_image as first fallback ───
        raw_imgs = row.get("machine_images") or []
        if isinstance(raw_imgs, list) and raw_imgs:
            sorted_imgs = sorted(
                [i for i in raw_imgs if isinstance(i, dict) and i.get("image_url")],
                key=lambda i: i.get("display_order", 999),
            )
            images = [i["image_url"] for i in sorted_imgs]
        else:
            # Fall back to main_image_url if sub-table returned nothing
            main_img = (row.get("main_image_url") or "").strip()
            images = [main_img] if main_img else []

        # ── Price ─────────────────────────────────────────────────────────────
        raw_price = row.get("price")
        try:
            price = float(raw_price) if raw_price is not None else None
        except (TypeError, ValueError):
            price = None

        currency = (row.get("currency") or "INR").strip().upper()

        # ── Location ──────────────────────────────────────────────────────────
        city    = (row.get("location_city")    or "").strip()
        country = (row.get("location_country") or "").strip()
        location = ", ".join(p for p in (city, country) if p) or None

        # ── Description ───────────────────────────────────────────────────────
        description = (row.get("description") or "").strip()[:2000] or None

        # ── Specs ─────────────────────────────────────────────────────────────
        specs: dict[str, str] = {}
        if row.get("year"):
            specs["Year"] = str(row["year"])
        if row.get("condition"):
            specs["Condition"] = str(row["condition"])
        specs["SKU"] = sku
        for spec_key in ("spec1_value", "spec2_value", "spec3_value"):
            val = row.get(spec_key)
            if val:
                label = spec_key.replace("_value", "").replace("spec", "Spec ").title()
                specs[label] = str(val)

        return MachineItem(
            machine_url=machine_url,
            website_id=self.website_id,
            website_source="zatpatmachines.com",
            category=category,
            machine_type=machine_type,
            brand=brand.strip() or None,
            model=model or None,
            stock_number=sku,
            price=price,
            currency=currency,
            location=location,
            description=description,
            images=images,
            specs=specs,
        )

    # ── Error callback ────────────────────────────────────────────────────────

    def _errback(self, failure):
        logger.warning(
            f"Request error: {failure.request.url} — "
            f"{repr(failure.value)[:100]}"
        )
