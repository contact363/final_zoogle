"""
ZatPatMachinesSpider — Generic Supabase Spider
===============================================

Handles ANY Vite/React SPA backed by Supabase PostgREST — not just
zatpatmachines.com. Works for zatpatestimate.com and any future
Supabase-backed supplier sites without code changes.

How it works
────────────
Phase 1 – Credential discovery
  • Fetch the site homepage → find the Vite JS bundle
  • Scan the bundle → extract Supabase project URL + anon JWT
  • Fallback: use known credentials for zatpatmachines.com if bundle fails

Phase 2 – Table discovery
  • Probe a list of common table names (machines, products, listings, …)
  • First table that returns a non-empty JSON array is used

Phase 3 – Paginated API fetch
  • GET /rest/v1/{table}?select=*&limit=1000&offset=N
  • Continue until a page smaller than 1000 is returned

Phase 4 – Generic row → MachineItem mapping
  • Tries zatpatmachines-specific field names first, then falls back to
    generic aliases so any Supabase schema yields useful data

To add a new Supabase-backed site:
  1. Add it to _DEDICATED_SPIDERS in tasks/crawl_tasks.py
  2. No other changes needed — credentials are auto-discovered from the bundle
"""

import json
import logging
import re
from urllib.parse import urljoin, urlparse

import scrapy

from zoogle_crawler.items import MachineItem
from zoogle_crawler.spiders.base_spider import BaseZoogleSpider

logger = logging.getLogger(__name__)

SPIDER_VERSION = "2026-03-16-v2"

# ── Fallback credentials for zatpatmachines.com ───────────────────────────────
_ZATPAT_SUPABASE_URL = "https://aqhgorgilxwrhzleztby.supabase.co"
_ZATPAT_SUPABASE_KEY = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
    ".eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImFxaGdvcmdpbHh3cmh6bGV6dGJ5Iiw"
    "icm9sZSI6ImFub24iLCJpYXQiOjE3NjU0MjMxNDEsImV4cCI6MjA4MDk5OTE0MX0"
    ".GD_HVD-98oUUM9RteG_DxPD3Deg8lyqLpq9d8tgYA5A"
)

# Supabase PostgREST page size (max 1000 per request)
_PAGE_SIZE = 1000

# Table names to probe in order — first one that returns rows wins
_TABLE_PROBES = [
    "machines",
    "products",
    "listings",
    "items",
    "inventory",
    "estimates",
    "used_machines",
    "machine_listings",
]

# zatpatmachines.com detailed select with joins (used when table=machines)
_ZATPAT_SELECT = ",".join([
    "id", "sku_number", "id_prefix", "model_name", "year", "condition",
    "price", "currency", "location_city", "location_country",
    "main_image_url", "description", "status",
    "brands(name)", "machine_types(name)", "categories(name)",
    "machine_images(image_url,display_order)",
])

# Generic select for unknown tables — just fetch everything
_GENERIC_SELECT = "*"


class ZatPatMachinesSpider(BaseZoogleSpider):
    """
    Generic Supabase spider. Handles zatpatmachines.com, zatpatestimate.com,
    and any future Supabase-backed supplier site.
    """

    name = "zatpatmachines"
    # allowed_domains is set dynamically in __init__ based on start_url
    allowed_domains = ["zatpatmachines.com", "zatpatestimate.com", "supabase.co"]

    custom_settings = {
        "DEPTH_LIMIT":                     3,
        "CLOSESPIDER_ITEMCOUNT":           0,       # no limit
        "DOWNLOAD_DELAY":                  0.3,
        "RANDOMIZE_DOWNLOAD_DELAY":        True,
        "CONCURRENT_REQUESTS_PER_DOMAIN":  4,
        "ROBOTSTXT_OBEY":                  False,
        "RETRY_TIMES":                     3,
        "RETRY_HTTP_CODES":                [500, 502, 503, 504, 408, 429],
        "DOWNLOAD_TIMEOUT":                30,
        "HTTPERROR_ALLOWED_CODES":         [404, 406],
        "REDIRECT_ENABLED":                True,
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
            "Accept-Encoding": "gzip, deflate",
        },
    }

    def __init__(self, website_id=None, start_url=None, crawl_log_id=None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.website_id   = int(website_id)   if website_id   else None
        self.crawl_log_id = int(crawl_log_id) if crawl_log_id else None

        # Derive site domain from start_url for dynamic URL building
        self._site_url    = (start_url or "https://zatpatmachines.com").rstrip("/")
        parsed            = urlparse(self._site_url)
        self._site_domain = parsed.netloc.lstrip("www.")
        self.start_urls   = [self._site_url]

        # Dynamically allow the target domain + all supabase domains
        self.allowed_domains = [self._site_domain, "supabase.co"]

        self._supabase_url:  str  = ""
        self._supabase_key:  str  = ""
        self._active_table:  str  = ""
        self._seen_ids:      set  = set()  # dedup by row id/sku within this run

        logger.info(
            f"[ZatPatMachinesSpider v={SPIDER_VERSION}] init — "
            f"website_id={self.website_id} site={self._site_domain}"
        )

    # =========================================================================
    # Phase 1 — Discover Supabase credentials from JS bundle
    # =========================================================================

    def start_requests(self):
        logger.info(f"[ZatPatMachinesSpider] fetching homepage: {self._site_url}")
        yield scrapy.Request(
            self._site_url,
            callback=self._parse_homepage,
            errback=self._homepage_errback,
        )

    def _parse_homepage(self, response):
        bundle_url = None
        for src in response.css("script[src]::attr(src)").getall():
            if re.search(r"/assets/index[^/]*\.js$", src):
                bundle_url = urljoin(self._site_url, src)
                break
        if not bundle_url:
            for src in response.css("script[src]::attr(src)").getall():
                if re.search(r"/assets/[^/]+\.js$|/static/js/[^/]+\.js$", src):
                    bundle_url = urljoin(self._site_url, src)
                    break

        if bundle_url:
            logger.info(f"JS bundle: {bundle_url}")
            yield scrapy.Request(
                bundle_url,
                callback=self._parse_js_bundle,
                errback=self._homepage_errback,
            )
        else:
            logger.warning("No JS bundle found — using fallback credentials")
            yield from self._start_api_crawl(_ZATPAT_SUPABASE_URL, _ZATPAT_SUPABASE_KEY)

    def _parse_js_bundle(self, response):
        if response.status != 200:
            logger.warning(f"Bundle HTTP {response.status} — using fallback credentials")
            yield from self._start_api_crawl(_ZATPAT_SUPABASE_URL, _ZATPAT_SUPABASE_KEY)
            return

        text = response.text
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
            logger.info(f"Supabase creds extracted: url={supabase_url}")
            # Allow this Supabase project domain
            self.allowed_domains.append(f"{url_m.group(1)}.supabase.co")
            yield from self._start_api_crawl(supabase_url, anon_key)
        else:
            logger.warning("Supabase creds not found in bundle — using fallback")
            yield from self._start_api_crawl(_ZATPAT_SUPABASE_URL, _ZATPAT_SUPABASE_KEY)

    def _homepage_errback(self, failure):
        logger.warning(f"Homepage/bundle error: {repr(failure.value)[:100]} — using fallback")
        yield from self._start_api_crawl(_ZATPAT_SUPABASE_URL, _ZATPAT_SUPABASE_KEY)

    # =========================================================================
    # Phase 2 — Table discovery: probe table names until one returns rows
    # =========================================================================

    def _start_api_crawl(self, supabase_url: str, anon_key: str):
        self._supabase_url = supabase_url
        self._supabase_key = anon_key
        logger.info(f"Starting table probe for {self._site_domain}")
        yield from self._probe_next_table(list(_TABLE_PROBES))

    def _probe_next_table(self, remaining_tables: list):
        """Try each table name with limit=1 to find which one has data."""
        if not remaining_tables:
            logger.error(f"No working table found for {self._site_domain} — giving up")
            return

        table = remaining_tables[0]
        url   = (
            f"{self._supabase_url}/rest/v1/{table}"
            f"?select=*&limit=1"
        )
        logger.info(f"Probing table: {table}")
        yield scrapy.Request(
            url,
            callback=self._parse_table_probe,
            errback=self._errback,
            headers={
                "apikey":        self._supabase_key,
                "Authorization": f"Bearer {self._supabase_key}",
                "Accept":        "application/json",
            },
            meta={"table": table, "remaining": remaining_tables[1:]},
            dont_filter=True,
        )

    def _parse_table_probe(self, response):
        table     = response.meta["table"]
        remaining = response.meta["remaining"]

        if response.status not in (200, 206):
            logger.debug(f"Table {table!r}: HTTP {response.status} — trying next")
            yield from self._probe_next_table(remaining)
            return

        try:
            rows = json.loads(response.text)
        except Exception:
            yield from self._probe_next_table(remaining)
            return

        if not isinstance(rows, list) or len(rows) == 0:
            logger.debug(f"Table {table!r}: empty or wrong shape — trying next")
            yield from self._probe_next_table(remaining)
            return

        # Check it looks like machine data (not just any table)
        first = rows[0] if rows else {}
        machine_signals = {
            "model_name", "title", "name", "brand", "price", "image",
            "sku_number", "sku", "machine_type", "description", "image_url",
        }
        if not any(k in first for k in machine_signals):
            logger.debug(f"Table {table!r}: found rows but no machine fields — trying next")
            yield from self._probe_next_table(remaining)
            return

        logger.info(f"Found working table: {table!r} for {self._site_domain}")
        self._active_table = table
        yield from self._fetch_page(offset=0)

    # =========================================================================
    # Phase 3 — Paginated API fetch
    # =========================================================================

    def _fetch_page(self, offset: int):
        # Use detailed zatpatmachines select for the machines table,
        # generic select for all others
        select = _ZATPAT_SELECT if self._active_table == "machines" else _GENERIC_SELECT

        # Try status filter for machines table; skip for unknown tables
        status_filter = "&status=eq.active" if self._active_table == "machines" else ""

        url = (
            f"{self._supabase_url}/rest/v1/{self._active_table}"
            f"?select={select}"
            f"{status_filter}"
            f"&order=id.asc"
            f"&limit={_PAGE_SIZE}"
            f"&offset={offset}"
        )
        logger.info(f"Fetching {self._active_table} offset={offset}")
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
        offset = response.meta.get("offset", 0)

        if response.status not in (200, 206):
            logger.error(f"API error: status={response.status} body={response.text[:200]!r}")
            return

        try:
            rows = json.loads(response.text)
        except Exception as exc:
            logger.error(f"JSON parse error (offset={offset}): {exc}")
            return

        if not isinstance(rows, list):
            if isinstance(rows, dict) and rows.get("message"):
                logger.error(f"Supabase error: {rows['message']}")
            return

        logger.info(f"Page offset={offset}: {len(rows)} rows")

        yielded = 0
        for row in rows:
            item = self._row_to_item(row)
            if item:
                yielded += 1
                yield item

        logger.info(f"Page offset={offset}: yielded {yielded}/{len(rows)}")

        if len(rows) == _PAGE_SIZE:
            yield from self._fetch_page(offset + _PAGE_SIZE)

    # =========================================================================
    # Phase 4 — Generic row → MachineItem
    # =========================================================================

    def _row_to_item(self, row: dict):
        if not isinstance(row, dict):
            return None

        def get(*keys):
            """Try multiple field name aliases, return first non-empty value."""
            for k in keys:
                v = row.get(k)
                if v and str(v).strip():
                    return str(v).strip()
            return ""

        # ── Unique ID — use sku_number, sku, id, or slug ──────────────────────
        uid = get("sku_number", "sku", "product_code", "reference_no")
        if not uid:
            uid = str(row.get("id") or row.get("_id") or "").strip()
        if not uid or uid in self._seen_ids:
            return None
        self._seen_ids.add(uid)

        # ── Brand ─────────────────────────────────────────────────────────────
        brand_raw = row.get("brands") or row.get("brand") or {}
        if isinstance(brand_raw, dict):
            brand = brand_raw.get("name", "")
        else:
            brand = str(brand_raw).strip()

        # ── Model / title ─────────────────────────────────────────────────────
        model = get("model_name", "model", "title", "name", "machine_name",
                    "product_name", "heading")

        title = f"{brand} {model}".strip() if brand else model
        if not title:
            return None

        # ── Machine type ──────────────────────────────────────────────────────
        type_raw     = row.get("machine_types") or row.get("machine_type") or {}
        machine_type = (
            type_raw.get("name") if isinstance(type_raw, dict) else str(type_raw or "")
        ) or None

        # ── Category ──────────────────────────────────────────────────────────
        cat_raw  = row.get("categories") or row.get("category") or {}
        category = (
            cat_raw.get("name") if isinstance(cat_raw, dict) else str(cat_raw or "")
        ) or None

        # ── Machine URL ───────────────────────────────────────────────────────
        # Use slug/url field if present, else build from site + uid
        slug = get("url", "slug", "product_url", "machine_url")
        if slug and slug.startswith("http"):
            machine_url = slug
        elif slug:
            machine_url = f"{self._site_url}/{slug}"
        else:
            # zatpatmachines pattern
            machine_url = f"{self._site_url}/machine/{uid}"

        # ── Images ────────────────────────────────────────────────────────────
        raw_imgs = row.get("machine_images") or []
        if isinstance(raw_imgs, list) and raw_imgs:
            sorted_imgs = sorted(
                [i for i in raw_imgs if isinstance(i, dict) and i.get("image_url")],
                key=lambda i: i.get("display_order", 999),
            )
            images = [i["image_url"] for i in sorted_imgs]
        else:
            main_img = get("main_image_url", "image_url", "image", "thumbnail",
                           "photo", "imageUrl")
            images = [main_img] if main_img and main_img.startswith("http") else []

        # ── Price ─────────────────────────────────────────────────────────────
        raw_price = row.get("price") or row.get("asking_price") or row.get("sale_price")
        try:
            price = float(raw_price) if raw_price is not None else None
        except (TypeError, ValueError):
            price = None

        currency = get("currency", "currency_code") or "INR"

        # ── Location ──────────────────────────────────────────────────────────
        city    = get("location_city",    "city")
        country = get("location_country", "country")
        location = ", ".join(p for p in (city, country) if p) or get("location") or None

        # ── Description ───────────────────────────────────────────────────────
        description = get("description", "details", "about")
        description = description[:2000] if description else None

        # ── Specs ─────────────────────────────────────────────────────────────
        specs: dict[str, str] = {}
        if row.get("year"):
            specs["Year"] = str(row["year"])
        if row.get("condition"):
            specs["Condition"] = str(row["condition"])
        specs["SKU"] = uid
        for spec_key in ("spec1_value", "spec2_value", "spec3_value"):
            val = row.get(spec_key)
            if val:
                label = spec_key.replace("_value", "").replace("spec", "Spec ").title()
                specs[label] = str(val)

        return MachineItem(
            machine_url    = machine_url,
            website_id     = self.website_id,
            website_source = self._site_domain,
            category       = category,
            machine_type   = machine_type,
            brand          = brand or None,
            model          = model or None,
            stock_number   = uid,
            price          = price,
            currency       = currency.strip().upper(),
            location       = location,
            description    = description,
            images         = images,
            specs          = specs,
        )

    # ── Error callback ────────────────────────────────────────────────────────

    def _errback(self, failure):
        logger.warning(
            f"Request error: {failure.request.url} — "
            f"{repr(failure.value)[:100]}"
        )
