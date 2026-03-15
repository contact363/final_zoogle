"""
CoreMachineSpider — Dedicated Scrapy spider for corelmachine.com
================================================================

Site: Corel Machinery (India) — Next.js App Router, Tailwind CSS
URL:  https://www.corelmachine.com

Crawl flow
──────────
Phase 1 – API Discovery
  • Fetch /api/subcategory/all  →  list of 24 subcategories
    Each entry: {_id, title, url (slug), image, category:{…}}
  • For each subcategory slug, probe multiple product-API patterns:
      /api/product/{slug}
      /api/product/subcategory/{slug}
      /api/products/{slug}
      /api/products?subcategory={slug}
      /api/products?url={slug}
  • Also schedule the category HTML page as a fallback.

Phase 2 – Category HTML page (CSR fallback)
  • The product grid loads via JavaScript — plain Scrapy sees an empty grid.
  • If Playwright is available, render with JS and extract product hrefs
    matching /usedmachinestocklist/{category}/{product-slug}.
  • If Playwright is NOT available, log a clear warning.

Phase 3 – Product detail pages (server-side rendered)
  • corelmachine.com detail pages are SSR — plain Scrapy works fine.
  • Extraction via specific DOM patterns:
      meta[name="description"]          → machine name / title
      meta[property="og:image"]         → image URL
      span.font-bold "Refrence No."     → stock number   (sic — site typo)
      span.font-bold "Capacity :"       → capacity
      span.font-bold "Year of construction" → year
      span.font-bold "Condition"        → condition
      span.font-bold "Location"         → location

Supplier info
─────────────
  Corel Machinery | 17/2, MIDC, Satpur, Nashik - 422007, India
  +91 98348 13425 | info@corelmachine.com
"""

import json
import logging
import re
from urllib.parse import urljoin, urlparse

import scrapy

from zoogle_crawler.items import MachineItem
from zoogle_crawler.spiders.generic_spider import GenericSpider

logger = logging.getLogger(__name__)

SPIDER_VERSION = "2026-03-15-v2"
BASE_URL       = "https://corelmachine.com"   # no www — API 404s on www variant

# Known 2-word brand prefixes (first word alone would misidentify the brand)
_TWO_WORD_BRANDS = (
    "MORI SEIKI", "DAEWOO DOOSAN", "HITACHI SEIKI", "DEAN SMITH",
    "OKUMA HOWA", "BROWN SHARPE", "JONES SHIPMAN", "NORTON GRINDERS",
    "KELLENBERGER VOUMARD", "FRITZ WERNER",
)

# URL segments that are site pages, not machine categories
_NON_CATEGORY_SLUGS = frozenset({
    "", "selltous", "aboutus", "contactus", "blogs",
    "privacy-policy", "terms-conditions", "cookie-policy",
    "marketing-opt-out", "sitemap",
})


class CoreMachineSpider(GenericSpider):
    """
    Dedicated spider for corelmachine.com.
    Inherits GenericSpider helpers (_infer_machine_type_from_text,
    _split_brand_model, JS renderer, etc.) but replaces the crawl flow
    with one tailored to this site's Next.js App Router architecture.

    Register as:
        scrapy crawl corelmachine -a website_id=<id> -a start_url=https://www.corelmachine.com
    """

    name = "corelmachine"
    allowed_domains = ["corelmachine.com", "www.corelmachine.com"]

    custom_settings = {
        "DEPTH_LIMIT":                     5,
        "CLOSESPIDER_ITEMCOUNT":           1000,
        "DOWNLOAD_DELAY":                  1.0,
        "RANDOMIZE_DOWNLOAD_DELAY":        True,
        "CONCURRENT_REQUESTS_PER_DOMAIN":  3,
        "ROBOTSTXT_OBEY":                  False,
        "RETRY_TIMES":                     3,
        "RETRY_HTTP_CODES":                [500, 502, 503, 504, 408, 429],
        "DOWNLOAD_TIMEOUT":                30,
        "HTTPERROR_ALLOWED_CODES":         [404],
        "REDIRECT_ENABLED":                True,
        "REDIRECT_MAX_TIMES":              10,
        "DOWNLOADER_MIDDLEWARES": {
            "scrapy.downloadermiddlewares.retry.RetryMiddleware":  None,
            "zoogle_crawler.middlewares.RetryWithBackoffMiddleware": 350,
            # SmartHeadersMiddleware is intentionally DISABLED for this spider.
            # It overwrites Accept: application/json with text/html on every
            # request, which causes the Next.js API routes to return empty body.
            "zoogle_crawler.middlewares.SmartHeadersMiddleware":     None,
            "zoogle_crawler.middlewares.BotDetectionMiddleware":     410,
            "zoogle_crawler.middlewares.RateLimiterMiddleware":      420,
            "zoogle_crawler.middlewares.ProxyMiddleware":            430,
        },
        # Static browser-like headers for all requests (no per-request rotation
        # needed — corelmachine.com does not enforce UA-based bot detection).
        "DEFAULT_REQUEST_HEADERS": {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
        },
    }

    def __init__(self, website_id=None, start_url=None, crawl_log_id=None,
                 training_rules=None, *args, **kwargs):
        # GenericSpider.__init__ sets self.website_id, self._base_domain,
        # self._js_available, etc.
        super().__init__(
            website_id=website_id,
            start_url=start_url or BASE_URL,
            crawl_log_id=crawl_log_id,
            training_rules=training_rules,
            *args, **kwargs,
        )
        self.name = "corelmachine"  # keep after super() which also sets it
        logger.info(
            f"[CoreMachineSpider v={SPIDER_VERSION}] init — "
            f"website_id={self.website_id} js={self._js_available}"
        )

    # ── Visited-URL helper (thin wrapper) ─────────────────────────────────────

    def _seen(self, url: str) -> bool:
        url = url.split("#")[0].rstrip("/")
        return self._is_visited(url)

    def _visit(self, url: str):
        url = url.split("#")[0].rstrip("/")
        self._mark_visited(url)

    # =========================================================================
    # Phase 1 — Entry point
    # =========================================================================

    def start_requests(self):
        """
        Seed the queue with:
          1. /api/subcategory/all  (JSON API — primary discovery)
          2. /usedmachinestocklist (SSR subcategory index — backup link scan)
        Deliberately skips the generic spider's path-blasting to avoid
        the 140+ 404s that pollute the crawl stats.
        """
        logger.info(f"[CoreMachineSpider v={SPIDER_VERSION}] start_requests")

        # 1. JSON subcategory API — probe both non-www and www so whichever
        #    the server resolves returns valid JSON (non-www is the working one).
        for api_base in ("https://corelmachine.com", "https://www.corelmachine.com"):
            api_url = f"{api_base}/api/subcategory/all"
            self._visit(api_url)
            yield scrapy.Request(
                api_url,
                callback=self._parse_subcategory_api,
                # SmartHeadersMiddleware is disabled so this header survives.
                headers={"Accept": "application/json, */*;q=0.5"},
                errback=self._errback,
                dont_filter=True,
            )

        # 2. Subcategory index page (plain SSR — link scan backup)
        index_url = f"{BASE_URL}/usedmachinestocklist"
        self._visit(index_url)
        yield scrapy.Request(
            index_url,
            callback=self._parse_subcategory_index,
            errback=self._errback,
        )

    # =========================================================================
    # Phase 1a — Subcategory JSON API
    # =========================================================================

    def _parse_subcategory_api(self, response):
        """
        Parse /api/subcategory/all.

        Expected shape:
          [{"_id": "...", "title": "CNC TURNING CENTERS", "url": "cnc-turning-centers",
            "image": "...", "status": true, "order": 1,
            "category": {"_id": "...", "title": "Engineering & Metal Works"},
            "description": "", "is_deleted": false}, ...]
        """
        ct   = self._content_type(response)
        body = (response.body or b"").strip()

        logger.info(
            f"[subcategory-api] status={response.status} "
            f"ct={ct[:50]!r} body_len={len(body)} "
            f"body_preview={body[:80]!r}"
        )

        if response.status != 200:
            logger.warning(f"Subcategory API HTTP {response.status} at {response.url}")
            return

        if not body:
            logger.error(
                f"Subcategory API returned empty body at {response.url}. "
                "This usually means the www-redirect stripped the response or "
                "the Accept header was wrong. Check DEFAULT_REQUEST_HEADERS."
            )
            return

        try:
            data = json.loads(body.decode("utf-8", errors="replace"))
        except Exception as exc:
            logger.error(
                f"Subcategory API JSON parse error at {response.url}: {exc} "
                f"— body preview: {body[:120]!r}"
            )
            return

        if not isinstance(data, list):
            logger.warning(f"Subcategory API: unexpected shape {type(data)}")
            return

        logger.info(f"Subcategory API: {len(data)} subcategories found")

        for entry in data:
            if not isinstance(entry, dict):
                continue
            slug       = (entry.get("url") or entry.get("slug") or "").strip()
            mongo_id   = (entry.get("_id") or "").strip()
            cat_title  = (entry.get("title") or slug.replace("-", " ").title()).strip()

            if not slug or slug in _NON_CATEGORY_SLUGS:
                continue

            # ── Try multiple product API patterns ─────────────────────────
            api_patterns = [
                f"{BASE_URL}/api/product/{slug}",
                f"{BASE_URL}/api/product/subcategory/{slug}",
                f"{BASE_URL}/api/products/{slug}",
                f"{BASE_URL}/api/products?subcategory={slug}&status=true",
                f"{BASE_URL}/api/products?url={slug}",
                f"{BASE_URL}/api/machine/{slug}",
                f"{BASE_URL}/api/machines?subcategory={slug}",
            ]
            if mongo_id:
                api_patterns += [
                    f"{BASE_URL}/api/product/{mongo_id}",
                    f"{BASE_URL}/api/products?category={mongo_id}",
                ]

            for api_url in api_patterns:
                if not self._seen(api_url):
                    self._visit(api_url)
                    yield scrapy.Request(
                        api_url,
                        callback=self._parse_product_api,
                        headers={"Accept": "application/json"},
                        errback=self._errback,
                        meta={"category": cat_title, "subcategory_slug": slug},
                    )

            # ── Schedule category HTML page (Playwright fallback) ─────────
            cat_url = f"{BASE_URL}/usedmachinestocklist/{slug}"
            if not self._seen(cat_url):
                self._visit(cat_url)
                yield scrapy.Request(
                    cat_url,
                    callback=self._parse_category_page,
                    errback=self._errback,
                    meta={"category": cat_title, "subcategory_slug": slug},
                )

    # =========================================================================
    # Phase 1b — Subcategory index page (SSR link scan)
    # =========================================================================

    def _parse_subcategory_index(self, response):
        """
        The /usedmachinestocklist page is SSR and contains <a> links for all
        24 subcategories.  Extract them and schedule category pages.
        """
        if response.status != 200:
            return

        for href in response.css("a::attr(href)").getall():
            full  = urljoin(BASE_URL, href).split("#")[0].rstrip("/")
            parts = urlparse(full).path.strip("/").split("/")

            # Category-level URL: /usedmachinestocklist/{slug}  (exactly 2 segments)
            if (
                len(parts) == 2
                and parts[0] == "usedmachinestocklist"
                and parts[1] not in _NON_CATEGORY_SLUGS
                and "corelmachine.com" in full
            ):
                slug = parts[1]
                if not self._seen(full):
                    self._visit(full)
                    yield scrapy.Request(
                        full,
                        callback=self._parse_category_page,
                        errback=self._errback,
                        meta={
                            "category": slug.replace("-", " ").title(),
                            "subcategory_slug": slug,
                        },
                    )

    # =========================================================================
    # Phase 1c — Product API response
    # =========================================================================

    def _parse_product_api(self, response):
        """
        Handle JSON responses from probed product API endpoints.
        Shape: list of product dicts  OR  {"data": [...]}  OR  empty.
        """
        ct         = self._content_type(response)
        body_start = response.text[:1].strip() if response.text else ""
        is_json    = body_start in ("[", "{") or "json" in ct

        if response.status != 200 or not is_json:
            return  # 404 / HTML — silently skip

        try:
            data = json.loads(response.text)
        except Exception:
            return

        category = response.meta.get("category", "")
        slug     = response.meta.get("subcategory_slug", "")

        # Unwrap common envelope shapes
        if isinstance(data, dict):
            for key in ("data", "results", "items", "products", "machines", "records"):
                if isinstance(data.get(key), list):
                    data = data[key]
                    break
            else:
                return  # Not a list — can't iterate as product list

        if not isinstance(data, list) or not data:
            return

        # Reject if it looks like a subcategory list (no price/year/ref fields)
        first = data[0] if isinstance(data[0], dict) else {}
        machine_signals = {"price", "year", "reference_no", "capacity",
                           "product_status", "stock_number", "condition"}
        if not any(f in first for f in machine_signals) and "url" in first and len(data) <= 50:
            # Probably another subcategory list — ignore
            logger.debug(f"Skipping subcategory-shaped response at {response.url}")
            return

        logger.info(
            f"Product API at {response.url}: "
            f"{len(data)} items (cat={category!r})"
        )

        for product in data:
            if not isinstance(product, dict):
                continue
            item, detail_url = self._product_dict_to_item(product, category, slug)
            if item:
                yield item
            if detail_url and not self._seen(detail_url):
                self._visit(detail_url)
                yield scrapy.Request(
                    detail_url,
                    callback=self._parse_detail_page,
                    errback=self._errback,
                    meta={"category": category},
                )

    # =========================================================================
    # Phase 2 — Category HTML page (CSR — Playwright needed)
    # =========================================================================

    def _parse_category_page(self, response):
        """
        /usedmachinestocklist/{slug} — product listing page.

        The product grid is loaded by React on the client (CSR).  Without JS
        rendering, the HTML contains an empty grid skeleton.

        Strategy:
          1. Scan the static HTML for any product-level hrefs (3 path segments).
             These appear if the page happens to be pre-rendered or SSR.
          2. If none found and Playwright is available, re-render with JS and
             scan again.
          3. If Playwright is not available, log a clear warning so the operator
             knows to install it.
        """
        if response.status != 200:
            return

        category = response.meta.get("category", "")
        slug     = response.meta.get("subcategory_slug", "")

        product_urls = self._extract_product_links_from_html(response)
        logger.info(
            f"Category page {slug!r}: "
            f"{len(product_urls)} product links in static HTML"
        )

        if product_urls:
            yield from self._schedule_product_pages(product_urls, category)
            return

        # Static HTML was empty — need JS rendering
        if self._js_available:
            yield from self._render_category_with_playwright(response, category, slug)
        else:
            logger.warning(
                f"[CoreMachineSpider] Category page {response.url} requires JS "
                "rendering but Playwright is not installed.  "
                "Run:  pip install playwright && playwright install chromium  "
                "to enable product discovery for this site."
            )

    def _extract_product_links_from_html(self, response) -> list[str]:
        """
        Extract hrefs that match the product URL pattern:
          /usedmachinestocklist/{category}/{product-slug}  (exactly 3 path segments)
        """
        found = []
        for href in response.css("a::attr(href)").getall():
            full  = urljoin(BASE_URL, href).split("#")[0].rstrip("/")
            parts = urlparse(full).path.strip("/").split("/")
            if (
                len(parts) == 3
                and parts[0] == "usedmachinestocklist"
                and "corelmachine.com" in full
            ):
                found.append(full)
        # Deduplicate, preserve order
        return list(dict.fromkeys(found))

    def _render_category_with_playwright(self, response, category: str, slug: str):
        """
        Use the shared JSRenderer to render the CSR category page and
        extract product links from the fully rendered DOM.
        """
        try:
            from zoogle_crawler.js_renderer import JSRenderer
            logger.info(f"Playwright rendering category: {response.url}")
            html = JSRenderer.render_sync(response.url, timeout_s=30)
            if not html:
                logger.warning(f"Playwright returned empty HTML for {response.url}")
                return

            fake = response.replace(body=html.encode("utf-8", errors="replace"))
            product_urls = self._extract_product_links_from_html(fake)
            logger.info(
                f"Playwright rendered {slug!r}: "
                f"{len(product_urls)} product links"
            )
            yield from self._schedule_product_pages(product_urls, category)

        except Exception as exc:
            logger.warning(f"Playwright render error at {response.url}: {exc}")

    def _schedule_product_pages(self, urls: list[str], category: str):
        for url in urls:
            if not self._seen(url):
                self._visit(url)
                yield scrapy.Request(
                    url,
                    callback=self._parse_detail_page,
                    errback=self._errback,
                    meta={"category": category},
                )

    # =========================================================================
    # Phase 3 — Product detail page (SSR — no JS needed)
    # =========================================================================

    def _parse_detail_page(self, response):
        """
        Full corelmachine.com detail-page extraction.

        Overrides GenericSpider._parse_detail_page so the Corel-specific
        label-based extractor runs first (fastest, most accurate for this site).
        Falls back to the generic pipeline only if the Corel extractor yields
        nothing (e.g. non-machine pages that somehow slipped through).
        """
        self._update_base_domain(response)
        try:
            items = list(self._extract_corel_detail(response))
            if items:
                yield from items
                return

            # Fallback to generic pipeline (JSON-LD → script JSON → CSS heuristics)
            yield from super()._parse_detail_page(response)

        except Exception as exc:
            logger.error(f"Detail page error at {response.url}: {exc}")

    def _extract_corel_detail(self, response):
        """
        Corel-specific extraction from a product detail page.
        Returns a single MachineItem or nothing.
        """
        if response.status != 200:
            return

        url      = response.url
        category = response.meta.get("category", "")

        # ── Skip clearly non-machine pages ────────────────────────────────────
        skip_words = {"contact", "about", "blog", "privacy", "terms", "career",
                      "sitemap", "selltous", "marketing-opt-out"}
        if any(w in url.lower() for w in skip_words):
            return

        # ── Title ─────────────────────────────────────────────────────────────
        # meta[name="description"] contains the clean machine name.
        # og:title is the verbose SEO title — use as fallback.
        title = (
            response.css("meta[name='description']::attr(content)").get("").strip()
            or response.css("meta[property='og:description']::attr(content)").get("").strip()
        )

        if not title:
            # Try to strip SEO prefix from og:title
            og_title = response.css("meta[property='og:title']::attr(content)").get("").strip()
            if og_title:
                for sep in (" | ", " - ", " – ", " — "):
                    if sep in og_title:
                        title = og_title.split(sep)[0].strip()
                        # Remove "Buy Used " prefix
                        if title.lower().startswith("buy used "):
                            title = title[9:].strip()
                        break
                else:
                    title = og_title

        if not title:
            # Last resort: h1 text
            h1_parts = response.css("h1::text, h1 *::text").getall()
            title = self.clean_text(h1_parts) or ""

        if not title or len(title) < 3:
            return

        # ── Image ─────────────────────────────────────────────────────────────
        image_url = (
            response.css("meta[property='og:image']::attr(content)").get("").strip()
            or response.css("img[src*='corel-images.s3']::attr(src)").get("").strip()
        )
        images = [image_url] if image_url else []

        # ── Labeled fields ────────────────────────────────────────────────────
        # Structure on the page:
        #   <div class="py-2">
        #     <span class="font-bold">Label text </span> plain text value
        #   </div>
        stock_number  = self._corel_label(response, "Refrence No")   # site typo
        capacity      = self._corel_label(response, "Capacity")
        year_raw      = self._corel_label(response, "Year of construction")
        condition     = self._corel_label(response, "Condition")
        location      = self._corel_label(response, "Location")
        page_category = self._corel_label(response, "Category") or category

        # ── Year ──────────────────────────────────────────────────────────────
        year: int | None = None
        if year_raw:
            m = re.search(r"\b(19|20)\d{2}\b", year_raw)
            year = int(m.group()) if m else None

        # ── Brand / model ─────────────────────────────────────────────────────
        brand, model = self._split_brand_model_corel(title)

        # ── Specs dict ────────────────────────────────────────────────────────
        specs: dict[str, str] = {}
        if capacity:
            specs["Capacity"] = capacity
        if stock_number:
            specs["Stock Number"] = stock_number
        if condition:
            specs["Condition"] = condition
        if year_raw:
            specs["Year of Construction"] = year_raw

        # Mine additional "Key : Value" pairs from the page body
        body_chunks = response.css(
            "main *::text, article *::text, "
            "div[class*='container'] *::text, "
            "div[class*='py-'] *::text"
        ).getall()
        body_text = "\n".join(t.strip() for t in body_chunks if t.strip())
        for line in body_text.split("\n"):
            if " : " in line and len(line) < 200:
                k, _, v = line.partition(" : ")
                k = k.strip(" :")
                v = v.strip()
                if k and v and 2 < len(k) < 60 and 1 < len(v) < 150:
                    specs.setdefault(k, v)

        machine_type = self._infer_machine_type_from_text(
            f"{title} {page_category} {url}"
        )

        logger.debug(
            f"[corel-detail] {title!r} "
            f"brand={brand!r} model={model!r} year={year} url={url}"
        )

        yield MachineItem(
            machine_url=url,
            website_id=self.website_id,
            website_source="corelmachine.com",
            category=page_category,
            machine_type=machine_type,
            brand=brand,
            model=model,
            price=None,          # site shows "Ask for Quotation" — no price
            currency="INR",
            location=location or "Nashik, India",
            description=None,
            images=images,
            specs=specs,
        )

    # =========================================================================
    # Extraction helpers
    # =========================================================================

    def _corel_label(self, response, label_text: str) -> str:
        """
        Extract the value for a labeled field on corelmachine.com detail pages.

        HTML structure:
            <div class="py-2">
              <span class="font-bold">Refrence No. </span> 400161
            </div>

        The value is a bare TEXT NODE inside the parent <div>, not a child element.
        Strategy: find the label span, get the parent's full text, strip the label.
        """
        label_lower = label_text.lower()
        for span in response.css("span.font-bold, span[class*='font-bold']"):
            span_text = (self.clean_text(span.css("::text").getall()) or "").strip()
            if label_lower not in span_text.lower():
                continue
            # Collect all text inside the parent element
            parent_text = (
                self.clean_text(span.xpath("..//text()").getall()) or ""
            ).strip()
            # Strip the label prefix and clean up punctuation
            val = parent_text.replace(span_text, "", 1).strip(" :/")
            if val and len(val) < 300:
                return val
        return ""

    def _split_brand_model_corel(self, title: str) -> tuple[str, str]:
        """
        corelmachine.com titles are ALL-CAPS: "MAZAK QT28N CNC LATHE".
        First word = brand (or first two words for known two-word brands).
        Second word = model.
        """
        if not title:
            return "", ""
        title_upper = title.upper()
        for brand in _TWO_WORD_BRANDS:
            if title_upper.startswith(brand):
                rest  = title[len(brand):].strip()
                model = rest.split()[0] if rest.split() else ""
                return brand.title(), model
        parts = title.split()
        brand = parts[0].title() if parts else ""
        model = parts[1] if len(parts) > 1 else ""
        return brand, model

    def _product_dict_to_item(
        self, d: dict, category: str, slug: str
    ) -> tuple[MachineItem | None, str | None]:
        """
        Convert a product record from the JSON API to a MachineItem.
        Returns (item | None, detail_url | None).
        """
        def get(*keys):
            for k in keys:
                for variant in (k, k.lower(), k.upper()):
                    v = d.get(variant)
                    if v and isinstance(v, (str, int, float)):
                        return str(v).strip()
            return None

        title = get("title", "name", "machineName", "productName", "heading")
        if not title or len(title) < 3:
            return None, None

        url_slug   = get("url", "slug", "product_url")
        detail_url: str | None = None
        if url_slug:
            if url_slug.startswith("http"):
                detail_url = url_slug
            else:
                detail_url = f"{BASE_URL}/usedmachinestocklist/{slug}/{url_slug}"
        else:
            detail_url = f"{BASE_URL}/usedmachinestocklist/{slug}"

        image_raw = get("image", "imageUrl", "thumbnail", "photo")
        images    = [image_raw] if image_raw and image_raw.startswith("http") else []

        brand, model = self._split_brand_model_corel(title)
        machine_type = self._infer_machine_type_from_text(f"{title} {category}")

        item = MachineItem(
            machine_url=detail_url or BASE_URL,
            website_id=self.website_id,
            website_source="corelmachine.com",
            category=category,
            machine_type=machine_type,
            brand=brand,
            model=model,
            price=None,
            currency="INR",
            location="Nashik, India",
            description=None,
            images=images,
            specs={},
        )
        return item, detail_url

    # ── Utility ───────────────────────────────────────────────────────────────

    @staticmethod
    def _content_type(response) -> str:
        return (
            response.headers.get("Content-Type", b"") or b""
        ).decode("utf-8", errors="ignore").lower()
