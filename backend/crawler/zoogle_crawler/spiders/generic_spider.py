"""
Zoogle Adaptive Spider  (generic)
══════════════════════════════════

Architecture
────────────
Phase 1 – Discovery
  • Fetch /sitemap.xml (+ sitemap index) for a full URL dump.
  • Walk every nav / header / sidebar / footer link for category pages.
  • Score every URL and dispatch to the correct handler.

Phase 2 – Listing / Category Pages
  • Detect 60+ card patterns and yield partial MachineItems immediately.
  • Schedule every card's detail URL for full extraction.
  • Follow 30+ pagination patterns (rel=next, page=N, offset=N, /page/N/).
  • Persist crawling on per-page errors — one bad page never kills the run.

Phase 3 – Detail Pages
  • Try in order: JSON-LD → embedded JS state → CSS heuristics.
  • If the page is a JS-rendered SPA (empty body), optionally re-render
    with a headless Playwright subprocess and re-run extraction.

Anti-blocking
─────────────
  • SmartHeadersMiddleware rotates 30+ UAs with full browser fingerprints.
  • RetryWithBackoffMiddleware does exponential back-off + Retry-After.
  • BotDetectionMiddleware flags Cloudflare / captcha pages.
  • RateLimiterMiddleware enforces per-domain cooldowns.
  • Per-request referer chaining.
  • Randomised delay (DOWNLOAD_DELAY + RANDOMIZE_DOWNLOAD_DELAY).

Memory safety
─────────────
  • _visited set is capped at MAX_VISITED (50 k) — oldest entries evicted.
  • Playwright runs in an isolated subprocess (zero RAM cost to main process).
  • Image downloads limited to 3 per machine (IMAGES_STORE-independent).
"""

import re
import json
import logging
from collections import deque
from urllib.parse import urlparse, urljoin

import scrapy
from scrapy.http import Response, TextResponse

from zoogle_crawler.items import MachineItem
from zoogle_crawler.spiders.base_spider import BaseZoogleSpider
from zoogle_crawler.page_analyzer import (
    skip_url, score_url, is_detail_url, is_listing_url, is_worth_following,
    same_domain, looks_like_pagination_url, build_page_url,
    NAV_SELECTORS, CARD_SELECTORS,
    CARD_TITLE_SELECTORS, CARD_PRICE_SELECTORS, CARD_LOCATION_SELECTORS,
    CARD_LINK_SELECTORS, CARD_IMAGE_SELECTORS,
    TITLE_SELECTORS, PRICE_SELECTORS, DESCRIPTION_SELECTORS,
    LOCATION_SELECTORS, IMAGE_SELECTORS, PAGINATION_SELECTORS,
    SPEC_TABLE_SELECTORS, BREADCRUMB_SELECTORS,
)

logger = logging.getLogger(__name__)

# Maximum number of URLs to keep in the visited set.
# Once exceeded, the oldest 10 % are removed to free memory.
MAX_VISITED = 50_000
EVICT_CHUNK  = 5_000  # remove this many when limit hit


# ─────────────────────────────────────────────────────────────────────────────
class GenericSpider(BaseZoogleSpider):
    """
    One spider for any industrial-machine website.
    Pass website_id, start_url, and crawl_log_id as Scrapy -a arguments.
    """

    name = "generic"

    custom_settings = {
        "DEPTH_LIMIT": 8,
        "CLOSESPIDER_ITEMCOUNT": 5000,
        "DOWNLOAD_DELAY": 0.5,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "ROBOTSTXT_OBEY": False,
        "RETRY_TIMES": 4,
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 408, 429, 403, 520, 521, 522, 524],
        "DOWNLOAD_TIMEOUT": 30,
        # Cap response size at 10 MB to prevent RAM spikes on huge HTML blobs
        "DOWNLOAD_MAXSIZE": 10 * 1024 * 1024,
        # Pass ALL HTTP responses (including 404) to spider callbacks — so
        # _parse_sitemap, _parse_listing_page etc. can handle them gracefully
        # instead of having them silently dropped by HttpErrorMiddleware.
        "HTTPERROR_ALLOWED_CODES": [404, 410],
        # Use our enhanced middlewares (override settings.py order if needed)
        "DOWNLOADER_MIDDLEWARES": {
            "scrapy.downloadermiddlewares.retry.RetryMiddleware": None,  # disable default
            "zoogle_crawler.middlewares.RetryWithBackoffMiddleware": 350,
            "zoogle_crawler.middlewares.SmartHeadersMiddleware":     400,
            "zoogle_crawler.middlewares.BotDetectionMiddleware":     410,
            "zoogle_crawler.middlewares.RateLimiterMiddleware":      420,
            "zoogle_crawler.middlewares.ProxyMiddleware":            430,
        },
    }

    def __init__(self, website_id=None, start_url=None, crawl_log_id=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.website_id  = int(website_id)  if website_id   else None
        self.crawl_log_id = int(crawl_log_id) if crawl_log_id else None
        self.start_urls  = [start_url] if start_url else []
        self._base_domain = urlparse(start_url).netloc if start_url else ""
        # Deque for O(1) insertion + ordered eviction
        self._visited_order: deque[str] = deque()
        self._visited: set[str] = set()

        # Check Playwright availability once at startup (informational only)
        from zoogle_crawler.js_renderer import JSRenderer
        self._js_available = JSRenderer.is_available()
        if self._js_available:
            logger.info("Playwright detected — JS fallback enabled")
        else:
            logger.info("Playwright not installed — JS fallback disabled")

    # ── Visited-URL tracking (memory-capped) ─────────────────────────────────

    def _mark_visited(self, url: str):
        if url in self._visited:
            return
        self._visited.add(url)
        self._visited_order.append(url)
        if len(self._visited) > MAX_VISITED:
            # Evict oldest chunk
            for _ in range(EVICT_CHUNK):
                if self._visited_order:
                    self._visited.discard(self._visited_order.popleft())

    def _is_visited(self, url: str) -> bool:
        return url in self._visited

    # ── Phase 1: Entry point ──────────────────────────────────────────────────

    def start_requests(self):
        """
        Seed the crawl queue with:
          1. The homepage (→ parse callback)
          2. Common sitemap paths — sent immediately so they're crawled
             even if parse() is slow or the homepage is a JS SPA.
          3. Common machine listing paths for sites without sitemaps.
        Sending these here (not inside parse()) guarantees they enter the
        Scrapy scheduler regardless of what the homepage response looks like.
        """
        start_url = self.start_urls[0] if self.start_urls else None
        if not start_url:
            return

        # 1. Homepage
        yield scrapy.Request(start_url, callback=self.parse, errback=self._errback)

        # 2. Sitemaps — dont_filter=True so they bypass duplicate filter
        base = start_url.rstrip("/")
        for path in ("/sitemap.xml", "/sitemap_index.xml", "/sitemap/",
                     "/sitemap-0.xml", "/page-sitemap.xml", "/product-sitemap.xml"):
            url = base + path
            if not self._is_visited(url):
                self._mark_visited(url)
                yield scrapy.Request(
                    url, callback=self._parse_sitemap,
                    dont_filter=True, errback=self._errback,
                )

        # 3. Common machine listing paths (handles sites with no sitemap)
        for path in (
            "/machines", "/maschinen", "/inventory", "/used-machines",
            "/gebrauchtmaschinen", "/search", "/catalog", "/listings",
            "/products", "/equipment", "/stock", "/shop",
            "/machines/", "/maschinen/", "/inventory/", "/catalog/",
        ):
            url = base + path
            if not self._is_visited(url):
                self._mark_visited(url)
                yield scrapy.Request(
                    url, callback=self._parse_listing_page,
                    errback=self._errback,
                    meta={"referer": start_url},
                )

    def parse(self, response: Response):
        """Homepage: process the page and follow all links."""
        yield from self._process_page(response)

    # ── Phase 1a: Sitemap ─────────────────────────────────────────────────────

    def _parse_sitemap(self, response: Response):
        """Parse sitemap.xml or a sitemap index. Silently skips non-XML responses."""
        try:
            # Skip if not an XML/text response (e.g. HTML 404 page returned for missing sitemap)
            content_type = (response.headers.get("Content-Type", b"") or b"").decode("utf-8", errors="ignore").lower()
            body = response.text

            # If this looks like an HTML page (e.g. 404 or redirect), treat as listing page
            if response.status != 200 or ("<html" in body[:500].lower() and "<loc>" not in body):
                if response.status == 200:
                    logger.debug(f"Sitemap URL returned HTML, treating as listing page: {response.url}")
                    yield from self._parse_listing_page(response)
                return

            # Sitemap index → recurse into child sitemaps
            for url in re.findall(r"<sitemap>\s*<loc>(.*?)</loc>", body, re.DOTALL):
                url = url.strip()
                if url and not self._is_visited(url):
                    self._mark_visited(url)
                    yield scrapy.Request(
                        url, callback=self._parse_sitemap,
                        dont_filter=True, errback=self._errback,
                    )

            # Regular <loc> entries
            for url in re.findall(r"<loc>(.*?)</loc>", body, re.DOTALL):
                url = url.strip()
                if not url or self._is_visited(url):
                    continue
                if not same_domain(url, self._base_domain):
                    continue
                if skip_url(url):
                    continue

                self._mark_visited(url)
                s = score_url(url)
                if s >= 5:
                    yield scrapy.Request(url, callback=self._parse_detail_page,
                                         errback=self._errback)
                else:
                    yield scrapy.Request(url, callback=self._parse_listing_page,
                                         errback=self._errback)
        except Exception as exc:
            logger.error(f"Sitemap parse error at {response.url}: {exc}")

    # ── Dispatch ──────────────────────────────────────────────────────────────

    def _process_page(self, response: Response):
        """Route any response to the correct handler."""
        try:
            url = response.url
            if is_detail_url(url) and self._looks_like_detail(response):
                yield from self._parse_detail_page(response)
            else:
                yield from self._parse_listing_page(response)
        except Exception as exc:
            logger.error(f"Process page error at {response.url}: {exc}")

    # ── Phase 2: Listing / Category page ──────────────────────────────────────

    def _parse_listing_page(self, response: Response):
        """
        Extract machine cards, follow detail links, follow pagination.
        Wrapped in try/except so one bad listing page never halts the crawl.
        """
        try:
            category = self._extract_category(response)
            yield from self._extract_machine_cards(response, category)
            yield from self._extract_jsonld(response)
            yield from self._follow_all_links(response)
            yield from self._follow_pagination(response)
        except Exception as exc:
            logger.error(f"Listing page error at {response.url}: {exc}")

    # ── Category extraction ───────────────────────────────────────────────────

    def _extract_category(self, response: Response) -> str | None:
        """
        Derive the machine category from the page context:
        breadcrumbs → <title> → URL path segments → meta keywords.
        """
        # 1. Breadcrumbs
        crumbs = [t.strip() for t in response.css(
            ", ".join(BREADCRUMB_SELECTORS)
        ).getall() if t.strip()]
        if len(crumbs) >= 2:
            # Last meaningful crumb before the current page is usually the category
            category = crumbs[-2] if len(crumbs) >= 3 else crumbs[-1]
            if 3 < len(category) < 60:
                return category

        # 2. <title> first word cluster
        title_tag = response.css("title::text").get("").strip()
        if title_tag and 4 < len(title_tag) < 60:
            # Remove the site name suffix (often after " | " or " - ")
            for sep in (" | ", " - ", " – ", " — ", " :: "):
                if sep in title_tag:
                    title_tag = title_tag.split(sep)[0].strip()
                    break
            return title_tag or None

        # 3. URL path last segment
        path = urlparse(response.url).path.rstrip("/")
        if path:
            segment = path.split("/")[-1].replace("-", " ").replace("_", " ").title()
            if 3 < len(segment) < 60:
                return segment

        return None

    # ── Phase 2a: Extract machine cards ───────────────────────────────────────

    def _extract_machine_cards(self, response: Response, category: str | None = None):
        """
        Detect product-card elements and yield partial MachineItems from them.
        Schedules detail-page requests for full extraction.
        """
        for container_sel in CARD_SELECTORS:
            try:
                cards = response.css(container_sel)
                if not cards:
                    continue

                extracted_any = False
                for card in cards:
                    try:
                        item = self._extract_card(card, response, category)
                        if item:
                            extracted_any = True
                            yield item
                            # Schedule full detail extraction
                            detail_url = item["machine_url"]
                            if not self._is_visited(detail_url):
                                self._mark_visited(detail_url)
                                yield response.follow(
                                    detail_url,
                                    callback=self._parse_detail_page,
                                    errback=self._errback,
                                    meta={"referer": response.url},
                                )
                    except Exception as exc:
                        logger.debug(f"Card extraction error: {exc}")
                        continue

                if extracted_any:
                    # Found the right card selector — stop trying others
                    return

            except Exception as exc:
                logger.debug(f"Card selector '{container_sel}' error: {exc}")
                continue

    def _extract_card(self, card, response: Response, category: str | None) -> MachineItem | None:
        """Extract a MachineItem from a single card element."""
        # Link is required to identify the machine
        link = None
        for sel in CARD_LINK_SELECTORS:
            link = card.css(sel).get()
            if link:
                break
        if not link:
            return None

        full_url = urljoin(response.url, link.strip()).split("#")[0]
        if not same_domain(full_url, self._base_domain) or skip_url(full_url):
            return None

        # Title is required
        title = None
        for sel in CARD_TITLE_SELECTORS:
            title = self.clean_text(card.css(sel).getall())
            if title and len(title) > 2:
                break
        if not title:
            return None

        # Price
        price_raw = None
        for sel in CARD_PRICE_SELECTORS:
            price_raw = self.clean_text(card.css(sel).getall())
            if price_raw:
                break
        price, currency = self.extract_price(price_raw or "")

        # Location
        location = None
        for sel in CARD_LOCATION_SELECTORS:
            location = self.clean_text(card.css(sel).getall())
            if location:
                break

        # Image (with lazy-load support)
        images = []
        for sel in CARD_IMAGE_SELECTORS:
            img = card.css(sel).get()
            if img and not img.startswith("data:"):
                images = self.normalize_image_urls([img], response.url)
                break

        brand, model = self._split_brand_model(title)
        machine_type = self._infer_machine_type_from_text(
            f"{title} {category or ''} {response.url}"
        ) or (category if category and self._looks_like_type(category) else None)

        return MachineItem(
            machine_url=full_url,
            website_id=self.website_id,
            website_source=self._base_domain,
            category=category,
            machine_type=machine_type,
            brand=brand,
            model=model,
            price=price,
            currency=currency,
            location=location,
            description=None,
            images=images,
            specs={},
        )

    # ── Phase 3: Detail page ──────────────────────────────────────────────────

    def _parse_detail_page(self, response: Response):
        """
        Full extraction pipeline:
        1. JSON-LD
        2. Embedded JS state (__NEXT_DATA__, window.__INITIAL_STATE__, etc.)
        3. CSS heuristics
        4. [Optional] JS render + retry CSS heuristics
        """
        try:
            # 1. JSON-LD (fastest, most structured)
            jsonld = list(self._extract_jsonld(response))
            if jsonld:
                yield from jsonld
                return

            # 2. Embedded script JSON
            script_items = list(self._extract_script_json(response))
            if script_items:
                yield from script_items
                return

            # 3. CSS heuristics
            css_items = list(self._parse_detail_css(response))
            if css_items:
                yield from css_items
                return

            # 4. Page looks empty — try JS rendering if available
            if self._js_available and self._needs_js(response):
                yield from self._try_js_render(response)

        except Exception as exc:
            logger.error(f"Detail page error at {response.url}: {exc}")

    def _needs_js(self, response: Response) -> bool:
        """Check if the page body is too sparse to have been server-rendered."""
        from zoogle_crawler.js_renderer import is_js_page
        return is_js_page(response.text)

    def _try_js_render(self, response: Response):
        """Render the page with Playwright and re-run CSS heuristics."""
        try:
            from zoogle_crawler.js_renderer import JSRenderer
            logger.info(f"JS render attempt: {response.url}")
            rendered_html = JSRenderer.render_sync(response.url, timeout_s=25)
            if rendered_html:
                fake = response.replace(body=rendered_html.encode("utf-8", errors="replace"))
                # Try all extraction methods again on the rendered HTML
                jsonld = list(self._extract_jsonld(fake))
                if jsonld:
                    yield from jsonld
                    return
                yield from self._parse_detail_css(fake)
        except Exception as exc:
            logger.warning(f"JS render pipeline error at {response.url}: {exc}")

    def _looks_like_detail(self, response: Response) -> bool:
        has_h1    = bool(response.css("h1").get())
        has_price = any(response.css(s).get() for s in PRICE_SELECTORS[:6])
        has_image = bool(response.css("img[src]").get())
        return has_h1 and (has_price or has_image)

    # ── JSON-LD extraction ────────────────────────────────────────────────────

    def _extract_jsonld(self, response: Response):
        for script in response.css("script[type='application/ld+json']::text").getall():
            try:
                data = json.loads(script)
            except (json.JSONDecodeError, ValueError):
                continue
            try:
                nodes = data if isinstance(data, list) else data.get("@graph", [data])
                for node in nodes:
                    if not isinstance(node, dict):
                        continue
                    node_type = node.get("@type", "")
                    if isinstance(node_type, list):
                        node_type = " ".join(node_type)
                    if any(t in node_type for t in ("Product", "Offer", "Vehicle", "Thing")):
                        item = self._jsonld_to_item(node, response.url)
                        if item:
                            yield item
                    elif "ItemList" in node_type:
                        for element in node.get("itemListElement", []):
                            if isinstance(element, dict):
                                item = self._jsonld_to_item(
                                    element.get("item", element), response.url
                                )
                                if item:
                                    yield item
            except Exception as exc:
                logger.debug(f"JSON-LD parse error: {exc}")

    def _jsonld_to_item(self, node: dict, page_url: str) -> MachineItem | None:
        name = node.get("name") or node.get("title") or ""
        if not name or len(str(name)) < 3:
            return None

        # Price
        price, currency = None, "USD"
        offers = node.get("offers") or node.get("Offers")
        if offers:
            if isinstance(offers, list):
                offers = offers[0]
            if isinstance(offers, dict):
                raw = offers.get("price") or offers.get("lowPrice")
                if raw:
                    try:
                        price = float(str(raw).replace(",", ""))
                    except ValueError:
                        pass
                currency = offers.get("priceCurrency", "USD")

        # URL
        url = node.get("url") or node.get("@id") or page_url
        if not str(url).startswith("http"):
            url = urljoin(page_url, str(url))

        # Description
        description = str(node.get("description") or "")[:2000] or None

        # Images
        images = []
        img = node.get("image")
        if isinstance(img, str):
            images = [img]
        elif isinstance(img, list):
            images = [i if isinstance(i, str) else (i.get("url", "") if isinstance(i, dict) else "") for i in img]
        elif isinstance(img, dict):
            images = [img.get("url", "")]
        images = self.normalize_image_urls([i for i in images if i], page_url)

        # Brand
        brand_raw = None
        brand_obj = node.get("brand") or node.get("manufacturer")
        if isinstance(brand_obj, str):
            brand_raw = brand_obj
        elif isinstance(brand_obj, dict):
            brand_raw = brand_obj.get("name")

        # Location
        location = None
        loc = node.get("location") or node.get("address") or node.get("areaServed")
        if isinstance(loc, str):
            location = loc
        elif isinstance(loc, dict):
            parts = [
                loc.get("addressLocality", ""),
                loc.get("addressRegion", ""),
                loc.get("addressCountry", ""),
            ]
            location = ", ".join(p for p in parts if p) or None

        brand, model = self._split_brand_model(str(name), brand_raw)
        machine_type = self._infer_machine_type_from_text(
            f"{name} {description or ''} {url}"
        )

        return MachineItem(
            machine_url=str(url),
            website_id=self.website_id,
            website_source=urlparse(str(url)).netloc or self._base_domain,
            category=None,
            machine_type=machine_type,
            brand=brand,
            model=model,
            price=price,
            currency=currency,
            location=location,
            description=description,
            images=images,
            specs={},
        )

    # ── Embedded script JSON ──────────────────────────────────────────────────

    def _extract_script_json(self, response: Response):
        """Extract machine data from __NEXT_DATA__, window.__INITIAL_STATE__, etc."""
        for script in response.css("script:not([src])::text").getall():
            if len(script) < 100:
                continue
            data = None

            # Next.js
            m = re.search(r'__NEXT_DATA__["\']?\s*[=:]\s*(\{.*?\})\s*[;<]',
                           script, re.DOTALL)
            if m:
                try:
                    data = json.loads(m.group(1))
                except Exception:
                    pass

            # window.__INITIAL_STATE__ / window.__STATE__
            if not data:
                m2 = re.search(r'window\.__(?:INITIAL_)?STATE__\s*=\s*(\{.*?\});',
                                script, re.DOTALL)
                if m2:
                    try:
                        data = json.loads(m2.group(1))
                    except Exception:
                        pass

            # Nuxt.js __NUXT__
            if not data:
                m3 = re.search(r'window\.__NUXT__\s*=\s*(\{.*?\});', script, re.DOTALL)
                if m3:
                    try:
                        data = json.loads(m3.group(1))
                    except Exception:
                        pass

            # window.dataLayer
            if not data:
                m4 = re.search(r'window\.dataLayer\s*=\s*(\[.*?\]);', script, re.DOTALL)
                if m4:
                    try:
                        data = json.loads(m4.group(1))
                    except Exception:
                        pass

            if data:
                yield from self._dig_for_machines(data, response.url)

    def _dig_for_machines(self, obj, page_url: str, depth: int = 0):
        if depth > 8:
            return
        _MACHINE_KEYS = {"machine", "brand", "model", "price", "equipment",
                          "listing", "product", "name", "title", "sku"}
        if isinstance(obj, dict):
            keys_lower = {k.lower() for k in obj.keys()}
            if len(keys_lower & _MACHINE_KEYS) >= 3:
                item = self._dict_to_item(obj, page_url)
                if item:
                    yield item
            for v in obj.values():
                yield from self._dig_for_machines(v, page_url, depth + 1)
        elif isinstance(obj, list):
            for elem in obj[:200]:  # cap to avoid huge lists
                yield from self._dig_for_machines(elem, page_url, depth + 1)

    def _dict_to_item(self, d: dict, page_url: str) -> MachineItem | None:
        def get(*keys):
            for k in keys:
                for variant in (k, k.lower(), k.upper(), k.title()):
                    v = d.get(variant)
                    if v and isinstance(v, (str, int, float)):
                        return str(v).strip()
            return None

        name = get("name", "title", "machineName", "productName", "listingTitle", "heading")
        if not name or len(name) < 4:
            return None

        url = get("url", "link", "href", "machineUrl", "listingUrl", "productUrl") or page_url
        if not str(url).startswith("http"):
            url = urljoin(page_url, str(url))

        price_raw = get("price", "askingPrice", "listPrice", "salePrice", "sellingPrice")
        price, currency = self.extract_price(price_raw or "")

        brand_raw  = get("brand", "manufacturer", "make", "brandName")
        model_raw  = get("model", "modelNumber", "modelName", "partNumber")
        location   = get("location", "city", "country", "address", "region", "state")
        description = get("description", "details", "summary", "body")

        brand, model = self._split_brand_model(name, brand_raw)
        if model_raw and not model:
            model = model_raw

        return MachineItem(
            machine_url=str(url),
            website_id=self.website_id,
            website_source=urlparse(str(url)).netloc or self._base_domain,
            category=None,
            machine_type=self._infer_machine_type_from_text(f"{name} {description or ''}"),
            brand=brand,
            model=model,
            price=price,
            currency=currency,
            location=location,
            description=description[:2000] if description else None,
            images=[],
            specs={},
        )

    # ── CSS heuristic detail extraction ───────────────────────────────────────

    def _parse_detail_css(self, response: Response):
        """Last-resort extraction via CSS selectors."""
        # Title is required
        title = None
        for sel in TITLE_SELECTORS:
            title = self.clean_text(response.css(sel).getall())
            if title and len(title) > 2:
                break
        if not title:
            return

        # Price
        price_raw = None
        for sel in PRICE_SELECTORS:
            price_raw = self.clean_text(response.css(sel).getall())
            if price_raw:
                break
        price, currency = self.extract_price(price_raw or "")

        # Location
        location = None
        for sel in LOCATION_SELECTORS:
            location = self.clean_text(response.css(sel).getall())
            if location:
                break

        # Description
        description = None
        for sel in DESCRIPTION_SELECTORS:
            description = self.clean_text(response.css(sel).getall())
            if description and len(description) > 20:
                break

        # Images — merge all sources, including lazy-load attrs
        image_urls: list[str] = []
        for sel in IMAGE_SELECTORS:
            found = [u for u in response.css(sel).getall()
                     if u and not u.startswith("data:")]
            image_urls.extend(found)
            if len(image_urls) >= 8:
                break
        # Also check all <img> elements for any data-* lazy src
        for attr in ("data-src", "data-lazy", "data-lazy-src", "data-original",
                     "data-image", "data-zoom-image", "data-full"):
            image_urls.extend(
                u for u in response.css(f"img::{attr}").getall()
                if u and not u.startswith("data:")
            )
        image_urls = self.normalize_image_urls(image_urls, response.url)

        # Specs — table rows first, then definition lists
        specs: dict = {}
        for sel in SPEC_TABLE_SELECTORS:
            specs = self.parse_spec_table(response, sel)
            if specs:
                break
        if not specs:
            keys = response.css(
                "dl dt::text, .spec-key::text, .spec-label::text, th::text"
            ).getall()
            vals = response.css(
                "dl dd::text, .spec-value::text, .spec-data::text, td::text"
            ).getall()
            specs = {k.strip(): v.strip()
                     for k, v in zip(keys, vals) if k.strip() and v.strip()}

        # Brand from dedicated elements
        brand_raw = None
        for sel in ("[itemprop='brand']::text", ".brand::text",
                    ".manufacturer::text", ".make::text", ".brand-name::text"):
            b = self.clean_text(response.css(sel).getall())
            if b:
                brand_raw = b
                break

        brand, model = self._split_brand_model(title, brand_raw)
        machine_type = self._infer_machine_type(response)
        category     = self._extract_category(response)

        yield MachineItem(
            machine_url=response.url,
            website_id=self.website_id,
            website_source=urlparse(response.url).netloc,
            category=category,
            machine_type=machine_type,
            brand=brand,
            model=model,
            price=price,
            currency=currency,
            location=location,
            description=description[:2000] if description else None,
            images=image_urls,
            specs=specs,
        )

    # ── Link following ────────────────────────────────────────────────────────

    def _follow_all_links(self, response: Response):
        """Follow category and detail links from any page."""
        seen: set[str] = set()

        # Priority: nav links (categories)
        nav_hrefs: list[str] = []
        for sel in NAV_SELECTORS:
            nav_hrefs.extend(response.css(sel).getall())

        all_hrefs = response.css("a::attr(href)").getall()

        for href in nav_hrefs + all_hrefs:
            href = (href or "").strip()
            if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
                continue

            url = urljoin(response.url, href).split("#")[0]
            if not same_domain(url, self._base_domain):
                continue
            if skip_url(url):
                continue
            if self._is_visited(url) or url in seen:
                continue

            seen.add(url)
            s = score_url(url)

            # Follow ALL intra-domain non-skipped links — machine listing URLs
            # frequently score 0 (e.g. ?cat=5&lang=de, ?id=123) so we cannot
            # gate on score >= 1.  Score is still used to choose the callback.
            self._mark_visited(url)
            if s >= 5:
                yield response.follow(
                    url, callback=self._parse_detail_page,
                    errback=self._errback,
                    meta={"referer": response.url},
                )
            else:
                yield response.follow(
                    url, callback=self._parse_listing_page,
                    errback=self._errback,
                    meta={"referer": response.url},
                )

    def _follow_pagination(self, response: Response):
        """
        Follow pagination links on listing pages.
        Handles: rel=next links, .pagination links, ?page=N, ?offset=N,
        /page/N/ path segments, WooCommerce, and numeric page links.
        """
        seen: set[str] = set()
        current = response.url

        # Standard CSS-based pagination selectors
        for sel in PAGINATION_SELECTORS:
            for href in response.css(sel).getall():
                url = urljoin(current, href).split("#")[0]
                if self._add_pagination_url(url, seen):
                    yield response.follow(
                        url, callback=self._parse_listing_page,
                        errback=self._errback,
                        meta={"referer": current},
                    )

        # Any link on the page that looks like a pagination URL
        for href in response.css("a::attr(href)").getall():
            url = urljoin(current, href).split("#")[0]
            if (looks_like_pagination_url(url)
                    and same_domain(url, self._base_domain)
                    and not skip_url(url)):
                if self._add_pagination_url(url, seen):
                    yield response.follow(
                        url, callback=self._parse_listing_page,
                        errback=self._errback,
                        meta={"referer": current},
                    )

        # Probe ahead if we're on page 1 and found results
        # (helps sites without explicit next buttons)
        page_match = re.search(r"[?&/]p(?:age)?[=/](\d+)", current, re.IGNORECASE)
        current_page = int(page_match.group(1)) if page_match else 1
        if current_page == 1:
            for next_page in (2, 3):
                candidate = build_page_url(current, next_page)
                if self._add_pagination_url(candidate, seen):
                    yield scrapy.Request(
                        candidate,
                        callback=self._parse_listing_page,
                        errback=self._errback,
                        meta={"referer": current},
                    )

    def _add_pagination_url(self, url: str, seen: set) -> bool:
        """Add url to seen + visited; return True if it's new."""
        if not url or url in seen or self._is_visited(url):
            return False
        if not same_domain(url, self._base_domain) or skip_url(url):
            return False
        seen.add(url)
        self._mark_visited(url)
        return True

    # ── Error callback ────────────────────────────────────────────────────────

    def _errback(self, failure):
        """Log request failures without crashing the spider."""
        logger.warning(
            f"Request failed [{failure.value.__class__.__name__}]: "
            f"{getattr(failure.request, 'url', 'unknown')}"
        )

    # ── Type / brand helpers ──────────────────────────────────────────────────

    def _split_brand_model(self, title: str, known_brand: str | None = None) -> tuple:
        if known_brand:
            model = title
            if title.lower().startswith(known_brand.lower()):
                model = title[len(known_brand):].strip(" -:")
            return known_brand.strip().title(), (model.strip() or None)

        if not title:
            return None, None

        try:
            from app.services.normalization_service import BRAND_ALIASES
            known = set(BRAND_ALIASES.values())
            parts = title.split()
            for n in (3, 2, 1):
                candidate = " ".join(parts[:n]).title()
                if candidate.lower() in {b.lower() for b in known}:
                    return candidate, " ".join(parts[n:]).strip() or None
        except Exception:
            pass

        parts = title.split()
        if len(parts) >= 2:
            return parts[0].title(), " ".join(parts[1:])
        return None, title

    def _infer_machine_type(self, response: Response) -> str | None:
        crumbs = " ".join(response.css(
            ", ".join(BREADCRUMB_SELECTORS)
        ).getall()).lower()
        meta_kw   = response.css("meta[name='keywords']::attr(content)").get("").lower()
        meta_desc = response.css("meta[name='description']::attr(content)").get("").lower()
        h_text    = " ".join(response.css("h1::text,h2::text,h3::text").getall()).lower()
        combined  = f"{response.url.lower()} {crumbs} {meta_kw} {meta_desc} {h_text}"
        return self._infer_machine_type_from_text(combined)

    def _infer_machine_type_from_text(self, text: str) -> str | None:
        try:
            from app.services.normalization_service import TYPE_SYNONYMS
            text_lower = text.lower()
            for synonym, canonical in TYPE_SYNONYMS.items():
                if synonym in text_lower:
                    return canonical
        except Exception:
            pass
        return None

    def _looks_like_type(self, text: str) -> bool:
        """True if the text contains a machine-type keyword."""
        from zoogle_crawler.page_analyzer import MACHINE_WORDS
        return bool(set(re.split(r"\W+", text.lower())) & MACHINE_WORDS)
