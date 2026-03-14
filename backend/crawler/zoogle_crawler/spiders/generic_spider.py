"""
Zoogle Power Spider — 3-phase industrial machine crawler.

Phase 1  Category Discovery
  • Fetch /sitemap.xml (+ sitemap index) to get every URL at once
  • Parse nav / header / sidebar / footer links for category pages
  • Score and queue every machine-relevant URL

Phase 2  Listing Pages
  • Detect machine "cards" on listing / category pages
  • Yield brief MachineItems directly from cards (price + image + URL)
  • Follow every "View Details" link for full data
  • Follow ALL pagination (URL params, rel=next, /page/N/)

Phase 3  Detail Pages
  • Try in order: JSON-LD → embedded JS JSON → CSS heuristics
  • Extract: title, brand, model, type, price, location,
             description, images, spec tables
"""
import re
import json
from urllib.parse import urlparse, urljoin, urlunparse, urlencode, parse_qs, urlencode

import scrapy
from scrapy.http import Response

from zoogle_crawler.items import MachineItem
from zoogle_crawler.spiders.base_spider import BaseZoogleSpider


# ─────────────────────────────────────────────────────────────────────────────
#  URL lists / scoring
# ─────────────────────────────────────────────────────────────────────────────

MACHINE_WORDS = {
    "cnc", "lathe", "lathes", "milling", "turning", "grinder", "grinders",
    "press", "presses", "laser", "injection", "molding", "moulding", "machining",
    "machine", "machines", "equipment", "used", "industrial", "metalworking",
    "fabrication", "stamping", "punching", "grinding", "boring", "drilling",
    "edm", "plasma", "waterjet", "bending", "shearing", "forming", "forging",
    "casting", "compressor", "pump", "generator", "crane", "forklift",
    "conveyor", "robot", "robotics", "welding", "welder", "cutting", "saw",
    "bandsaw", "router", "spindle", "turret", "swiss", "multiturn",
}

CATEGORY_WORDS = {
    "category", "categories", "catalog", "catalogue", "type", "types",
    "brand", "brands", "browse", "shop", "find", "all", "listing", "listings",
    "inventory", "stock", "search", "collection", "department",
}

SKIP_WORDS = {
    "login", "register", "contact", "about", "faq", "cart", "checkout",
    "blog", "news", "policy", "terms", "privacy", "sitemap.html", "rss",
    "feed", "cdn", "static", "assets", "/css", "/js", "/fonts",
    "download", "upload", "account", "profile", "sign-in", "sign-up",
    "subscribe", "advertise", "sell-your", "sell-machine", "post-listing",
    "dealer", "dealers", "help", "support", "warranty", "financing",
}

# ─────────────────────────────────────────────────────────────────────────────
#  CSS selectors
# ─────────────────────────────────────────────────────────────────────────────

# Nav / category discovery
NAV_SELECTORS = [
    "nav a::attr(href)", "header a::attr(href)",
    "aside a::attr(href)", "#sidebar a::attr(href)",
    ".sidebar a::attr(href)", ".category-menu a::attr(href)",
    ".categories a::attr(href)", ".navigation a::attr(href)",
    ".main-menu a::attr(href)", ".site-nav a::attr(href)",
    "footer a::attr(href)",
]

# Machine card containers on listing pages
CARD_SELECTORS = [
    # class-based
    ".product-card", ".machine-card", ".listing-card", ".item-card",
    ".equipment-card", ".inventory-card", ".result-card",
    ".search-result", ".result-item",
    ".product-item", ".machine-item", ".listing-item", ".equipment-item",
    ".inventory-item",
    # article/section tags
    "article.product", "article.machine", "article.listing",
    "article.equipment", "article.item",
    # grid children
    ".listing-grid > *", ".product-grid > *", ".machine-grid > *",
    ".equipment-grid > *", ".results-grid > *",
    ".machines-list > li", ".product-list > li", ".listing-list > li",
    # generic but common
    "li.product", "li.listing", "li.machine", "li.item",
    ".woocommerce ul.products li",
]

# Within a card — title selectors
CARD_TITLE = [
    "h2::text", "h3::text", "h4::text",
    ".title::text", ".name::text", ".product-name::text",
    ".machine-name::text", ".listing-title::text",
    "a::text",
]

# Within a card — price
CARD_PRICE = [
    ".price::text", ".asking-price::text", ".listing-price::text",
    ".product-price::text", ".sale-price::text",
    "[data-price]::attr(data-price)", "span.price::text",
    ".amount::text",
]

# Within a card — location
CARD_LOCATION = [
    ".location::text", ".city::text", ".country::text",
    ".seller-location::text", ".item-location::text",
    "[data-location]::text",
]

# Within a card — link
CARD_LINK = ["a::attr(href)", "h2 a::attr(href)", "h3 a::attr(href)", ".title a::attr(href)"]

# Within a card — image
CARD_IMAGE = ["img::attr(src)", "img::attr(data-src)", "img::attr(data-lazy)"]

# Full detail — title
TITLE_SELECTORS = [
    "[itemprop='name']::text",
    "h1.product-title::text", "h1.listing-title::text",
    "h1.machine-name::text", "h1.entry-title::text",
    ".product_title::text", ".product-name h1::text",
    "[data-testid='product-name']::text",
    "h1::text",
]

# Full detail — price
PRICE_SELECTORS = [
    "[itemprop='price']::attr(content)", "[itemprop='price']::text",
    ".price::text", ".listing-price::text", ".product-price::text",
    ".sale-price::text", ".offer-price::text", ".asking-price::text",
    "span.price::text", ".amount::text",
    "[data-price]::attr(data-price)", "[data-testid='price']::text",
    ".woocommerce-Price-amount::text", "ins .amount::text",
]

# Full detail — description
DESCRIPTION_SELECTORS = [
    "[itemprop='description']",
    ".product-description", ".listing-description", ".machine-description",
    ".description", ".detail-text", "article.description",
    ".product-details", ".item-description", ".listing-details",
    "#description", ".tab-description", ".product_description",
    ".entry-content", ".post-content", ".spec-description",
]

# Full detail — location
LOCATION_SELECTORS = [
    "[itemprop='addressLocality']::text", "[itemprop='addressRegion']::text",
    "[itemprop='addressCountry']::text",
    ".location::text", ".listing-location::text", ".city::text",
    ".country::text", ".address::text", ".seller-location::text",
    ".item-location::text", "[data-location]::text",
]

# Full detail — images
IMAGE_SELECTORS = [
    "img[itemprop='image']::attr(src)",
    ".product-gallery img::attr(src)", ".product-gallery img::attr(data-src)",
    ".swiper-slide img::attr(src)", ".swiper-slide img::attr(data-src)",
    ".carousel img::attr(src)", ".carousel-item img::attr(src)",
    "#product-images img::attr(src)", ".image-gallery img::attr(src)",
    ".photos img::attr(src)", ".photos img::attr(data-lazy)",
    ".gallery img::attr(src)", ".gallery img::attr(data-src)",
    "img.product-image::attr(src)", "img.listing-image::attr(src)",
    "img.main-image::attr(src)", ".main-photo img::attr(src)",
    "img.wp-post-image::attr(src)",
]

# Pagination
PAGINATION_SELECTORS = [
    "a[rel='next']::attr(href)",
    ".pagination a::attr(href)", ".pagination li a::attr(href)",
    "a.next::attr(href)", "a.next-page::attr(href)",
    ".next-page a::attr(href)",
    "a[aria-label='Next']::attr(href)", "a[aria-label='next']::attr(href)",
    "a[aria-label='Next page']::attr(href)",
    ".pager a::attr(href)", "li.next a::attr(href)",
    "a.page-link[rel='next']::attr(href)",
]

# Spec tables
SPEC_TABLE_SELECTORS = [
    "table.specs tr", "table.specifications tr",
    "table.product-specs tr", ".specs-table tr",
    ".specification-table tr", ".attributes table tr",
    "table.woocommerce-product-attributes tr",
    "table tr",
]


# ─────────────────────────────────────────────────────────────────────────────
#  Helper functions
# ─────────────────────────────────────────────────────────────────────────────

def _url_tokens(url: str) -> set:
    return set(re.split(r"[\W_]+", url.lower()))


def _skip_url(url: str) -> bool:
    url_lower = url.lower()
    for w in SKIP_WORDS:
        if w in url_lower:
            return True
    # Skip image/asset extensions
    if re.search(r"\.(jpg|jpeg|png|gif|svg|webp|pdf|zip|css|js|woff|ttf)(\?|$)", url_lower):
        return True
    return False


def _score_url(url: str) -> int:
    if _skip_url(url):
        return -1
    tokens = _url_tokens(url)
    score = 0
    score += len(tokens & MACHINE_WORDS) * 2
    score += len(tokens & CATEGORY_WORDS)

    url_lower = url.lower()
    # Strong detail page signals
    if re.search(r"/machine[s]?/\d+", url_lower): score += 6
    if re.search(r"/listing[s]?/\d+", url_lower): score += 6
    if re.search(r"/product[s]?/\d+", url_lower): score += 5
    if re.search(r"/item[s]?/\d+", url_lower): score += 5
    if re.search(r"/detail[s]?/\w", url_lower): score += 5
    if re.search(r"/used-\w", url_lower): score += 3
    if re.search(r"\?.*id=\d+", url_lower): score += 3
    if re.search(r"/equipment/\w", url_lower): score += 4

    return score


def _is_detail_url(url: str) -> bool:
    return _score_url(url) >= 5


def _is_listing_or_category_url(url: str) -> bool:
    s = _score_url(url)
    return 1 <= s < 5


def _is_worth_following(url: str) -> bool:
    return _score_url(url) >= 1


def _same_domain(url: str, base: str) -> bool:
    return urlparse(url).netloc == base


def _paginate_url(base_url: str, page: int) -> str:
    """Build a paginated URL by appending ?page=N."""
    parsed = urlparse(base_url)
    qs = parse_qs(parsed.query)
    qs["page"] = [str(page)]
    new_query = urlencode({k: v[0] for k, v in qs.items()})
    return urlunparse(parsed._replace(query=new_query))


# ─────────────────────────────────────────────────────────────────────────────
#  Spider
# ─────────────────────────────────────────────────────────────────────────────

class GenericSpider(BaseZoogleSpider):
    name = "generic"
    custom_settings = {
        "DEPTH_LIMIT": 6,
        "CLOSESPIDER_ITEMCOUNT": 5000,
        "DOWNLOAD_DELAY": 0.3,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 8,
        "ROBOTSTXT_OBEY": False,
        "RETRY_TIMES": 3,
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 408, 429],
        "DEFAULT_REQUEST_HEADERS": {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Cache-Control": "no-cache",
        },
        "USER_AGENT": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
    }

    def __init__(self, website_id=None, start_url=None, crawl_log_id=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.website_id = int(website_id) if website_id else None
        self.crawl_log_id = int(crawl_log_id) if crawl_log_id else None
        self.start_urls = [start_url] if start_url else []
        self._base_domain = urlparse(start_url).netloc if start_url else ""
        self._visited: set = set()

    # ── Phase 1: Entry point ─────────────────────────────────────────────────

    def parse(self, response: Response):
        """Homepage — discover categories + try sitemap."""
        # Try sitemap first
        sitemap_url = urljoin(response.url, "/sitemap.xml")
        if sitemap_url not in self._visited:
            self._visited.add(sitemap_url)
            yield scrapy.Request(sitemap_url, callback=self._parse_sitemap, dont_filter=True,
                                 errback=lambda _: None)

        # Try sitemap index
        sitemap_index = urljoin(response.url, "/sitemap_index.xml")
        if sitemap_index not in self._visited:
            self._visited.add(sitemap_index)
            yield scrapy.Request(sitemap_index, callback=self._parse_sitemap, dont_filter=True,
                                 errback=lambda _: None)

        # Crawl the homepage itself
        yield from self._process_page(response)

    # ── Phase 1a: Sitemap parsing ─────────────────────────────────────────────

    def _parse_sitemap(self, response: Response):
        """
        Parse sitemap.xml or sitemap index.
        Handles: <loc>, <sitemap><loc>, nested sitemap indexes.
        """
        try:
            from scrapy.utils.gz import gunzip
        except ImportError:
            pass

        body = response.text

        # Sitemap index — recurse into child sitemaps
        child_sitemaps = re.findall(r"<sitemap>\s*<loc>(.*?)</loc>", body, re.DOTALL)
        for url in child_sitemaps:
            url = url.strip()
            if url not in self._visited:
                self._visited.add(url)
                yield scrapy.Request(url, callback=self._parse_sitemap, dont_filter=True,
                                     errback=lambda _: None)

        # URLs in this sitemap
        all_urls = re.findall(r"<loc>(.*?)</loc>", body, re.DOTALL)
        for url in all_urls:
            url = url.strip()
            if not url or url in self._visited:
                continue
            if not _same_domain(url, self._base_domain):
                continue
            if _skip_url(url):
                continue

            self._visited.add(url)
            score = _score_url(url)

            if score >= 5:
                # Likely a detail page
                yield scrapy.Request(url, callback=self._parse_detail_page, dont_filter=False)
            elif score >= 1:
                # Category or listing page
                yield scrapy.Request(url, callback=self._parse_listing_page, dont_filter=False)

    # ── Phase 2: Listing / category page ────────────────────────────────────

    def _process_page(self, response: Response):
        """Decide what to do with any page."""
        url = response.url

        # Always try JSON-LD on every page
        yield from self._extract_jsonld(response)

        if _is_detail_url(url) and self._looks_like_detail(response):
            yield from self._parse_detail_page(response)
        else:
            yield from self._parse_listing_page(response)

    def _parse_listing_page(self, response: Response):
        """
        Phase 2 — listing / category page.
        1. Extract machine cards visible on the page.
        2. Follow links to detail pages.
        3. Follow all nav/category links.
        4. Follow pagination.
        """
        # 1. Extract cards
        yield from self._extract_machine_cards(response)

        # 2. JSON-LD (some listing pages have ItemList)
        yield from self._extract_jsonld(response)

        # 3. Follow all links
        yield from self._follow_all_links(response)

        # 4. Pagination
        yield from self._follow_pagination(response)

    # ── Phase 2a: Extract machine cards from listing pages ───────────────────

    def _extract_machine_cards(self, response: Response):
        """
        Extract machine data from product/listing card elements on the page.
        Yields MachineItems with whatever data is available in the card.
        Also schedules detail page requests for full data.
        """
        for container_sel in CARD_SELECTORS:
            cards = response.css(container_sel)
            if not cards:
                continue

            for card in cards:
                # Get link first — required to identify the machine
                link = None
                for lsel in CARD_LINK:
                    link = card.css(lsel).get()
                    if link:
                        break
                if not link:
                    continue

                full_url = urljoin(response.url, link.strip())
                if not _same_domain(full_url, self._base_domain):
                    continue
                if _skip_url(full_url):
                    continue

                # Get title
                title = None
                for tsel in CARD_TITLE:
                    title = self.clean_text(card.css(tsel).getall())
                    if title and len(title) > 3:
                        break

                if not title:
                    continue

                # Get price
                price_raw = None
                for psel in CARD_PRICE:
                    price_raw = self.clean_text(card.css(psel).getall())
                    if price_raw:
                        break
                price, currency = self.extract_price(price_raw or "")

                # Get location
                location = None
                for locsel in CARD_LOCATION:
                    location = self.clean_text(card.css(locsel).getall())
                    if location:
                        break

                # Get image
                images = []
                for isel in CARD_IMAGE:
                    img = card.css(isel).get()
                    if img and not img.startswith("data:"):
                        images = self.normalize_image_urls([img], response.url)
                        break

                brand, model = self._split_brand_model(title)
                machine_type = self._infer_machine_type_from_text(
                    f"{title} {response.url}"
                )

                # Yield a partial item from the card
                yield MachineItem(
                    machine_url=full_url,
                    website_id=self.website_id,
                    website_source=self._base_domain,
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

                # Also schedule a detail page crawl for complete data
                if full_url not in self._visited:
                    self._visited.add(full_url)
                    yield response.follow(
                        full_url,
                        callback=self._parse_detail_page,
                        dont_filter=False,
                    )

            if cards:
                break  # found cards with first matching selector, stop trying others

    # ── Phase 3: Detail page ─────────────────────────────────────────────────

    def _parse_detail_page(self, response: Response):
        """
        Full detail extraction:
        Priority: JSON-LD → embedded JSON in script → CSS heuristics
        """
        # Try JSON-LD first
        jsonld_items = list(self._extract_jsonld(response))
        if jsonld_items:
            yield from jsonld_items
            return

        # Try embedded JS JSON (__NEXT_DATA__ / window.__INITIAL_STATE__ etc.)
        script_items = list(self._extract_script_json(response))
        if script_items:
            yield from script_items
            return

        # CSS heuristic fallback
        yield from self._parse_detail_css(response)

    def _looks_like_detail(self, response: Response) -> bool:
        has_h1 = bool(response.css("h1").get())
        has_price = any(response.css(sel).get() for sel in PRICE_SELECTORS[:8])
        has_image = bool(response.css("img[src]").get())
        return has_h1 and (has_price or has_image)

    # ── JSON-LD extraction ────────────────────────────────────────────────────

    def _extract_jsonld(self, response: Response):
        for script in response.css("script[type='application/ld+json']::text").getall():
            try:
                data = json.loads(script)
            except (json.JSONDecodeError, ValueError):
                continue

            nodes = []
            if isinstance(data, list):
                nodes = data
            elif isinstance(data, dict):
                nodes = data.get("@graph", [data])

            for node in nodes:
                if not isinstance(node, dict):
                    continue
                node_type = node.get("@type", "")
                if isinstance(node_type, list):
                    node_type = " ".join(node_type)

                if any(t in node_type for t in ("Product", "Offer", "Vehicle")):
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
        brand_obj = node.get("brand") or node.get("manufacturer")
        brand_raw = None
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
            parts = [loc.get("addressLocality", ""), loc.get("addressRegion", ""), loc.get("addressCountry", "")]
            location = ", ".join(p for p in parts if p) or None

        brand, model = self._split_brand_model(str(name), brand_raw)
        machine_type = self._infer_machine_type_from_text(f"{name} {description or ''} {url}")

        return MachineItem(
            machine_url=str(url),
            website_id=self.website_id,
            website_source=urlparse(str(url)).netloc or self._base_domain,
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

    # ── Embedded script JSON extraction ──────────────────────────────────────

    def _extract_script_json(self, response: Response):
        """Extract machine data from embedded JS (Next.js, React, etc.)."""
        for script in response.css("script:not([src])::text").getall():
            if len(script) < 100:
                continue

            data = None

            # Next.js __NEXT_DATA__
            m = re.search(r'id=["\']__NEXT_DATA__["\'][^>]*>(\{.*?\})</script>', script + "</script>", re.DOTALL)
            if not m:
                m = re.search(r'__NEXT_DATA__\s*=\s*(\{.*?\})\s*;', script, re.DOTALL)

            if m:
                try:
                    data = json.loads(m.group(1))
                except Exception:
                    pass

            # window.__INITIAL_STATE__ / window.__STATE__
            if not data:
                m2 = re.search(r'window\.__(?:INITIAL_)?STATE__\s*=\s*(\{.*?\});', script, re.DOTALL)
                if m2:
                    try:
                        data = json.loads(m2.group(1))
                    except Exception:
                        pass

            # window.dataLayer push objects
            if not data:
                m3 = re.search(r'window\.dataLayer\s*=\s*(\[.*?\]);', script, re.DOTALL)
                if m3:
                    try:
                        data = json.loads(m3.group(1))
                    except Exception:
                        pass

            if data:
                items = list(self._dig_for_machines(data, response.url))
                yield from items

    def _dig_for_machines(self, obj, page_url: str, depth: int = 0):
        if depth > 8:
            return
        if isinstance(obj, dict):
            keys_lower = {k.lower() for k in obj.keys()}
            if len(keys_lower & {"machine", "brand", "model", "price", "equipment", "listing", "product", "name"}) >= 3:
                item = self._dict_to_item(obj, page_url)
                if item:
                    yield item
            for v in obj.values():
                yield from self._dig_for_machines(v, page_url, depth + 1)
        elif isinstance(obj, list):
            for elem in obj[:300]:
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

        url = get("url", "link", "href", "machineUrl", "listingUrl", "productUrl", "detailUrl") or page_url
        if not str(url).startswith("http"):
            url = urljoin(page_url, str(url))

        price_raw = get("price", "askingPrice", "listPrice", "salePrice", "sellingPrice")
        price, currency = self.extract_price(price_raw or "")

        brand_raw = get("brand", "manufacturer", "make", "brandName")
        model_raw = get("model", "modelNumber", "modelName", "partNumber")
        location_raw = get("location", "city", "country", "address", "region", "state")
        description_raw = get("description", "details", "summary", "body")

        brand, model = self._split_brand_model(name, brand_raw)
        if model_raw and not model:
            model = model_raw

        return MachineItem(
            machine_url=str(url),
            website_id=self.website_id,
            website_source=urlparse(str(url)).netloc or self._base_domain,
            machine_type=self._infer_machine_type_from_text(f"{name} {description_raw or ''}"),
            brand=brand,
            model=model,
            price=price,
            currency=currency,
            location=location_raw,
            description=description_raw[:2000] if description_raw else None,
            images=[],
            specs={},
        )

    # ── CSS heuristic detail extraction ──────────────────────────────────────

    def _parse_detail_css(self, response: Response):
        title = None
        for sel in TITLE_SELECTORS:
            title = self.clean_text(response.css(sel).getall())
            if title and len(title) > 3:
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

        # Images — try all selectors and merge; also check data-* attrs
        image_urls = []
        for sel in IMAGE_SELECTORS:
            found = [u for u in response.css(sel).getall() if u and not u.startswith("data:")]
            image_urls.extend(found)
            if len(image_urls) >= 8:
                break
        for attr in ("data-src", "data-lazy", "data-original", "data-image", "data-zoom-image"):
            image_urls.extend(
                u for u in response.css(f"img::{attr}").getall()
                if u and not u.startswith("data:")
            )
        image_urls = self.normalize_image_urls(image_urls, response.url)

        # Specs
        specs = {}
        for sel in SPEC_TABLE_SELECTORS:
            specs = self.parse_spec_table(response, sel)
            if specs:
                break
        if not specs:
            # dl / dt / dd pattern
            keys = response.css("dl dt::text, .spec-key::text, .spec-label::text, th::text").getall()
            vals = response.css("dl dd::text, .spec-value::text, .spec-data::text, td::text").getall()
            specs = {k.strip(): v.strip() for k, v in zip(keys, vals) if k.strip() and v.strip()}

        # Brand from dedicated elements
        brand_raw = None
        for bsel in ("[itemprop='brand']::text", ".brand::text", ".manufacturer::text", ".make::text"):
            b = self.clean_text(response.css(bsel).getall())
            if b:
                brand_raw = b
                break

        brand, model = self._split_brand_model(title, brand_raw)
        machine_type = self._infer_machine_type(response)

        yield MachineItem(
            machine_url=response.url,
            website_id=self.website_id,
            website_source=urlparse(response.url).netloc,
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

    # ── Link following helpers ────────────────────────────────────────────────

    def _follow_all_links(self, response: Response):
        """Follow category, nav, and detail links from any page."""
        seen_this_page: set = set()

        # Priority: nav / sidebar / header links (categories)
        nav_hrefs = []
        for nav_sel in NAV_SELECTORS:
            nav_hrefs.extend(response.css(nav_sel).getall())

        # All links on page
        all_hrefs = response.css("a::attr(href)").getall()

        for href in nav_hrefs + all_hrefs:
            href = (href or "").strip()
            if not href or href.startswith("#") or href.startswith("javascript:") or href.startswith("mailto:"):
                continue

            url = urljoin(response.url, href).split("#")[0]

            if not _same_domain(url, self._base_domain):
                continue
            if _skip_url(url):
                continue
            if url in self._visited or url in seen_this_page:
                continue

            seen_this_page.add(url)
            score = _score_url(url)

            if score >= 5:
                # Detail page
                self._visited.add(url)
                yield response.follow(url, callback=self._parse_detail_page, dont_filter=False)
            elif score >= 1:
                # Category / listing
                self._visited.add(url)
                yield response.follow(url, callback=self._parse_listing_page, dont_filter=False)

    def _follow_pagination(self, response: Response):
        """Follow all pagination links on a listing page."""
        seen: set = set()

        # Standard rel=next and class-based pagination
        for sel in PAGINATION_SELECTORS:
            for href in response.css(sel).getall():
                url = urljoin(response.url, href).split("#")[0]
                if url not in self._visited and url not in seen and _same_domain(url, self._base_domain):
                    seen.add(url)
                    self._visited.add(url)
                    yield response.follow(url, callback=self._parse_listing_page, dont_filter=False)

        # Numeric pagination links (/page/2/, /page/3/, ?page=2)
        current_url = response.url
        for page_href in response.css("a::attr(href)").getall():
            page_url = urljoin(current_url, page_href).split("#")[0]
            if re.search(r"[?&/]p(?:age)?[=/]\d+", page_url) and _same_domain(page_url, self._base_domain):
                if page_url not in self._visited and page_url not in seen:
                    seen.add(page_url)
                    self._visited.add(page_url)
                    yield response.follow(page_url, callback=self._parse_listing_page, dont_filter=False)

    # ── Type / brand helpers ─────────────────────────────────────────────────

    def _split_brand_model(self, title: str, known_brand: str | None = None) -> tuple[str | None, str | None]:
        if known_brand:
            model = title
            if title.lower().startswith(known_brand.lower()):
                model = title[len(known_brand):].strip(" -:")
            return known_brand.strip().title(), model.strip() or None

        if not title:
            return None, None

        try:
            from app.services.normalization_service import BRAND_ALIASES
            known_brands = set(BRAND_ALIASES.values())
            parts = title.split()
            for n in (3, 2, 1):
                candidate = " ".join(parts[:n]).title()
                if candidate in known_brands or candidate.lower() in {b.lower() for b in known_brands}:
                    return candidate, " ".join(parts[n:]).strip() or None
        except Exception:
            pass

        parts = title.split()
        if len(parts) >= 2:
            return parts[0].title(), " ".join(parts[1:])
        return None, title

    def _infer_machine_type(self, response: Response) -> str | None:
        breadcrumbs = " ".join(response.css(
            ".breadcrumb a::text, .breadcrumbs a::text, "
            "[aria-label='breadcrumb'] a::text, nav.breadcrumb::text"
        ).getall()).lower()
        meta_kw = response.css("meta[name='keywords']::attr(content)").get("").lower()
        meta_desc = response.css("meta[name='description']::attr(content)").get("").lower()
        h_text = " ".join(response.css("h1::text, h2::text, h3::text").getall()).lower()
        combined = f"{response.url.lower()} {breadcrumbs} {meta_kw} {meta_desc} {h_text}"
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
