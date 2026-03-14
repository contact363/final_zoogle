"""
Generic industrial machine spider — improved.

Strategy:
  1. Start at the given URL.
  2. Discover ALL category/subcategory links and follow them (breadth-first).
  3. On listing pages: follow every machine detail link + pagination.
  4. On detail pages: extract via (in priority order):
       a) JSON-LD / Schema.org Product structured data
       b) OpenGraph meta tags
       c) Embedded JSON in <script> tags (Next.js / React hydration, etc.)
       d) CSS/XPath heuristic selectors
  5. Yield MachineItem for every machine found.
"""
import re
import json
from urllib.parse import urlparse, urljoin

import scrapy
from scrapy.http import Response

from zoogle_crawler.items import MachineItem
from zoogle_crawler.spiders.base_spider import BaseZoogleSpider


# ── URL scoring keywords ──────────────────────────────────────────────────────

MACHINE_KEYWORDS = [
    "cnc", "lathe", "lathes", "milling", "turning", "grinder", "grinders",
    "press", "presses", "laser", "injection", "molding", "machining", "machine",
    "machines", "equipment", "used", "industrial", "metalworking", "fabrication",
    "stamping", "punching", "grinding", "boring", "drilling", "edm", "plasma",
    "waterjet", "bending", "shearing", "forming", "forging", "casting",
    "compressor", "pump", "generator", "crane", "forklift", "conveyor",
    "robot", "robotics", "welding", "welder", "cutting", "saw", "bandsaw",
]

CATEGORY_KEYWORDS = [
    "category", "categories", "catalog", "catalogue", "type", "types",
    "brand", "brands", "browse", "search", "shop", "find", "all",
    "listing", "listings", "inventory", "stock", "used", "new",
    "industrial", "machine", "equipment",
]

BAD_URL_PARTS = [
    "login", "register", "contact", "about", "faq", "cart", "checkout",
    "blog", "news", "policy", "terms", "privacy", "sitemap", "rss",
    "feed", "cdn", "static", "assets", "images", "img", "css", "js",
    "fonts", "download", "upload", "api/", "admin", "wp-admin",
    "account", "profile", "sign-in", "sign-up", "subscribe",
]

# ── CSS selectors ─────────────────────────────────────────────────────────────

TITLE_SELECTORS = [
    "h1.product-title::text", "h1.listing-title::text", "h1.machine-name::text",
    ".product-name h1::text", ".listing-header h1::text",
    "h1.entry-title::text", ".product_title::text",
    "[itemprop='name']::text", "[data-testid='product-name']::text",
    "h1::text", "title::text",
]

PRICE_SELECTORS = [
    "[itemprop='price']::attr(content)", "[itemprop='price']::text",
    ".price::text", ".listing-price::text", ".product-price::text",
    ".sale-price::text", ".offer-price::text", ".asking-price::text",
    "span.price::text", ".amount::text", "[data-price]::attr(data-price)",
    "[data-testid='price']::text", ".price-value::text",
    ".woocommerce-Price-amount::text", "ins .amount::text",
]

DESCRIPTION_SELECTORS = [
    "[itemprop='description']", ".product-description",
    ".listing-description", ".machine-description",
    ".description", ".detail-text", "article.description",
    ".product-details", ".item-description", ".listing-details",
    "#description", ".tab-description", ".product_description",
    ".entry-content", ".post-content",
]

LOCATION_SELECTORS = [
    "[itemprop='addressLocality']::text", "[itemprop='addressRegion']::text",
    "[itemprop='addressCountry']::text",
    ".location::text", ".listing-location::text", ".city::text",
    ".country::text", ".address::text", ".seller-location::text",
    ".item-location::text", "[data-location]::text",
    ".geography::text", ".region::text",
]

IMAGE_SELECTORS = [
    "img[itemprop='image']::attr(src)",
    ".product-gallery img::attr(src)", ".product-gallery img::attr(data-src)",
    ".swiper-slide img::attr(src)", ".swiper-slide img::attr(data-src)",
    ".carousel img::attr(src)", ".carousel-item img::attr(src)",
    "#product-images img::attr(src)", ".image-gallery img::attr(src)",
    "img.product-image::attr(src)", "img.listing-image::attr(src)",
    ".gallery img::attr(src)", ".gallery img::attr(data-src)",
    ".photos img::attr(src)", ".photos img::attr(data-lazy)",
    "img.main-image::attr(src)", ".main-photo img::attr(src)",
    "img.wp-post-image::attr(src)",
    "img::attr(src)",
]

SPEC_TABLE_SELECTORS = [
    "table.specs tr", "table.specifications tr", "table.product-specs tr",
    ".specs-table tr", ".specification-table tr",
    ".attributes table tr", "table.woocommerce-product-attributes tr",
    "dl.specs dt, dl.specs dd", "table tr",
]

PAGINATION_SELECTORS = [
    "a[rel='next']::attr(href)",
    ".pagination a::attr(href)", ".pagination li a::attr(href)",
    "a.next::attr(href)", "a.next-page::attr(href)",
    ".next-page a::attr(href)", "a[aria-label='Next']::attr(href)",
    "a[aria-label='next']::attr(href)", ".pager a::attr(href)",
    "li.next a::attr(href)",
]


def _score_url(url: str) -> int:
    """Score a URL: positive = machine content, negative = skip."""
    url_lower = url.lower()
    score = 0

    for bad in BAD_URL_PARTS:
        if bad in url_lower:
            return -999

    for kw in MACHINE_KEYWORDS:
        if kw in url_lower:
            score += 2

    if re.search(r"/machine[s]?/\d+", url_lower):
        score += 6
    if re.search(r"/listing[s]?/\d+", url_lower):
        score += 6
    if re.search(r"/product[s]?/\d+", url_lower):
        score += 5
    if re.search(r"/detail[s]?/", url_lower):
        score += 5
    if re.search(r"/equipment/\w", url_lower):
        score += 4
    if re.search(r"/item[s]?/\d+", url_lower):
        score += 5
    if re.search(r"/used-\w", url_lower):
        score += 3
    if re.search(r"\?.*id=\d+", url_lower):
        score += 3

    for kw in CATEGORY_KEYWORDS:
        if kw in url_lower:
            score += 1

    return score


def _is_detail_url(url: str) -> bool:
    return _score_url(url) >= 5


def _is_worth_following(url: str) -> bool:
    return _score_url(url) >= 1


# ── Spider ────────────────────────────────────────────────────────────────────

class GenericSpider(BaseZoogleSpider):
    name = "generic"
    custom_settings = {
        "DEPTH_LIMIT": 5,
        "CLOSESPIDER_ITEMCOUNT": 2000,
        "DOWNLOAD_DELAY": 0.5,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "ROBOTSTXT_OBEY": False,
        "DEFAULT_REQUEST_HEADERS": {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        },
        "USER_AGENT": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
    }

    def __init__(self, website_id=None, start_url=None, crawl_log_id=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.website_id = int(website_id) if website_id else None
        self.crawl_log_id = int(crawl_log_id) if crawl_log_id else None
        self.start_urls = [start_url] if start_url else []
        self._visited: set = set()
        self._base_domain = urlparse(start_url).netloc if start_url else ""

    # ── Entry point ───────────────────────────────────────────────────────────

    def parse(self, response: Response):
        url = response.url

        # Try JSON-LD first — works even on listing pages with multiple products
        yield from self._extract_jsonld(response)

        if _is_detail_url(url) and self._looks_like_detail(response):
            yield from self._parse_detail(response)
        else:
            yield from self._follow_links(response)

    # ── Link following ────────────────────────────────────────────────────────

    def _follow_links(self, response: Response):
        """Follow all promising links (categories, listings, details, pagination)."""
        all_hrefs = response.css("a::attr(href)").getall()

        for href in all_hrefs:
            href = href.strip()
            if not href or href.startswith("#") or href.startswith("javascript:") or href.startswith("mailto:"):
                continue

            url = urljoin(response.url, href)

            # Stay on same domain
            if urlparse(url).netloc != self._base_domain:
                continue

            # Strip fragments
            url = url.split("#")[0]

            if url in self._visited:
                continue

            score = _score_url(url)
            if score >= 1:
                self._visited.add(url)
                yield response.follow(url, callback=self.parse, dont_filter=False)

        # Pagination
        for sel in PAGINATION_SELECTORS:
            for next_url in response.css(sel).getall():
                full = urljoin(response.url, next_url)
                if full not in self._visited and urlparse(full).netloc == self._base_domain:
                    self._visited.add(full)
                    yield response.follow(full, callback=self.parse)

    # ── JSON-LD extraction ────────────────────────────────────────────────────

    def _extract_jsonld(self, response: Response):
        """
        Extract Schema.org Product / ItemList data from <script type='application/ld+json'>.
        This works for many modern e-commerce and listing sites.
        """
        for script in response.css("script[type='application/ld+json']::text").getall():
            try:
                data = json.loads(script)
            except (json.JSONDecodeError, ValueError):
                continue

            # Handle @graph arrays
            nodes = []
            if isinstance(data, list):
                nodes = data
            elif isinstance(data, dict):
                if data.get("@graph"):
                    nodes = data["@graph"]
                else:
                    nodes = [data]

            for node in nodes:
                if not isinstance(node, dict):
                    continue
                node_type = node.get("@type", "")
                if isinstance(node_type, list):
                    node_type = " ".join(node_type)

                if "Product" in node_type or "Offer" in node_type:
                    item = self._jsonld_to_item(node, response.url)
                    if item:
                        yield item

                elif "ItemList" in node_type:
                    for element in node.get("itemListElement", []):
                        if isinstance(element, dict):
                            item = self._jsonld_to_item(element.get("item", element), response.url)
                            if item:
                                yield item

    def _jsonld_to_item(self, node: dict, page_url: str) -> MachineItem | None:
        name = node.get("name") or node.get("title") or ""
        if not name:
            return None

        # Price from offers
        price, currency = None, "USD"
        offers = node.get("offers") or node.get("Offers")
        if offers:
            if isinstance(offers, list):
                offers = offers[0]
            if isinstance(offers, dict):
                raw_price = offers.get("price") or offers.get("lowPrice")
                if raw_price:
                    try:
                        price = float(str(raw_price).replace(",", ""))
                    except ValueError:
                        pass
                currency = offers.get("priceCurrency", "USD")

        # URL
        url = node.get("url") or node.get("@id") or page_url
        if not url.startswith("http"):
            url = urljoin(page_url, url)

        # Description
        description = node.get("description") or ""
        if isinstance(description, dict):
            description = str(description)

        # Images
        images = []
        img = node.get("image")
        if isinstance(img, str):
            images = [img]
        elif isinstance(img, list):
            images = [i if isinstance(i, str) else i.get("url", "") for i in img]
        elif isinstance(img, dict):
            images = [img.get("url", "")]
        images = self.normalize_image_urls([i for i in images if i], page_url)

        # Brand
        brand_obj = node.get("brand") or node.get("manufacturer")
        brand = None
        if isinstance(brand_obj, str):
            brand = brand_obj
        elif isinstance(brand_obj, dict):
            brand = brand_obj.get("name")

        # Location
        location = None
        loc_obj = node.get("location") or node.get("address") or node.get("areaServed")
        if isinstance(loc_obj, str):
            location = loc_obj
        elif isinstance(loc_obj, dict):
            parts = [
                loc_obj.get("addressLocality", ""),
                loc_obj.get("addressRegion", ""),
                loc_obj.get("addressCountry", ""),
            ]
            location = ", ".join(p for p in parts if p) or None

        brand, model = self._split_brand_model(name, brand)
        machine_type = self._infer_machine_type_from_text(
            f"{name} {description} {url}"
        )

        return MachineItem(
            machine_url=url,
            website_id=self.website_id,
            website_source=urlparse(url).netloc or self._base_domain,
            machine_type=machine_type,
            brand=brand,
            model=model,
            price=price,
            currency=currency,
            location=location,
            description=str(description)[:2000] if description else None,
            images=images,
            specs={},
        )

    # ── Script-tag JSON extraction (Next.js __NEXT_DATA__, etc.) ─────────────

    def _extract_script_json(self, response: Response):
        """
        Extract machine data embedded as JSON in script tags.
        Handles Next.js __NEXT_DATA__, window.__INITIAL_STATE__, etc.
        """
        items = []

        for script in response.css("script:not([src])::text").getall():
            # Next.js
            if "__NEXT_DATA__" in script or "pageProps" in script:
                try:
                    match = re.search(r'=\s*(\{.*\})', script, re.DOTALL)
                    if match:
                        data = json.loads(match.group(1))
                        items.extend(self._dig_for_machines(data, response.url))
                except Exception:
                    pass

            # window.__INITIAL_STATE__ or similar
            if "initialState" in script or "INITIAL_STATE" in script or "window.__" in script:
                try:
                    match = re.search(r'=\s*(\{.*?\});?\s*(?:$|window\.)', script, re.DOTALL)
                    if match:
                        data = json.loads(match.group(1))
                        items.extend(self._dig_for_machines(data, response.url))
                except Exception:
                    pass

        return items

    def _dig_for_machines(self, obj, page_url: str, depth=0) -> list:
        """Recursively search a JSON object for machine-like records."""
        if depth > 6:
            return []
        results = []

        if isinstance(obj, dict):
            # Check if this dict looks like a machine record
            keys_lower = {k.lower() for k in obj.keys()}
            machine_indicators = {"machine", "brand", "model", "price", "equipment", "listing", "product"}
            if len(keys_lower & machine_indicators) >= 2:
                item = self._dict_to_item(obj, page_url)
                if item:
                    results.append(item)
            # Recurse
            for v in obj.values():
                results.extend(self._dig_for_machines(v, page_url, depth + 1))

        elif isinstance(obj, list):
            for elem in obj[:200]:  # cap to avoid huge lists
                results.extend(self._dig_for_machines(elem, page_url, depth + 1))

        return results

    def _dict_to_item(self, d: dict, page_url: str) -> MachineItem | None:
        """Convert a generic dict that looks like a machine to a MachineItem."""
        # Try various key names
        def get(*keys):
            for k in keys:
                v = d.get(k) or d.get(k.lower()) or d.get(k.upper())
                if v and isinstance(v, (str, int, float)):
                    return str(v).strip()
            return None

        name = get("name", "title", "machineName", "productName", "listingTitle")
        if not name:
            return None

        url = get("url", "link", "href", "machineUrl", "listingUrl", "productUrl") or page_url
        if not url.startswith("http"):
            url = urljoin(page_url, url)

        price_raw = get("price", "askingPrice", "listPrice", "salePrice")
        price, currency = self.extract_price(price_raw or "")

        brand_raw = get("brand", "manufacturer", "make")
        model_raw = get("model", "modelNumber", "partNumber")
        location_raw = get("location", "city", "country", "address", "region")
        description_raw = get("description", "details", "summary")

        brand, model = self._split_brand_model(name, brand_raw)
        if model_raw and not model:
            model = model_raw

        return MachineItem(
            machine_url=url,
            website_id=self.website_id,
            website_source=urlparse(url).netloc or self._base_domain,
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

    # ── CSS/XPath heuristic detail page extraction ────────────────────────────

    def _looks_like_detail(self, response: Response) -> bool:
        has_h1 = bool(response.css("h1").get())
        has_price = any(response.css(sel).get() for sel in PRICE_SELECTORS[:8])
        has_image = bool(response.css("img[src]").get())
        return has_h1 and (has_price or has_image)

    def _parse_detail(self, response: Response):
        """Extract machine data from a detail page using CSS selectors."""
        # Title
        title = None
        for sel in TITLE_SELECTORS:
            title = self.clean_text(response.css(sel).getall())
            if title:
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
            if description:
                break

        # Images
        image_urls = []
        for sel in IMAGE_SELECTORS:
            found = [u for u in response.css(sel).getall() if u and not u.startswith("data:")]
            if found:
                image_urls.extend(found)
                if len(image_urls) >= 5:
                    break
        # Also check data-src / data-lazy for lazy-loaded images
        for attr in ("data-src", "data-lazy", "data-original", "data-image"):
            found = response.css(f"img::{attr}").getall()
            image_urls.extend(f for f in found if f and not f.startswith("data:"))
        image_urls = self.normalize_image_urls(image_urls, response.url)

        # Specs from all common table formats
        specs = {}
        for sel in SPEC_TABLE_SELECTORS:
            specs = self.parse_spec_table(response, sel)
            if specs:
                break

        # Also parse dl/dt/dd spec patterns
        if not specs:
            keys = response.css("dl dt::text, .spec-key::text, .spec-label::text").getall()
            vals = response.css("dl dd::text, .spec-value::text, .spec-data::text").getall()
            specs = {k.strip(): v.strip() for k, v in zip(keys, vals) if k.strip() and v.strip()}

        brand, model = self._split_brand_model(title)
        machine_type = self._infer_machine_type(response)

        # Try to get brand from a dedicated element if not found from title
        if not brand:
            for sel in ["[itemprop='brand']::text", ".brand::text", ".manufacturer::text"]:
                b = self.clean_text(response.css(sel).getall())
                if b:
                    brand = b
                    break

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

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _split_brand_model(self, title: str, known_brand: str | None = None) -> tuple[str | None, str | None]:
        if known_brand:
            # Remove brand prefix from title to get model
            model = title
            if title.lower().startswith(known_brand.lower()):
                model = title[len(known_brand):].strip(" -:")
            return known_brand.title(), model or None

        if not title:
            return None, None

        try:
            from app.services.normalization_service import BRAND_ALIASES
            known_brands = set(BRAND_ALIASES.values())
            parts = title.split()
            for n in (3, 2, 1):
                candidate = " ".join(parts[:n]).title()
                if candidate in known_brands or candidate.lower() in {b.lower() for b in known_brands}:
                    return candidate, " ".join(parts[n:]) or None
        except Exception:
            pass

        parts = title.split()
        if len(parts) >= 2:
            return parts[0].title(), " ".join(parts[1:])
        return None, title

    def _infer_machine_type(self, response: Response) -> str | None:
        breadcrumbs = " ".join(
            response.css(
                ".breadcrumb a::text, .breadcrumbs a::text, "
                "nav.breadcrumb::text, [aria-label='breadcrumb'] a::text"
            ).getall()
        ).lower()
        meta_kw = response.css("meta[name='keywords']::attr(content)").get("").lower()
        meta_desc = response.css("meta[name='description']::attr(content)").get("").lower()
        h2_text = " ".join(response.css("h2::text, h3::text").getall()).lower()
        title_el = response.css("h1::text").get("").lower()
        combined = f"{response.url.lower()} {breadcrumbs} {meta_kw} {meta_desc} {h2_text} {title_el}"
        return self._infer_machine_type_from_text(combined)

    def _infer_machine_type_from_text(self, text: str) -> str | None:
        try:
            from app.services.normalization_service import TYPE_SYNONYMS, _clean
            text_lower = text.lower()
            for synonym, canonical in TYPE_SYNONYMS.items():
                if synonym in text_lower:
                    return canonical
        except Exception:
            pass
        return None
