"""
Generic industrial machine spider.

Strategy:
  1. Start at the given URL.
  2. Identify machine listing pages (pagination-aware).
  3. Follow links to individual machine detail pages.
  4. Extract machine data using heuristic selectors + ML-based fallback.
  5. Yield MachineItem for each machine found.

The spider uses a scoring approach to identify which links are machine
detail pages vs navigation/utility links.
"""
import re
from urllib.parse import urlparse, urljoin

import scrapy
from scrapy.http import Response

from zoogle_crawler.items import MachineItem
from zoogle_crawler.spiders.base_spider import BaseZoogleSpider

# Keywords that suggest a page is a machine listing
MACHINE_KEYWORDS = [
    "cnc", "lathe", "milling", "turning", "grinder", "press", "laser",
    "injection", "molding", "machining", "machine", "equipment", "used",
    "industrial", "metalworking", "fabrication", "stamping", "punching",
    "grinding", "boring", "drilling", "edm", "plasma", "waterjet",
    "bending", "shearing", "forming",
]

# CSS selectors tried in order to find machine detail data
TITLE_SELECTORS = [
    "h1.product-title::text",
    "h1.listing-title::text",
    "h1.machine-name::text",
    ".product-name h1::text",
    ".listing-header h1::text",
    "h1::text",
    "title::text",
]

PRICE_SELECTORS = [
    ".price::text",
    ".listing-price::text",
    ".product-price::text",
    "[itemprop='price']::attr(content)",
    "[itemprop='price']::text",
    ".amount::text",
    "span.price::text",
]

DESCRIPTION_SELECTORS = [
    ".product-description",
    ".listing-description",
    ".machine-description",
    ".description",
    "[itemprop='description']",
    ".detail-text",
    "article.description",
]

LOCATION_SELECTORS = [
    ".location::text",
    ".listing-location::text",
    "[itemprop='addressLocality']::text",
    ".city::text",
    ".country::text",
    ".address::text",
]

IMAGE_SELECTORS = [
    "img.product-image::attr(src)",
    "img.listing-image::attr(src)",
    ".gallery img::attr(src)",
    ".product-gallery img::attr(data-src)",
    ".swiper-slide img::attr(src)",
    ".carousel img::attr(src)",
    "#product-images img::attr(src)",
    ".image-gallery img::attr(src)",
    "img[itemprop='image']::attr(src)",
    "img::attr(src)",
]


def _score_url_as_machine(url: str) -> int:
    """Score a URL likelihood of being a machine detail page (higher = more likely)."""
    score = 0
    url_lower = url.lower()
    for kw in MACHINE_KEYWORDS:
        if kw in url_lower:
            score += 2
    # Typical detail page patterns
    if re.search(r"/machine[s]?/\d+", url_lower):
        score += 5
    if re.search(r"/listing[s]?/", url_lower):
        score += 4
    if re.search(r"/product[s]?/", url_lower):
        score += 3
    if re.search(r"/detail[s]?/", url_lower):
        score += 4
    if re.search(r"/equipment/", url_lower):
        score += 3
    if re.search(r"/item[s]?/", url_lower):
        score += 3
    # Penalize obvious non-machine pages
    for bad in ["login", "register", "contact", "about", "faq", "cart", "checkout",
                "blog", "news", "policy", "terms", "privacy", "sitemap"]:
        if bad in url_lower:
            score -= 10
    return score


class GenericSpider(BaseZoogleSpider):
    name = "generic"
    custom_settings = {
        "DEPTH_LIMIT": 4,
        "CLOSESPIDER_ITEMCOUNT": 1000,  # safety cap per site
    }

    def __init__(self, website_id=None, start_url=None, crawl_log_id=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.website_id = int(website_id) if website_id else None
        self.crawl_log_id = int(crawl_log_id) if crawl_log_id else None
        self.start_urls = [start_url] if start_url else []
        self._visited_detail_urls: set = set()

    def parse(self, response: Response):
        """Entry point — detect listing vs detail page."""
        url = response.url

        # If this looks like a machine detail page, parse it
        if _score_url_as_machine(url) >= 4 and self._looks_like_detail_page(response):
            yield from self._parse_machine_detail(response)
        else:
            yield from self._follow_machine_links(response)

    def _looks_like_detail_page(self, response: Response) -> bool:
        """Heuristic: detail pages have an h1 + price + description."""
        has_h1 = bool(response.css("h1").get())
        has_price = any(response.css(sel).get() for sel in PRICE_SELECTORS[:5])
        has_image = bool(response.css("img").get())
        return has_h1 and (has_price or has_image)

    def _follow_machine_links(self, response: Response):
        """From a listing/nav page, follow promising links."""
        base_domain = urlparse(self.start_urls[0]).netloc

        for href in response.css("a::attr(href)").getall():
            url = urljoin(response.url, href.strip())
            # Stay on same domain
            if urlparse(url).netloc != base_domain:
                continue
            if url in self._visited_detail_urls:
                continue
            if _score_url_as_machine(url) >= 3:
                self._visited_detail_urls.add(url)
                yield response.follow(url, callback=self.parse)

        # Follow pagination
        for next_page in response.css(
            "a[rel='next']::attr(href), .pagination a::attr(href), "
            ".next-page::attr(href), a.next::attr(href)"
        ).getall():
            yield response.follow(next_page, callback=self.parse)

    def _parse_machine_detail(self, response: Response):
        """Extract machine data from a detail page."""

        # Title / model extraction
        title = None
        for sel in TITLE_SELECTORS:
            title = self.clean_text(response.css(sel).getall())
            if title:
                break

        if not title:
            return   # nothing useful on this page

        # Parse title into brand + model
        brand, model = self._split_brand_model(title)

        # Machine type from URL or breadcrumbs
        machine_type = self._infer_machine_type(response)

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
            found = response.css(sel).getall()
            if found:
                image_urls.extend(found)
                break
        image_urls = self.normalize_image_urls(image_urls, response.url)

        # Specs (generic table)
        specs = self.parse_spec_table(response, "table tr") or {}

        item = MachineItem(
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
        yield item

    def _split_brand_model(self, title: str) -> tuple[str | None, str | None]:
        """
        Try to split "Brand Model" title.
        Known brand list is used for the first token match.
        """
        from app.services.normalization_service import BRAND_ALIASES

        if not title:
            return None, None

        parts = title.split()
        if len(parts) == 0:
            return None, title

        # Check if first word(s) match a known brand
        known_brands = set(BRAND_ALIASES.values())
        for n in (2, 1):   # try 2-word brand first, then 1-word
            candidate = " ".join(parts[:n]).title()
            if candidate in known_brands or candidate.lower() in {b.lower() for b in known_brands}:
                brand = candidate
                model = " ".join(parts[n:]) or None
                return brand, model

        # No match: first word = brand, rest = model
        return parts[0].title(), " ".join(parts[1:]) or None

    def _infer_machine_type(self, response: Response) -> str | None:
        """Infer machine type from URL, breadcrumbs, or page headings."""
        from app.services.normalization_service import TYPE_SYNONYMS, _clean

        # Check breadcrumbs
        breadcrumbs = " ".join(
            response.css(".breadcrumb a::text, .breadcrumbs a::text, nav.breadcrumb::text").getall()
        ).lower()

        # Check meta keywords
        meta_kw = response.css("meta[name='keywords']::attr(content)").get("").lower()
        h2_text = " ".join(response.css("h2::text").getall()).lower()

        combined = f"{response.url.lower()} {breadcrumbs} {meta_kw} {h2_text}"

        for synonym, canonical in TYPE_SYNONYMS.items():
            if synonym in combined:
                return canonical

        return None
