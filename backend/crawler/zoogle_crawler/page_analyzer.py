"""
page_analyzer.py — URL classification, page-type detection, and pagination helpers.

Extracted from generic_spider.py so the spider stays focused on crawl logic.
All functions are pure / stateless and thoroughly commented.
"""
import re
from urllib.parse import urlparse, urlunparse, urlencode, parse_qs


# ─────────────────────────────────────────────────────────────────────────────
# Keyword sets used for URL scoring
# ─────────────────────────────────────────────────────────────────────────────

MACHINE_WORDS: set[str] = {
    "cnc", "lathe", "lathes", "milling", "turning", "grinder", "grinders",
    "press", "presses", "laser", "injection", "molding", "moulding", "machining",
    "machine", "machines", "equipment", "used", "industrial", "metalworking",
    "fabrication", "stamping", "punching", "grinding", "boring", "drilling",
    "edm", "plasma", "waterjet", "bending", "shearing", "forming", "forging",
    "casting", "compressor", "pump", "generator", "crane", "forklift",
    "conveyor", "robot", "robotics", "welding", "welder", "cutting", "saw",
    "bandsaw", "router", "spindle", "turret", "swiss", "multiturn",
    "press-brake", "pressbrake", "machinetools", "lathes",
    # German / European terms common on multi-language sites
    "maschine", "maschinen", "drehmaschine", "fraesmaschine", "schleifmaschine",
    # French
    "machine-outil", "tour", "fraiseuse",
}

CATEGORY_WORDS: set[str] = {
    "category", "categories", "catalog", "catalogue", "type", "types",
    "brand", "brands", "browse", "shop", "find", "all", "listing", "listings",
    "inventory", "stock", "search", "collection", "department",
    "used-machines", "used-equipment", "secondhand", "second-hand",
    "pre-owned", "refurbished",
}

SKIP_WORDS: set[str] = {
    "login", "register", "contact", "about", "faq", "cart", "checkout",
    "blog", "news", "policy", "terms", "privacy", "sitemap.html", "rss",
    "feed", "cdn", "static", "assets", "/css/", "/js/", "/fonts/",
    "download", "upload", "account", "profile", "sign-in", "sign-up",
    "subscribe", "advertise", "sell-your", "sell-machine", "post-listing",
    "help", "support", "warranty", "financing",
    "/auth/", "/user/", "logout", "wp-admin", "wp-login", "/admin/",
    "/api/", "/ajax/", "mailto:", "tel:", "javascript:", ".pdf", ".zip",
    # Note: .js/.css/.woff etc. are already caught by ASSET_EXTENSIONS regex —
    # do NOT add them here as bare strings since that would also match URLs like
    # "/listings?style=ajax" or query params containing "js".
}

ASSET_EXTENSIONS = re.compile(
    r"\.(jpg|jpeg|png|gif|svg|webp|ico|pdf|zip|rar|gz|css|js|woff2?|ttf|eot|otf|mp4|mp3|wav)(\?.*)?$",
    re.IGNORECASE,
)

# Strong signals that a URL points to a single machine detail page
_DETAIL_PATTERNS = [
    # English
    re.compile(r"/machine[s]?/\d+", re.I),
    re.compile(r"/listing[s]?/\d+", re.I),
    re.compile(r"/product[s]?/\d+", re.I),
    re.compile(r"/item[s]?/\d+", re.I),
    re.compile(r"/equipment[s]?/[a-z0-9-]{3,}", re.I),
    re.compile(r"/detail[s]?/[a-z0-9-]{3,}", re.I),
    re.compile(r"/stock/[a-z0-9-]{3,}", re.I),
    re.compile(r"/used-[a-z]+-[a-z0-9-]{2,}", re.I),
    re.compile(r"/[a-z0-9-]+-for-sale-\d+", re.I),
    re.compile(r"\?.*\bid=\d+", re.I),
    re.compile(r"\?.*\blisting_?id=\d+", re.I),
    re.compile(r"\?.*\bsku=", re.I),
    re.compile(r"/sku/[a-z0-9-]+", re.I),
    re.compile(r"/ref/[a-z0-9-]+", re.I),
    re.compile(r"/view/[a-z0-9-]+", re.I),
    # German industrial machine sites
    re.compile(r"/produkt[e]?/[a-z0-9-]{3,}", re.I),       # German: product
    re.compile(r"/angebot[e]?/[a-z0-9-]{3,}", re.I),       # German: offer/listing
    re.compile(r"/maschine[n]?/[a-z0-9-]{3,}", re.I),      # German: machine
    re.compile(r"/artikel/[a-z0-9-]{3,}", re.I),           # German: article/item
    re.compile(r"/gebrauchtmaschine[n]?/[a-z0-9-]{3,}", re.I),  # German: used machine
    re.compile(r"/occasion[s]?/[a-z0-9-]{3,}", re.I),      # French/German: used machine
    re.compile(r"/annonce[s]?/[a-z0-9-]{3,}", re.I),       # French: listing
    re.compile(r"/occasion[s]?/\d+", re.I),
    re.compile(r"/angebot[e]?/\d+", re.I),
    re.compile(r"/produkt[e]?/\d+", re.I),
    re.compile(r"/maschine[n]?/\d+", re.I),
    # Generic: slug path after known category keywords followed by a long slug
    re.compile(r"/(machine|equipment|product|listing|item|maschine|produkt)/[a-z0-9][a-z0-9-]{5,}", re.I),
]

# Patterns that look like category/listing pages
_LISTING_PATTERNS = [
    re.compile(r"/categor", re.I),
    re.compile(r"/browse", re.I),
    re.compile(r"/inventory", re.I),
    re.compile(r"/catalog", re.I),
    re.compile(r"/search", re.I),
    re.compile(r"/shop", re.I),
    re.compile(r"/find", re.I),
    re.compile(r"/all-[a-z]", re.I),
    re.compile(r"/used-machines?", re.I),
    re.compile(r"\?.*\bcat(egory)?=", re.I),
    re.compile(r"\?.*\btype=", re.I),
]


def _url_tokens(url: str) -> set[str]:
    """Split URL path into lowercase word tokens."""
    return set(re.split(r"[\W_]+", url.lower()))


def skip_url(url: str) -> bool:
    """Return True if this URL should never be crawled."""
    if not url or not url.startswith("http"):
        return True
    url_lower = url.lower()
    for w in SKIP_WORDS:
        if w in url_lower:
            return True
    if ASSET_EXTENSIONS.search(url_lower):
        return True
    return False


def score_url(url: str) -> int:
    """
    Score a URL for machine-relevance.
    > 0  worth following
    >= 5 likely a detail page (single machine)
    1-4  likely a category / listing page
    """
    if skip_url(url):
        return -1

    tokens = _url_tokens(url)
    score = 0
    score += len(tokens & MACHINE_WORDS) * 2
    score += len(tokens & CATEGORY_WORDS)

    url_lower = url.lower()
    # Strong detail-page signals
    for pattern in _DETAIL_PATTERNS:
        if pattern.search(url_lower):
            score += 6
            break

    # Listing-page signals
    for pattern in _LISTING_PATTERNS:
        if pattern.search(url_lower):
            score += 2
            break

    return score


def is_detail_url(url: str) -> bool:
    return score_url(url) >= 5


def is_listing_url(url: str) -> bool:
    s = score_url(url)
    return 1 <= s < 5


def is_worth_following(url: str) -> bool:
    return score_url(url) >= 1


def same_domain(url: str, base_domain: str) -> bool:
    """True if *url* belongs to the same (sub)domain as *base_domain*."""
    parsed = urlparse(url)
    netloc = parsed.netloc.lower()
    base = base_domain.lower()
    return netloc == base or netloc.endswith("." + base)


# ─────────────────────────────────────────────────────────────────────────────
# Pagination helpers
# ─────────────────────────────────────────────────────────────────────────────

# All query-parameter names that carry a page number
_PAGE_PARAMS = [
    "page", "p", "pg", "paged",       # generic
    "pagenum", "page_num",             # variations
    "PageNumber", "pageNumber",
    "start", "from", "offset",        # offset-based
    "skip",
    "pgno", "pageno",
]

# Path-segment patterns like /page/2/, /pg/3, /p/4
_PAGE_PATH_RE = re.compile(
    r"(?<=/)(p(?:age|g)?)/(\d+)(/|$)",
    re.IGNORECASE,
)

# WooCommerce-style /page/N/
_WC_PAGE_RE = re.compile(r"/page/(\d+)/?$", re.IGNORECASE)


def build_page_url(base_url: str, page: int) -> str:
    """
    Construct a page-N variant of *base_url*.
    Uses the existing query-param name if one is detected, else appends ?page=N.
    """
    parsed = urlparse(base_url)
    qs = parse_qs(parsed.query, keep_blank_values=True)

    # Find the existing page param name
    existing_param = None
    for param in _PAGE_PARAMS:
        if param in qs:
            existing_param = param
            break

    param_name = existing_param or "page"
    qs[param_name] = [str(page)]
    new_query = urlencode({k: v[0] for k, v in qs.items()})
    return urlunparse(parsed._replace(query=new_query))


def looks_like_pagination_url(url: str) -> bool:
    """True if url contains a page-number indicator."""
    return bool(re.search(r"[?&/]p(?:age)?[=/]\d+", url, re.IGNORECASE))


# ─────────────────────────────────────────────────────────────────────────────
# CSS selectors used throughout the spider (single source of truth)
# ─────────────────────────────────────────────────────────────────────────────

# Navigation / category discovery
NAV_SELECTORS: list[str] = [
    "nav a::attr(href)", "header a::attr(href)",
    "aside a::attr(href)", "#sidebar a::attr(href)",
    ".sidebar a::attr(href)", ".category-menu a::attr(href)",
    ".categories a::attr(href)", ".navigation a::attr(href)",
    ".main-menu a::attr(href)", ".site-nav a::attr(href)",
    ".menu a::attr(href)", "#menu a::attr(href)",
    ".navbar a::attr(href)", "#navbar a::attr(href)",
    ".top-nav a::attr(href)", ".primary-nav a::attr(href)",
    ".mega-menu a::attr(href)", ".dropdown-menu a::attr(href)",
    "footer a::attr(href)",
    "[role='navigation'] a::attr(href)",
    "[aria-label='Main Navigation'] a::attr(href)",
]

# Machine card containers — order matters (most specific first)
CARD_SELECTORS: list[str] = [
    # Explicit class names
    ".product-card", ".machine-card", ".listing-card", ".item-card",
    ".equipment-card", ".inventory-card", ".result-card",
    ".search-result-item", ".result-item",
    ".product-item", ".machine-item", ".listing-item", ".equipment-item",
    ".inventory-item", ".stock-item",
    ".used-machine-card", ".used-equipment-card",
    # Article tags
    "article.product", "article.machine", "article.listing",
    "article.equipment", "article.item", "article",
    # Section tags
    "section.product", "section.machine", "section.listing",
    # Grid / list children
    ".listing-grid > div", ".listing-grid > li",
    ".product-grid > div", ".product-grid > li",
    ".machine-grid > div", ".machine-grid > li",
    ".equipment-grid > div", ".equipment-grid > li",
    ".results-grid > div", ".results-grid > li",
    ".machines-list > li", ".product-list > li",
    ".listing-list > li", ".inventory-list > li",
    ".search-results > div", ".search-results > li",
    # WooCommerce
    ".woocommerce ul.products > li",
    # Bootstrap cards
    ".card-deck .card", ".row .card",
    # Generic but matched only after specific ones
    "li.product", "li.listing", "li.machine", "li.item",
    # Broad fallback: any <div> inside a list container that has a link + heading
    ".products > div", ".listings > div", ".machines > div",
]

# Within a card — title
CARD_TITLE_SELECTORS: list[str] = [
    "h2 a::text", "h3 a::text", "h4 a::text",
    "h2::text", "h3::text", "h4::text",
    ".title::text", ".name::text", ".product-name::text",
    ".machine-name::text", ".listing-title::text",
    ".card-title::text",
    "a[class*='title']::text", "a[class*='name']::text",
    "a::text",
]

# Within a card — price
CARD_PRICE_SELECTORS: list[str] = [
    ".price::text", ".asking-price::text", ".listing-price::text",
    ".product-price::text", ".sale-price::text",
    "[data-price]::attr(data-price)", "span.price::text",
    ".amount::text", ".cost::text", ".offer-price::text",
    "[itemprop='price']::attr(content)", "[itemprop='price']::text",
]

# Within a card — location
CARD_LOCATION_SELECTORS: list[str] = [
    ".location::text", ".city::text", ".country::text",
    ".seller-location::text", ".item-location::text",
    "[data-location]::text", ".address::text",
    ".region::text", ".state::text",
    "[itemprop='addressLocality']::text",
]

# Within a card — link
CARD_LINK_SELECTORS: list[str] = [
    "h2 a::attr(href)", "h3 a::attr(href)", "h4 a::attr(href)",
    ".title a::attr(href)", ".name a::attr(href)",
    "a::attr(href)",
]

# Within a card — image (including lazy-load attributes)
CARD_IMAGE_SELECTORS: list[str] = [
    "img::attr(src)",
    "img::attr(data-src)",
    "img::attr(data-lazy)",
    "img::attr(data-lazy-src)",
    "img::attr(data-original)",
    "img::attr(data-url)",
    "[data-background-image]::attr(data-background-image)",
    ".product-image img::attr(src)",
]

# ── Detail page selectors ─────────────────────────────────────────────────────

TITLE_SELECTORS: list[str] = [
    "[itemprop='name']::text",
    "h1.product-title::text", "h1.listing-title::text",
    "h1.machine-name::text", "h1.entry-title::text",
    ".product_title::text", ".product-name h1::text",
    "[data-testid='product-name']::text",
    "h1::text",
    # Broader fallbacks for sites that use h2/h3 as their main listing title
    ".machine-title::text", ".product-heading::text", ".listing-heading::text",
    "h2.title::text", "h2.name::text", "h2.product::text",
    "h2::text", "h3::text",
]

PRICE_SELECTORS: list[str] = [
    "[itemprop='price']::attr(content)", "[itemprop='price']::text",
    ".price::text", ".listing-price::text", ".product-price::text",
    ".sale-price::text", ".offer-price::text", ".asking-price::text",
    "span.price::text", ".amount::text",
    "[data-price]::attr(data-price)",
    "[data-testid='price']::text",
    ".woocommerce-Price-amount::text", "ins .amount::text",
    ".cost::text", ".currency::text",
]

DESCRIPTION_SELECTORS: list[str] = [
    "[itemprop='description']",
    ".product-description", ".listing-description", ".machine-description",
    ".description", ".detail-text", "article.description",
    ".product-details", ".item-description", ".listing-details",
    "#description", ".tab-description", ".product_description",
    ".entry-content", ".post-content", ".spec-description",
    ".detail-description", ".machine-details",
]

LOCATION_SELECTORS: list[str] = [
    "[itemprop='addressLocality']::text", "[itemprop='addressRegion']::text",
    "[itemprop='addressCountry']::text",
    ".location::text", ".listing-location::text", ".city::text",
    ".country::text", ".address::text", ".seller-location::text",
    ".item-location::text", "[data-location]::text",
    ".region::text", ".dealer-location::text",
]

IMAGE_SELECTORS: list[str] = [
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
    # Lazy-load attributes on any img
    "img::attr(data-src)", "img::attr(data-lazy)",
    "img::attr(data-lazy-src)", "img::attr(data-original)",
    "img::attr(data-zoom-image)", "img::attr(data-image)",
    "img::attr(data-full)",
    # meta og:image
    "meta[property='og:image']::attr(content)",
    "meta[name='twitter:image']::attr(content)",
]

PAGINATION_SELECTORS: list[str] = [
    "a[rel='next']::attr(href)",
    ".pagination a::attr(href)", ".pagination li a::attr(href)",
    "a.next::attr(href)", "a.next-page::attr(href)",
    ".next-page a::attr(href)",
    "a[aria-label='Next']::attr(href)",
    "a[aria-label='next']::attr(href)",
    "a[aria-label='Next page']::attr(href)",
    "a[aria-label='next page']::attr(href)",
    ".pager a::attr(href)", "li.next a::attr(href)",
    "a.page-link[rel='next']::attr(href)",
    # WooCommerce
    ".woocommerce-pagination a.next::attr(href)",
    # Common button patterns
    "button.next[data-href]::attr(data-href)",
    "[data-testid='next-page']::attr(href)",
    # Nav-style
    "nav.pagination a::attr(href)",
    # Arrow / numeric at end
    ".arrow-right a::attr(href)",
]

SPEC_TABLE_SELECTORS: list[str] = [
    "table.specs tr", "table.specifications tr",
    "table.product-specs tr", ".specs-table tr",
    ".specification-table tr", ".attributes table tr",
    "table.woocommerce-product-attributes tr",
    ".product-attributes tr", ".machine-specs tr",
    "table tr",
]

BREADCRUMB_SELECTORS: list[str] = [
    ".breadcrumb a::text", ".breadcrumbs a::text",
    "[aria-label='breadcrumb'] a::text",
    "[aria-label='Breadcrumb'] a::text",
    "nav.breadcrumb a::text",
    ".breadcrumb-item a::text",
    "[itemtype*='BreadcrumbList'] [itemprop='name']::text",
]
