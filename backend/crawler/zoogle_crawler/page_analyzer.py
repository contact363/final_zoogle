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
    "bearbeitungszentrum", "drehzentrum", "bohrmaschine", "saege", "kreissaege",
    "bandsaege", "schweissmaschine", "blechbearbeitung", "stanzmaschine",
    "abkantpresse", "gesenkbiegepresse", "hydraulikpresse", "exzenterpresse",
    "gebraucht", "occasion", "werkzeugmaschine", "werkzeugmaschinen",
    "zerspanungsmaschine", "umformmaschine", "kunststoffmaschine",
    "spritzguss", "spritzgiessmaschine", "extruder",
    # French
    "machine-outil", "tour", "fraiseuse", "rectifieuse", "presse",
    # Italian
    "tornio", "fresatrice", "rettificatrice",
    # Spanish
    "torno", "fresadora", "rectificadora",
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


# ─────────────────────────────────────────────────────────────────────────────
# Product page validation
# ─────────────────────────────────────────────────────────────────────────────

# CSS selectors that indicate a product title is present
_PRODUCT_TITLE_SELECTORS = [
    "h1", "h2.product-title", "h1.product-title",
    ".machine-title", ".product-name", ".listing-title",
    ".machine-name", ".item-title", ".product-heading",
    "[itemprop='name']", "[data-testid='product-title']",
    ".wp-block-post-title",
]

# CSS selectors for specification tables / blocks
_SPEC_SELECTORS = [
    "table.specs", "table.specifications", "table.machine-specs",
    ".specs", ".specifications", ".machine-specs", ".tech-specs",
    ".product-specs", ".details-table", ".technical-data",
    "dl.specs", "ul.specs", ".spec-list", ".attribute-list",
    "[itemprop='description'] table",
    ".woocommerce-product-attributes",
    ".product-attributes", ".product-meta",
]

# CSS selectors for product images (not navigation logos / banners)
_PRODUCT_IMAGE_SELECTORS = [
    ".product-image img", ".machine-image img",
    ".gallery img", ".product-gallery img",
    ".listing-image img", ".main-image img",
    "[itemprop='image']", ".swiper-slide img",
    ".product-photo img", ".machine-photo img",
    "figure.product img", ".featured-image img",
    "img.product-img", "img.machine-img",
]

# CSS selectors for price
_PRICE_SELECTORS_VALIDATE = [
    ".price", ".product-price", ".machine-price",
    ".asking-price", "[itemprop='price']",
    ".listing-price", ".sale-price",
]


def is_valid_product_page(response) -> bool:
    """
    Return True if the response looks like a genuine machine product page.

    A valid product page must have ALL of:
      1. A product title (h1 or known title selectors)
      2. At least ONE of: specs table | product image | price

    Pages lacking these signals (category pages, blog posts, contact forms,
    homepage, etc.) are rejected — no MachineItem will be emitted.

    Designed to be called from _parse_detail_page before extraction begins.
    """
    try:
        # ── 1. Title presence ─────────────────────────────────────────────────
        title_found = False
        for sel in _PRODUCT_TITLE_SELECTORS:
            text = " ".join(response.css(f"{sel}::text").getall()).strip()
            if len(text) > 4:         # minimum meaningful length
                title_found = True
                break

        if not title_found:
            return False

        # ── 2. At least one content signal ────────────────────────────────────
        # Specs table
        for sel in _SPEC_SELECTORS:
            if response.css(sel).get():
                return True

        # Product image (must be large-ish — skip tiny logo/icon images)
        for sel in _PRODUCT_IMAGE_SELECTORS:
            for img in response.css(sel):
                src = img.attrib.get("src", "") or img.attrib.get("data-src", "")
                # Skip data URIs and tiny images indicated by 'logo'/'icon' in src
                if src and "logo" not in src.lower() and "icon" not in src.lower():
                    # Check width if present
                    w = img.attrib.get("width")
                    if w:
                        try:
                            if int(w) >= 100:
                                return True
                        except ValueError:
                            pass
                    else:
                        return True   # no width attr — accept

        # Price
        for sel in _PRICE_SELECTORS_VALIDATE:
            text = " ".join(response.css(f"{sel}::text").getall()).strip()
            if text and re.search(r"[\d,]+", text):
                return True

        return False

    except Exception:
        # If CSS selectors fail (e.g. non-HTML response) — reject
        return False


def _strip_www(domain: str) -> str:
    """Remove leading 'www.' so www.foo.com and foo.com are treated identically."""
    return domain[4:] if domain.startswith("www.") else domain


def same_domain(url: str, base_domain: str) -> bool:
    """
    True if *url* belongs to the same (sub)domain as *base_domain*.
    Ignores the www. prefix so that a www→non-www (or non-www→www) redirect
    does not break link-following for the whole crawl.
    """
    parsed = urlparse(url)
    netloc = _strip_www(parsed.netloc.lower())
    base   = _strip_www(base_domain.lower())
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
    # Article tags — SPECIFIC classes only (bare "article" is too broad)
    "article.product", "article.machine", "article.listing",
    "article.equipment", "article.item",
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
    # Broad fallback: any <div>/<li> inside common list/grid containers
    ".products > div", ".listings > div", ".machines > div",
    ".products > li", ".listings > li", ".machines > li",
    ".items > li", ".items > div",
    ".offers > li", ".offers > div",
    ".angebote > li", ".angebote > div",
    # Common German/European CMS patterns
    "[class*='machine'] > div", "[class*='maschine'] > div",
    "[class*='product'] > li", "[class*='listing'] > li",
    "[class*='equipment'] > div", "[class*='equipment'] > li",
    # Table-based layouts (older sites)
    "table.products tr", "table.machines tr", "table.listings tr",
    # Very generic: any <div> with class containing common patterns
    "div[class*='card']", "div[class*='item']", "div[class*='result']",
    # Last resort: article without class constraint
    "article",
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
