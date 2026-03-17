"""
Lightweight 2-Stage Product Crawler
=====================================
Stage 1 — Link Extractor:
  • Fetches homepage, scans for hidden APIs FIRST
  • Extracts only product-relevant internal links (max 50)
  • Prioritizes English pages (/en/, ?lang=en)
  • Filters by: /product, /products, /machine, /machines,
                /catalog, /equipment, /item, /listing

Stage 2 — Product Extractor:
  • Visits each product link
  • Extracts: type, brand, model, image
  • Extraction order: JSON-LD → OG meta → h1/h2/title → regex
  • Brand normalized across languages (strips GmbH, S.r.l., etc.)

API Detection (runs before HTML parsing):
  • Scans HTML source for: fetch(, axios(, .json URLs, /api/ paths
  • Probes found endpoints for JSON product data
  • Skips HTML parsing entirely when API data found

Production guards:
  • Max 100 requests per site
  • 10 sec timeout per request
  • Deduplication of visited URLs
  • Realistic User-Agent headers

Usage:
    python lightweight_crawler.py https://example.com

    # Or import:
    from backend.crawler.lightweight_crawler import crawl
    results = crawl("https://example.com")

Output:
    [
      {"type": "...", "brand": "...", "model": "...",
       "image": "...", "url": "..."},
      ...
    ]
"""
from __future__ import annotations

import json
import logging
import re
import sys
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse, urldefrag, urlunparse

import requests
from bs4 import BeautifulSoup

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("crawler")

# ── Production constants ──────────────────────────────────────────────────────

MAX_REQUESTS   = 100    # hard cap on HTTP requests per site
MAX_LINKS      = 50     # max product links collected in Stage 1
REQUEST_TIMEOUT = 10    # seconds per request
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

# ── URL filtering patterns ────────────────────────────────────────────────────

# Paths that are DEFINITELY product/machine pages
PRODUCT_PATH_RE = re.compile(
    r"/(product|products|machine|machines|catalog|catalogue|"
    r"equipment|item|items|listing|listings|inventory|"
    r"shop|store|used|new|stock|"
    # European language equivalents
    r"maschine|maschinen|produkt|produkte|"   # German
    r"macchina|macchine|prodotto|prodotti|"    # Italian
    r"maquina|maquinas|catalogo|"              # Spanish
    r"produit|produits|machine|materiel|"      # French
    r"apparaat|apparaten|materieel"            # Dutch
    r")s?(/|$)",
    re.IGNORECASE,
)

# Paths to always skip
SKIP_PATH_RE = re.compile(
    r"/(cart|checkout|account|login|register|signin|signup|"
    r"password|reset|admin|wp-admin|wp-json|"
    r"contact|about|privacy|terms|faq|help|support|"
    r"blog|news|press|events|jobs|careers|sitemap|"
    r"feed|rss|tag|author|search|imprint|impressum|"
    r"datenschutz|cookie|gdpr)"
    r"|\.(pdf|jpg|jpeg|png|gif|svg|css|js|xml|zip|webp|ico)(\?|$)",
    re.IGNORECASE,
)

# English-language URL signals
ENGLISH_URL_RE = re.compile(r"(/en/|/en$|\blang=en\b|[?&]language=en)", re.IGNORECASE)

# API endpoint patterns found in page source / JS
API_SOURCE_PATTERNS = [
    re.compile(r"""(?:fetch|axios\.get|axios\.post|\.get|\.post)\s*\(\s*["'`]([^"'`\s]{5,})["'`]"""),
    re.compile(r"""(?:url|endpoint|apiUrl|api_url|baseUrl)\s*[=:]\s*["'`]([^"'`\s]{5,})["'`]"""),
    re.compile(r"""["'`]((?:https?://[^"'`\s]+)?(?:/api/|/rest/|/graphql)[^"'`\s]{3,})["'`]"""),
    re.compile(r"""["'`]([^"'`\s]+\.json(?:\?[^"'`\s]*)?)["'`]"""),
]

SKIP_API_RE = re.compile(
    r"(?:\.css|\.js|\.png|\.jpg|\.svg|\.ico|"
    r"schema\.org|cdnjs|googleapis|gstatic|jquery|bootstrap|fontawesome|"
    r"/wp-json/wp/v2/(?:posts|pages|comments|users))",
    re.IGNORECASE,
)

# Model number patterns (e.g. ABC-123, XYZ 4000, V-200-B)
MODEL_REGEX = re.compile(
    r"\b([A-Z]{1,5}[-_ ]?\d{2,6}(?:[-_ ][A-Z0-9]{1,4})?)\b"
)

# Corporate suffix normalization (strip from brand names)
CORP_SUFFIX_RE = re.compile(
    r"\b(GmbH|AG|KG|OHG|GbR|UG|"           # German
    r"S\.r\.l\.|S\.p\.A\.|S\.a\.s\.|S\.n\.c\.|"  # Italian
    r"S\.L\.|S\.A\.|S\.C\.|S\.A\.U\.|"      # Spanish
    r"SARL|SAS|SA|EURL|SNC|"                # French
    r"B\.V\.|N\.V\.|V\.O\.F\.|"             # Dutch
    r"Ltd\.?|PLC|LLP|LP|LLC|Corp\.?|Inc\.?|Co\.?)\s*$",
    re.IGNORECASE,
)


# ── HTTP session ──────────────────────────────────────────────────────────────

def _make_session() -> requests.Session:
    """Create a requests session with production headers."""
    s = requests.Session()
    s.headers.update({
        "User-Agent":      USER_AGENT,
        "Accept":          "text/html,application/xhtml+xml,application/json,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,de;q=0.7,fr;q=0.6",
        "Accept-Encoding": "gzip, deflate",
        "Connection":      "keep-alive",
    })
    return s


class RequestBudget:
    """Hard cap on total HTTP requests to protect memory and avoid bans."""

    def __init__(self, limit: int = MAX_REQUESTS):
        self.limit = limit
        self.used  = 0

    def get(self, session: requests.Session, url: str, **kwargs) -> Optional[requests.Response]:
        if self.used >= self.limit:
            log.warning("Request budget exhausted (%d/%d) — skipping %s",
                        self.used, self.limit, url)
            return None
        try:
            kwargs.setdefault("timeout", REQUEST_TIMEOUT)
            kwargs.setdefault("allow_redirects", True)
            r = session.get(url, **kwargs)
            self.used += 1
            return r
        except Exception as exc:
            log.debug("GET %s failed: %s", url, exc)
            return None


# ── URL helpers ───────────────────────────────────────────────────────────────

def _abs_url(href: str, base: str) -> Optional[str]:
    """Resolve href to absolute URL. Returns None for non-HTTP or garbage."""
    if not href:
        return None
    href = href.strip()
    if href.startswith(("#", "javascript:", "mailto:", "tel:", "data:")):
        return None
    try:
        url, _ = urldefrag(urljoin(base, href))
        p = urlparse(url)
        if p.scheme not in ("http", "https"):
            return None
        return url
    except Exception:
        return None


def _same_domain(url: str, base: str) -> bool:
    try:
        return urlparse(url).netloc == urlparse(base).netloc
    except Exception:
        return False


def _clean_url(url: str) -> str:
    """Strip query + fragment for deduplication."""
    p = urlparse(url)
    return urlunparse(p._replace(query="", fragment=""))


def _is_english(url: str) -> bool:
    return bool(ENGLISH_URL_RE.search(url))


def _score_link(url: str, base: str) -> int:
    """
    Score a URL for product-page likelihood.
    Higher = more likely to be a product/machine page.
    Returns 0 if should be skipped.
    """
    path = urlparse(url).path
    if SKIP_PATH_RE.search(path):
        return 0
    if not _same_domain(url, base):
        return 0
    score = 0
    if PRODUCT_PATH_RE.search(path):
        score += 10
    if _is_english(url):
        score += 5
    # Penalise very short paths (homepage-level)
    if len(path.strip("/")) < 3:
        return 0
    # Penalise paths that look like category pages without a slug
    segments = [s for s in path.split("/") if s]
    if len(segments) == 1 and not re.search(r"\d", path):
        score = max(score - 3, 0)
    return score


# ── Brand normalization ───────────────────────────────────────────────────────

def normalize_brand(brand: str) -> str:
    """
    Normalize a brand name across languages:
      • Strip corporate suffixes (GmbH, S.r.l., Ltd., etc.)
      • Normalize whitespace
      • Title-case the result

    Examples:
        "Maschinen GmbH"       → "Maschinen"
        "ABC Equipment S.r.l." → "ABC Equipment"
        "XYZ Corp."            → "XYZ"
    """
    if not brand:
        return ""
    brand = brand.strip()
    # Iteratively strip suffixes (there can be multiple, e.g. "Foo & Co. GmbH")
    for _ in range(3):
        cleaned = CORP_SUFFIX_RE.sub("", brand).strip(" ,&.")
        if cleaned == brand:
            break
        brand = cleaned
    return brand.strip()


# ── API detection & extraction ────────────────────────────────────────────────

def _find_api_candidates(html: str, base_url: str) -> List[str]:
    """
    Scan HTML source (and inline JS) for XHR/fetch call URLs.
    Returns a deduplicated list of candidate API endpoint URLs.
    """
    candidates: List[str] = []
    seen: set = set()

    def _collect(text: str) -> None:
        for pat in API_SOURCE_PATTERNS:
            for m in pat.finditer(text):
                raw = m.group(1).strip()
                if SKIP_API_RE.search(raw):
                    continue
                abs_url = urljoin(base_url, raw) if raw.startswith("/") else raw
                if abs_url.startswith("http") and abs_url not in seen:
                    seen.add(abs_url)
                    candidates.append(abs_url)

    _collect(html)
    return candidates


def _probe_api_endpoint(url: str, session: requests.Session,
                         budget: RequestBudget) -> Optional[List[dict]]:
    """
    Probe a single URL.  Returns a list of item dicts if it looks like a
    product API, otherwise None.
    """
    r = budget.get(session, url, headers={"Accept": "application/json"})
    if r is None or r.status_code != 200:
        return None
    ct = r.headers.get("content-type", "")
    if "json" not in ct and not url.endswith(".json"):
        return None
    try:
        data = r.json()
    except Exception:
        return None

    # Accept list of dicts
    if isinstance(data, list) and data and isinstance(data[0], dict):
        return data

    # Accept common wrappers
    for key in ("data", "results", "items", "products", "machines",
                "listings", "equipment", "records", "entries"):
        val = data.get(key) if isinstance(data, dict) else None
        if isinstance(val, list) and val and isinstance(val[0], dict):
            return val

    return None


def _items_from_api(html: str, base_url: str, session: requests.Session,
                    budget: RequestBudget) -> Optional[List[Dict]]:
    """
    Try to get product data directly from a hidden API.
    Returns list of normalized product dicts, or None if no API found.
    """
    candidates = _find_api_candidates(html, base_url)
    if not candidates:
        return None

    log.info("API scan found %d candidate endpoints", len(candidates))

    for url in candidates[:20]:
        log.info("Probing API: %s", url)
        items = _probe_api_endpoint(url, session, budget)
        if items:
            log.info("API hit! %d items from %s", len(items), url)
            results = []
            for item in items:
                product = _normalize_api_item(item, base_url)
                if product.get("brand") or product.get("model"):
                    results.append(product)
            if results:
                return results

    return None


def _normalize_api_item(raw: dict, base_url: str) -> Dict[str, Any]:
    """Map raw API dict fields → canonical {type, brand, model, image, url}."""
    def _get(*keys):
        for k in keys:
            v = raw.get(k)
            if v and isinstance(v, str):
                return v.strip()
        return ""

    brand = normalize_brand(_get("brand", "make", "manufacturer",
                                  "hersteller", "marque", "marca"))
    model = _get("model", "model_number", "modell", "modele", "modelo", "mpn")
    mtype = _get("type", "category", "machine_type", "typ",
                  "categorie", "categoria", "kind")

    # Fallback: extract model from title with regex
    if not model:
        title = _get("title", "name", "machine_name", "product_name", "label")
        m = MODEL_REGEX.search(title)
        if m:
            model = m.group(1)

    # Image
    image = ""
    for key in ("image", "photo", "thumbnail", "picture", "bild", "foto"):
        val = raw.get(key)
        if isinstance(val, str) and val.startswith("http"):
            image = val
            break
        if isinstance(val, list) and val:
            img = val[0]
            if isinstance(img, str):
                image = img
                break
            if isinstance(img, dict):
                image = img.get("url") or img.get("src") or ""
                break

    # URL
    url = ""
    for key in ("url", "link", "href", "permalink", "product_url", "machine_url"):
        val = raw.get(key)
        if isinstance(val, str):
            url = urljoin(base_url, val) if val.startswith("/") else val
            break

    return {
        "type":  mtype,
        "brand": brand,
        "model": model,
        "image": image,
        "url":   url,
    }


# ── Stage 1 — Link Extractor ──────────────────────────────────────────────────

def extract_product_links(html: str, base_url: str) -> List[str]:
    """
    Stage 1: Extract internal links that likely lead to product/machine pages.

    Strategy:
    1. Score every internal link (product-path match + English bonus).
    2. Prioritise English URLs.
    3. Deduplicate by cleaned URL.
    4. Return max MAX_LINKS results.
    """
    soup = BeautifulSoup(html, "lxml")
    scored: Dict[str, int] = {}  # clean_url → score

    for a in soup.find_all("a", href=True):
        url = _abs_url(a["href"], base_url)
        if not url:
            continue
        score = _score_link(url, base_url)
        if score <= 0:
            continue
        clean = _clean_url(url)
        if score > scored.get(clean, 0):
            scored[clean] = score

    # Sort: English URLs first, then by score
    ranked = sorted(scored.items(), key=lambda x: (not _is_english(x[0]), -x[1]))

    seen: set = set()
    result: List[str] = []
    for clean, _ in ranked:
        if clean not in seen:
            seen.add(clean)
            result.append(clean)
        if len(result) >= MAX_LINKS:
            break

    log.info("Stage 1: found %d product links (from %s)", len(result), base_url)
    return result


# ── Stage 2 — Product Data Extraction ────────────────────────────────────────

def _extract_json_ld(soup: BeautifulSoup) -> Dict[str, Any]:
    """Extract product data from JSON-LD schema.org/Product blocks."""
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string or "{}")
            if isinstance(data, list):
                data = next(
                    (d for d in data if "Product" in str(d.get("@type", ""))), {}
                )
            if "Product" not in str(data.get("@type", "")):
                continue

            brand_raw = data.get("brand") or {}
            brand = normalize_brand(
                brand_raw.get("name") if isinstance(brand_raw, dict) else str(brand_raw or "")
            )
            model = str(data.get("model") or data.get("mpn") or "")
            name  = str(data.get("name") or "")

            # Image
            imgs = data.get("image") or []
            if isinstance(imgs, str):
                imgs = [imgs]
            elif isinstance(imgs, dict):
                imgs = [imgs.get("url") or imgs.get("contentUrl") or ""]
            image = next((i for i in imgs if i), "")

            # Category
            mtype = str(data.get("category") or data.get("productType") or "")

            return {"brand": brand, "model": model, "type": mtype,
                    "image": image, "name": name}
        except Exception:
            continue
    return {}


def _extract_og_meta(soup: BeautifulSoup) -> Dict[str, Any]:
    """Extract data from OpenGraph meta tags."""
    result: Dict[str, Any] = {}
    for meta in soup.find_all("meta"):
        prop    = meta.get("property") or meta.get("name") or ""
        content = (meta.get("content") or "").strip()
        if prop == "og:title" and not result.get("name"):
            result["name"] = content
        elif prop == "og:image" and not result.get("image"):
            result["image"] = content
        elif prop == "og:description" and not result.get("description"):
            result["description"] = content
    return result


def _extract_heuristics(soup: BeautifulSoup, url: str, base_url: str) -> Dict[str, Any]:
    """
    CSS/HTML heuristics extraction.
    Tries common selectors for title, brand, model, type, image.
    """
    result: Dict[str, Any] = {}

    # Title / machine name
    for sel in ["h1.product-title", "h1.machine-title", ".product-title h1",
                "[itemprop='name']", "h1"]:
        el = soup.select_one(sel)
        if el:
            result["name"] = el.get_text(strip=True)
            break

    # Brand
    for sel in ["[itemprop='brand']", ".brand", ".manufacturer",
                ".make", '[class*="brand"]', '[class*="manufacturer"]']:
        el = soup.select_one(sel)
        if el:
            result["brand"] = normalize_brand(el.get_text(strip=True))
            break

    # Model
    for sel in ["[itemprop='model']", ".model", ".model-number",
                '[class*="model"]', '[data-model]']:
        el = soup.select_one(sel)
        if el:
            result["model"] = el.get_text(strip=True)
            break

    # Machine type: try breadcrumb or category meta
    for sel in [".breadcrumb li:nth-child(2)", "nav.breadcrumb li:nth-child(2)",
                "[itemprop='category']", ".category", ".machine-type",
                '[class*="category"]', '[class*="type"]']:
        el = soup.select_one(sel)
        if el:
            text = el.get_text(strip=True)
            if text and len(text) < 80:
                result["type"] = text
                break

    # Try first h2 as type fallback (often "Category > Machine Type")
    if not result.get("type"):
        h2 = soup.find("h2")
        if h2:
            text = h2.get_text(strip=True)
            if text and len(text) < 80:
                result["type"] = text

    # Image
    if not result.get("image"):
        for sel in ["[itemprop='image']", ".product-image img",
                    ".machine-image img", ".gallery img",
                    ".main-image img", '[class*="product-img"] img',
                    '[class*="machine-img"] img', "img.main"]:
            img = soup.select_one(sel)
            if img:
                src = img.get("src") or img.get("data-src") or img.get("data-lazy") or ""
                if src:
                    abs_src = _abs_url(src, base_url)
                    if abs_src:
                        result["image"] = abs_src
                        break

    return result


def _infer_model_from_text(soup: BeautifulSoup, url: str) -> str:
    """
    Regex fallback: scan h1/h2/title text for model-number patterns like ABC-123.
    """
    sources = []
    title = soup.find("title")
    if title:
        sources.append(title.get_text(strip=True))
    for tag in soup.find_all(["h1", "h2"]):
        sources.append(tag.get_text(strip=True))

    # Also try URL path (e.g., /machines/ABC-123/)
    sources.append(urlparse(url).path.replace("-", " ").replace("_", " "))

    for text in sources:
        m = MODEL_REGEX.search(text.upper())
        if m:
            return m.group(1)
    return ""


def _infer_type_from_url(url: str) -> str:
    """Infer machine type from the URL path segments."""
    path = urlparse(url).path
    segments = [s for s in path.split("/") if s and not re.search(r"\d", s) and len(s) > 2]
    # Skip generic segments
    skip = {"products", "product", "machines", "machine", "equipment",
            "catalog", "catalogue", "en", "de", "fr", "it", "es", "nl"}
    for seg in segments:
        if seg.lower() not in skip:
            return seg.replace("-", " ").replace("_", " ").title()
    return ""


def extract_product_data(html: str, url: str, base_url: str) -> Dict[str, Any]:
    """
    Stage 2: Extract structured product data from a product page.

    Extraction order (highest priority first):
      1. JSON-LD schema.org/Product
      2. OpenGraph meta tags
      3. CSS/HTML heuristics (h1, brand selectors, etc.)
      4. Regex model fallback + URL-based type inference

    Returns dict with keys: type, brand, model, image, url
    """
    soup = BeautifulSoup(html, "lxml")

    # Layer 1 — JSON-LD
    data = _extract_json_ld(soup)

    # Layer 2 — OG meta (fill gaps)
    og = _extract_og_meta(soup)
    for k, v in og.items():
        if v and not data.get(k):
            data[k] = v

    # Layer 3 — CSS heuristics (fill remaining gaps)
    heuristics = _extract_heuristics(soup, url, base_url)
    for k, v in heuristics.items():
        if v and not data.get(k):
            data[k] = v

    # Layer 4 — Regex / URL fallbacks
    if not data.get("model"):
        data["model"] = _infer_model_from_text(soup, url)

    if not data.get("type"):
        data["type"] = _infer_type_from_url(url)

    # Normalise brand one final time (catches heuristic results)
    if data.get("brand"):
        data["brand"] = normalize_brand(str(data["brand"]))

    return {
        "type":  data.get("type",  ""),
        "brand": data.get("brand", ""),
        "model": data.get("model", ""),
        "image": data.get("image", ""),
        "url":   url,
    }


# ── Shopify / WooCommerce fast-path ───────────────────────────────────────────

def _try_shopify(base_url: str, session: requests.Session,
                 budget: RequestBudget) -> Optional[List[Dict]]:
    url = urljoin(base_url, "/products.json?limit=250")
    r = budget.get(session, url, headers={"Accept": "application/json"})
    if r is None or r.status_code != 200:
        return None
    try:
        products = r.json().get("products") or []
        if not products:
            return None
        log.info("Shopify API found — %d products", len(products))
        results = []
        for p in products:
            brand = normalize_brand(p.get("vendor") or "")
            model = p.get("handle") or p.get("title") or ""
            mtype = p.get("product_type") or ""
            # First variant image
            image = ""
            imgs = p.get("images") or []
            if imgs:
                image = imgs[0].get("src") or ""
            product_url = urljoin(base_url, f"/products/{p.get('handle', '')}")
            results.append({
                "type":  mtype,
                "brand": brand,
                "model": model,
                "image": image,
                "url":   product_url,
            })
        return results or None
    except Exception:
        return None


def _try_woocommerce(base_url: str, session: requests.Session,
                     budget: RequestBudget) -> Optional[List[Dict]]:
    url = urljoin(base_url, "/wp-json/wc/v3/products?per_page=100")
    r = budget.get(session, url, headers={"Accept": "application/json"})
    if r is None or r.status_code != 200:
        return None
    try:
        products = r.json()
        if not isinstance(products, list) or not products:
            return None
        log.info("WooCommerce API found — %d products", len(products))
        results = []
        for p in products:
            brand = normalize_brand(p.get("brands", [{}])[0].get("name", "")
                                    if p.get("brands") else "")
            model = p.get("sku") or p.get("slug") or ""
            mtype = (p.get("categories") or [{}])[0].get("name", "") if p.get("categories") else ""
            image = (p.get("images") or [{}])[0].get("src", "") if p.get("images") else ""
            results.append({
                "type":  mtype,
                "brand": brand,
                "model": model,
                "image": image,
                "url":   p.get("permalink") or urljoin(base_url, f"/product/{p.get('slug','')}"),
            })
        return results or None
    except Exception:
        return None


# ── Main crawl entry point ────────────────────────────────────────────────────

def crawl(start_url: str) -> List[Dict[str, Any]]:
    """
    Full 2-stage crawl of a website.

    Stage 1: Discover product links (max 50)
    Stage 2: Extract product data from each link

    Returns list of {type, brand, model, image, url} dicts.
    """
    session = _make_session()
    budget  = RequestBudget(MAX_REQUESTS)
    visited: set = set()
    results: List[Dict] = []

    # ── Normalise start URL ───────────────────────────────────────────────────
    if not start_url.startswith("http"):
        start_url = "https://" + start_url
    _parsed  = urlparse(start_url)
    base_url = f"{_parsed.scheme}://{_parsed.netloc}"

    log.info("=" * 60)
    log.info("Crawling: %s", start_url)
    log.info("=" * 60)

    # ── Fetch homepage ────────────────────────────────────────────────────────
    log.info("Visiting: %s", start_url)
    r = budget.get(session, start_url)
    if r is None or not r.ok:
        log.warning("Failed to load homepage — aborting")
        return []

    homepage_html = r.text
    visited.add(_clean_url(start_url))

    # ── Fast-path: Shopify / WooCommerce ─────────────────────────────────────
    shopify = _try_shopify(base_url, session, budget)
    if shopify:
        log.info("Returning %d items from Shopify API", len(shopify))
        return shopify

    woo = _try_woocommerce(base_url, session, budget)
    if woo:
        log.info("Returning %d items from WooCommerce API", len(woo))
        return woo

    # ── API detection from HTML source ───────────────────────────────────────
    api_data = _items_from_api(homepage_html, base_url, session, budget)
    if api_data:
        log.info("Returning %d items from detected API", len(api_data))
        return api_data

    # ── Stage 1 — Extract product links ──────────────────────────────────────
    product_links = extract_product_links(homepage_html, base_url)

    # If no product links found from homepage, try /en/ variant
    if not product_links and not _is_english(start_url):
        for en_path in ("/en/", "/en"):
            en_url = base_url + en_path
            if en_url not in visited:
                log.info("Trying English sub-path: %s", en_url)
                er = budget.get(session, en_url)
                if er and er.ok:
                    product_links = extract_product_links(er.text, base_url)
                    visited.add(en_url)
                    if product_links:
                        break

    if not product_links:
        log.warning("No product links found — site may be JS-rendered or unsupported")
        return []

    # ── Stage 2 — Extract product data ───────────────────────────────────────
    for link in product_links:
        if budget.used >= MAX_REQUESTS:
            log.warning("Budget exhausted — stopping early")
            break

        clean = _clean_url(link)
        if clean in visited:
            continue
        visited.add(clean)

        log.info("Visiting: %s", link)
        pr = budget.get(session, link)
        if pr is None or not pr.ok:
            log.debug("Skipping %s (status %s)", link,
                      pr.status_code if pr else "error")
            continue

        try:
            data = extract_product_data(pr.text, link, base_url)
        except Exception as exc:
            log.warning("Extraction error on %s: %s", link, exc)
            continue

        # Only keep items with at least one meaningful field
        if data.get("brand") or data.get("model") or data.get("type"):
            log.info("Found: brand=%r  model=%r  type=%r",
                     data["brand"], data["model"], data["type"])
            results.append(data)
        else:
            log.debug("No data extracted from %s — skipping", link)

    log.info("-" * 60)
    log.info("Done. Total products found: %d  (requests used: %d/%d)",
             len(results), budget.used, MAX_REQUESTS)
    return results


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python lightweight_crawler.py <url>")
        sys.exit(1)

    url = sys.argv[1]
    data = crawl(url)
    print(json.dumps(data, indent=2, ensure_ascii=False))
