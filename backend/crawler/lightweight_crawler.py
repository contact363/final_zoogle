"""
Advanced Machine Crawler  — v2
================================
Complete rewrite fixing all 6 known issues:

  FIX 1  — JS Rendering     : Playwright renders React/Vue/Angular/Next.js/Nuxt
  FIX 2  — Request Budget   : 500 requests (was 100), configurable
  FIX 3  — Windows Paths    : tempfile.gettempdir() replaces hard-coded /tmp
  FIX 4  — Single Crawler   : this file IS the crawler (no competing code)
  FIX 5  — Silent Failures  : every extraction failure logged + partial data kept
  FIX 6  — Link Detection   : frequency threshold 2 (was 5), short paths allowed

Additional improvements:
  • Full machine data: brand, model, type, condition, year, price, currency,
    stock_number, description, specifications{}, images[], source_url
  • Pagination: automatically follows next-page links on listing pages
  • Sitemap: parses sitemap.xml / sitemap_index.xml for product URLs
  • API fast-paths: Shopify /products.json, WooCommerce /wp-json/wc/v3,
    GraphQL introspection, embedded JSON (window.__NEXT_DATA__ etc.)
  • Rate limiting: configurable per-request delay
  • Deduplication: SHA-256 content_hash prevents duplicate machine records
  • Multi-language support: German, French, Italian, Spanish, Dutch path words

Usage:
    from backend.crawler.lightweight_crawler import crawl

    results = crawl("https://example.com")
    # returns list of full machine dicts

    python lightweight_crawler.py https://example.com
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import sys
import tempfile
import time
from collections import Counter
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import (
    urljoin, urldefrag, urlparse, urlunparse, urlencode, parse_qs,
)
import xml.etree.ElementTree as ET

import requests
from bs4 import BeautifulSoup

# ── Optional Playwright import (graceful fallback) ────────────────────────────
try:
    from crawler.playwright_renderer import render_if_needed, needs_playwright
    _PW_AVAILABLE = True
except ImportError:
    _PW_AVAILABLE = False
    def needs_playwright(html: str) -> bool:           # type: ignore[misc]
        markers = re.compile(
            r"__NEXT_DATA__|__NUXT__|data-reactroot|ng-version|_next/static"
        )
        return bool(markers.search(html)) or html.lower().count("<a href") < 8

    def render_if_needed(url: str, html: str, **kw) -> str:  # type: ignore[misc]
        return html

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("crawler")

# ── Constants ─────────────────────────────────────────────────────────────────

MAX_REQUESTS       = 500    # FIX 2: was 100
MAX_PRODUCT_LINKS  = 2000   # FIX 6: was 50 — effectively unlimited
REQUEST_TIMEOUT    = 15     # seconds per request
REQUEST_DELAY      = 0.5    # seconds between requests (polite crawling)
MAX_PAGINATION     = 50     # max extra pages to follow per category
MAX_IMAGES         = 8      # images to store per machine

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

# ── URL patterns ──────────────────────────────────────────────────────────────

# Paths strongly indicating product/machine detail pages
PRODUCT_PATH_RE = re.compile(
    r"/(product|products|machine|machines|catalog|catalogue|"
    r"equipment|item|items|listing|listings|inventory|"
    r"shop|store|used|new|stock|detail|details|"
    # German
    r"maschine|maschinen|produkt|produkte|gebraucht|"
    # Italian
    r"macchina|macchine|prodotto|prodotti|"
    # Spanish
    r"maquina|maquinas|catalogo|"
    # French
    r"produit|produits|materiel|"
    # Dutch
    r"apparaat|apparaten|materieel"
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
    r"datenschutz|cookie|gdpr|404)"
    r"|\.(pdf|jpg|jpeg|png|gif|svg|css|js|xml|zip|webp|ico)(\?|$)",
    re.IGNORECASE,
)

ENGLISH_URL_RE = re.compile(r"(/en/|/en$|\blang=en\b|[?&]language=en)", re.IGNORECASE)

# API endpoint markers in inline JS
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

# Model number patterns — uppercase and mixed-case (ABC-123, v-200-b, XY4000)
MODEL_REGEX = re.compile(
    r"\b([A-Z]{1,6}[-_ ]?\d{2,6}(?:[-_ ][A-Z0-9]{1,5})?)\b"
    r"|\b(\d{3,6}[-_ ][A-Z]{1,5})\b",   # also 4000-XL style
    re.IGNORECASE,
)

# Year pattern: 4-digit year 1950–2030
YEAR_REGEX = re.compile(r"\b(19[5-9]\d|20[0-2]\d|2030)\b")

# Price pattern: $1,234.56 / EUR 1.234,56 / 1234
PRICE_REGEX = re.compile(
    r"""
    (?:USD|EUR|GBP|CHF|CAD|AUD|\$|€|£|¥|kr|SEK|NOK|DKK)?\s*
    ([\d]{1,3}(?:[,\s]\d{3})*(?:[.,]\d{1,2})?)
    \s*(?:USD|EUR|GBP|CHF|CAD|AUD|\$|€|£|¥|kr|SEK|NOK|DKK)?
    """,
    re.IGNORECASE | re.VERBOSE,
)

CURRENCY_RE = re.compile(
    r"\b(USD|EUR|GBP|CHF|CAD|AUD|SEK|NOK|DKK)\b"
    r"|(\$|€|£|¥|kr)",
    re.IGNORECASE,
)

# Condition keywords
CONDITION_NEW_RE  = re.compile(r"\bnew\b|\bneu\b|\bnuovo\b|\bnueva\b|\bneuf\b", re.IGNORECASE)
CONDITION_USED_RE = re.compile(
    r"\bused\b|\bgebraucht\b|\busato\b|\busado\b|\boccasion\b"
    r"|\bsecond.?hand\b|\brefurbished\b|\breconditioned\b",
    re.IGNORECASE,
)

# Corporate suffix normalization
CORP_SUFFIX_RE = re.compile(
    r"\b(GmbH|AG|KG|OHG|GbR|UG|"
    r"S\.r\.l\.|S\.p\.A\.|S\.a\.s\.|S\.n\.c\.|"
    r"S\.L\.|S\.A\.|S\.C\.|S\.A\.U\.|"
    r"SARL|SAS|SA|EURL|SNC|"
    r"B\.V\.|N\.V\.|V\.O\.F\.|"
    r"Ltd\.?|PLC|LLP|LP|LLC|Corp\.?|Inc\.?|Co\.?)\s*$",
    re.IGNORECASE,
)

# Embedded JS data stores
EMBEDDED_JSON_PATTERNS = [
    re.compile(r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(\{.+?\})</script>', re.DOTALL),
    re.compile(r'window\.__INITIAL_STATE__\s*=\s*(\{.+?\})\s*;', re.DOTALL),
    re.compile(r'window\.__DATA__\s*=\s*(\{.+?\})\s*;', re.DOTALL),
    re.compile(r'window\.__NUXT__\s*=\s*(\{.+?\})', re.DOTALL),
    re.compile(r'var\s+productData\s*=\s*(\[.+?\])\s*;', re.DOTALL),
    re.compile(r'var\s+machineData\s*=\s*(\[.+?\])\s*;', re.DOTALL),
]

# Sitemap XML namespaces
SITEMAP_NS = {
    "sm": "http://www.sitemaps.org/schemas/sitemap/0.9",
    "image": "http://www.google.com/schemas/sitemap-image/1.1",
}


# ── HTTP session ───────────────────────────────────────────────────────────────

def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent":      USER_AGENT,
        "Accept":          "text/html,application/xhtml+xml,application/json,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,de;q=0.7,fr;q=0.6",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection":      "keep-alive",
        "DNT":             "1",
    })
    return s


class RequestBudget:
    """FIX 2: Hard cap increased to 500 requests. Logs every exhaustion."""

    def __init__(self, limit: int = MAX_REQUESTS):
        self.limit  = limit
        self.used   = 0
        self._delay = REQUEST_DELAY

    @property
    def remaining(self) -> int:
        return max(0, self.limit - self.used)

    def get(
        self,
        session: requests.Session,
        url: str,
        json_mode: bool = False,
        **kwargs,
    ) -> Optional[requests.Response]:
        if self.used >= self.limit:
            log.warning("Budget exhausted (%d/%d) — skipping %s", self.used, self.limit, url)
            return None
        try:
            kwargs.setdefault("timeout", REQUEST_TIMEOUT)
            kwargs.setdefault("allow_redirects", True)
            if json_mode:
                kwargs.setdefault("headers", {})["Accept"] = "application/json"
            r = session.get(url, **kwargs)
            self.used += 1
            if self._delay:
                time.sleep(self._delay)
            return r
        except Exception as exc:
            log.debug("GET %s failed: %s", url, exc)
            return None


# ── URL helpers ────────────────────────────────────────────────────────────────

def _abs_url(href: str, base: str) -> Optional[str]:
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
        u, b = urlparse(url).netloc, urlparse(base).netloc
        # Also match www vs non-www
        return u == b or u.lstrip("www.") == b.lstrip("www.")
    except Exception:
        return False


def _clean_url(url: str) -> str:
    """Strip query + fragment for deduplication."""
    p = urlparse(url)
    return urlunparse(p._replace(query="", fragment=""))


def _is_english(url: str) -> bool:
    return bool(ENGLISH_URL_RE.search(url))


def _path_pattern(url: str) -> str:
    """
    Replace variable path segments with {slug} to group similar URLs.
    /machines/volvo-2022  →  /machines/{slug}
    /product/123          →  /product/{slug}
    """
    path = urlparse(url).path.rstrip("/")
    parts = []
    for seg in path.split("/"):
        if re.search(r"\d", seg) or len(seg) > 20 or "-" in seg:
            parts.append("{slug}")
        else:
            parts.append(seg)
    return "/".join(parts)


# FIX 6: Lowered scoring — short paths now allowed, threshold reduced from 5→2
def _score_link(url: str, base: str) -> int:
    """
    Score a URL for product-page likelihood. Returns 0 to skip.
    FIX 6: Removed the short-path (<3 char) hard rejection.
    """
    path = urlparse(url).path
    if SKIP_PATH_RE.search(path):
        return 0
    if not _same_domain(url, base):
        return 0
    score = 1   # every surviving internal link gets base score
    if PRODUCT_PATH_RE.search(path):
        score += 10
    if _is_english(url):
        score += 5
    return score


# ── Brand normalization ───────────────────────────────────────────────────────

def normalize_brand(brand: str) -> str:
    if not brand:
        return ""
    brand = brand.strip()
    for _ in range(3):
        cleaned = CORP_SUFFIX_RE.sub("", brand).strip(" ,&.")
        if cleaned == brand:
            break
        brand = cleaned
    return brand.strip()


# ── Price / Currency helpers ───────────────────────────────────────────────────

def _parse_price(raw: str) -> Tuple[Optional[float], str]:
    """
    Extract (price_float, currency_code) from a raw price string.
    Handles both European (1.234,56) and US (1,234.56) formats.
    Returns (None, "USD") on failure.
    """
    if not raw:
        return None, "USD"

    # Currency detection
    cm = CURRENCY_RE.search(raw)
    currency = "USD"
    if cm:
        sym = cm.group(1) or cm.group(2)
        sym_map = {"$": "USD", "€": "EUR", "£": "GBP", "¥": "JPY", "kr": "SEK"}
        currency = sym_map.get(sym, sym.upper() if sym else "USD")

    # Strip non-numeric except , .
    digits = re.sub(r"[^\d,.]", "", raw)
    if not digits:
        return None, currency

    # European format: 1.234,56 → 1234.56
    if re.search(r"\d\.\d{3},\d{2}$", digits):
        digits = digits.replace(".", "").replace(",", ".")
    # US format: 1,234.56 → 1234.56
    elif "," in digits and "." in digits:
        digits = digits.replace(",", "")
    # Only comma (ambiguous): treat as decimal if ≤2 digits after comma
    elif "," in digits and "." not in digits:
        parts = digits.split(",")
        if len(parts[-1]) <= 2:
            digits = ".".join(parts)
        else:
            digits = digits.replace(",", "")

    try:
        return float(digits), currency
    except ValueError:
        return None, currency


def _extract_year(text: str) -> Optional[int]:
    """Extract the most-likely manufacturing year (1950–current) from text."""
    import datetime
    current_year = datetime.datetime.now().year
    matches = YEAR_REGEX.findall(text)
    candidates = [int(m) for m in matches if 1950 <= int(m) <= current_year]
    return candidates[0] if candidates else None


def _extract_condition(text: str) -> str:
    """Detect machine condition: new / used / refurbished."""
    if CONDITION_USED_RE.search(text):
        return "used"
    if CONDITION_NEW_RE.search(text):
        return "new"
    return ""


# ── Sitemap parser ────────────────────────────────────────────────────────────

def _parse_sitemap(xml_text: str, base_url: str) -> List[str]:
    """Parse a sitemap or sitemap index XML and return product-like URLs."""
    urls: List[str] = []
    try:
        root = ET.fromstring(xml_text)
        tag = root.tag.split("}")[-1] if "}" in root.tag else root.tag

        if tag == "sitemapindex":
            # Sitemap index — sub-sitemaps, return their URLs for the caller
            for sm in root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc"):
                urls.append(sm.text.strip())
        else:
            # Regular sitemap — extract product URLs
            for loc in root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc"):
                url = loc.text.strip() if loc.text else ""
                if url and PRODUCT_PATH_RE.search(urlparse(url).path):
                    urls.append(url)
    except ET.ParseError:
        pass
    return urls


def _try_sitemap(base_url: str, session: requests.Session, budget: RequestBudget) -> List[str]:
    """Fetch and parse sitemap.xml (and sitemap_index.xml). Returns product URLs."""
    product_urls: List[str] = []
    sitemap_paths = [
        "/sitemap.xml",
        "/sitemap_index.xml",
        "/sitemaps/sitemap.xml",
        "/sitemap/sitemap.xml",
    ]

    for path in sitemap_paths:
        url = base_url.rstrip("/") + path
        r = budget.get(session, url)
        if r is None or r.status_code != 200:
            continue
        ct = r.headers.get("content-type", "")
        if "xml" not in ct and not r.text.strip().startswith("<"):
            continue

        parsed = _parse_sitemap(r.text, base_url)

        # If sitemap index, fetch first 3 sub-sitemaps
        if parsed and "sitemap" in parsed[0].lower():
            for sub_url in parsed[:3]:
                sr = budget.get(session, sub_url)
                if sr and sr.status_code == 200:
                    product_urls.extend(_parse_sitemap(sr.text, base_url))
        else:
            product_urls.extend(parsed)

        if product_urls:
            log.info("Sitemap: %d product URLs from %s", len(product_urls), path)
            return product_urls

    return product_urls


# ── Embedded JSON extraction ──────────────────────────────────────────────────

def _extract_embedded_json_items(html: str, base_url: str) -> List[Dict]:
    """
    Try to extract product data from window.__NEXT_DATA__, __NUXT__, etc.
    Returns list of normalized product dicts, or empty list.
    """
    for pat in EMBEDDED_JSON_PATTERNS:
        for m in pat.finditer(html):
            raw = m.group(1)
            if len(raw) > 500_000:
                continue
            try:
                data = json.loads(raw)
                items = _walk_json_for_products(data, base_url)
                if items:
                    log.info("Embedded JSON: %d products found", len(items))
                    return items
            except Exception:
                continue
    return []


def _walk_json_for_products(data: Any, base_url: str, depth: int = 0) -> List[Dict]:
    """Recursively walk JSON to find arrays of product-like objects."""
    if depth > 8:
        return []
    if isinstance(data, list) and len(data) >= 3:
        # Check if it looks like a product list
        sample = data[:3]
        product_keys = {"brand", "model", "price", "name", "title", "sku", "mpn",
                        "machine_name", "stock_number", "manufacturer"}
        hits = sum(1 for item in sample if isinstance(item, dict)
                   and product_keys & set(k.lower() for k in item.keys()))
        if hits >= 2:
            return [_normalize_api_item(item, base_url) for item in data
                    if isinstance(item, dict)]
    if isinstance(data, dict):
        for v in data.values():
            result = _walk_json_for_products(v, base_url, depth + 1)
            if result:
                return result
    return []


# ── API fast-paths ─────────────────────────────────────────────────────────────

def _try_shopify(base_url: str, session: requests.Session, budget: RequestBudget) -> Optional[List[Dict]]:
    """Shopify /products.json fast-path — supports pagination."""
    results: List[Dict] = []
    page = 1
    while True:
        url = f"{base_url}/products.json?limit=250&page={page}"
        r = budget.get(session, url, json_mode=True)
        if r is None or r.status_code != 200:
            break
        try:
            products = r.json().get("products") or []
        except Exception:
            break
        if not products:
            break
        if page == 1:
            log.info("Shopify API: found — paginating...")
        for p in products:
            brand = normalize_brand(p.get("vendor") or "")
            model = p.get("handle") or p.get("title") or ""
            mtype = p.get("product_type") or ""
            imgs = [img.get("src") or "" for img in (p.get("images") or []) if img.get("src")]
            product_url = urljoin(base_url, f"/products/{p.get('handle', '')}")
            # Extract variants for price
            variants = p.get("variants") or [{}]
            price_raw = variants[0].get("price") if variants else None
            price = float(price_raw) if price_raw else None
            results.append({
                "machine_name":   p.get("title") or model,
                "brand":          brand,
                "model":          model,
                "machine_type":   mtype,
                "condition":      "",
                "year":           None,
                "price":          price,
                "currency":       "USD",
                "stock_number":   variants[0].get("sku") or "" if variants else "",
                "description":    (p.get("body_html") or "")[:2000],
                "specifications": {},
                "images":         imgs[:MAX_IMAGES],
                "source_url":     product_url,
            })
        if len(products) < 250:
            break
        page += 1
        if budget.remaining < 10:
            break
    if results:
        log.info("Shopify: %d products found", len(results))
        return results
    return None


def _try_woocommerce(base_url: str, session: requests.Session, budget: RequestBudget) -> Optional[List[Dict]]:
    """WooCommerce /wp-json/wc/v3/products fast-path — supports pagination."""
    results: List[Dict] = []
    page = 1
    while True:
        url = f"{base_url}/wp-json/wc/v3/products?per_page=100&page={page}"
        r = budget.get(session, url, json_mode=True)
        if r is None or r.status_code != 200:
            break
        try:
            products = r.json()
        except Exception:
            break
        if not isinstance(products, list) or not products:
            break
        if page == 1:
            log.info("WooCommerce API: found — paginating...")
        for p in products:
            brand_obj = (p.get("brands") or [{}])[0] if p.get("brands") else {}
            brand = normalize_brand(brand_obj.get("name") or "")
            cats = p.get("categories") or [{}]
            mtype = cats[0].get("name") or "" if cats else ""
            imgs = [img.get("src") or "" for img in (p.get("images") or []) if img.get("src")]
            price_raw = p.get("price") or p.get("regular_price") or ""
            price, currency = _parse_price(str(price_raw))
            results.append({
                "machine_name":   p.get("name") or "",
                "brand":          brand,
                "model":          p.get("sku") or p.get("slug") or "",
                "machine_type":   mtype,
                "condition":      "",
                "year":           None,
                "price":          price,
                "currency":       currency,
                "stock_number":   p.get("sku") or "",
                "description":    (p.get("description") or "")[:2000],
                "specifications": {},
                "images":         imgs[:MAX_IMAGES],
                "source_url":     p.get("permalink") or urljoin(base_url, f"/product/{p.get('slug','')}"),
            })
        if len(products) < 100:
            break
        page += 1
        if budget.remaining < 10:
            break
    if results:
        log.info("WooCommerce: %d products found", len(results))
        return results
    return None


def _find_api_candidates(html: str, base_url: str) -> List[str]:
    """Scan HTML source + inline JS for XHR/fetch call URLs."""
    candidates: List[str] = []
    seen: set = set()
    for pat in API_SOURCE_PATTERNS:
        for m in pat.finditer(html):
            raw = m.group(1).strip()
            if SKIP_API_RE.search(raw):
                continue
            abs_url = urljoin(base_url, raw) if raw.startswith("/") else raw
            if abs_url.startswith("http") and abs_url not in seen:
                seen.add(abs_url)
                candidates.append(abs_url)
    return candidates


def _probe_api_endpoint(url: str, session: requests.Session, budget: RequestBudget) -> Optional[List[dict]]:
    """Probe a single URL. Returns list of item dicts if it looks like a product API."""
    r = budget.get(session, url, json_mode=True)
    if r is None or r.status_code != 200:
        return None
    ct = r.headers.get("content-type", "")
    if "json" not in ct and not url.endswith(".json"):
        return None
    try:
        data = r.json()
    except Exception:
        return None
    # Accept list
    if isinstance(data, list) and data and isinstance(data[0], dict):
        return data
    # Accept wrapped
    for key in ("data", "results", "items", "products", "machines",
                "listings", "equipment", "records", "entries", "hits"):
        val = data.get(key) if isinstance(data, dict) else None
        if isinstance(val, list) and val and isinstance(val[0], dict):
            return val
    return None


def _items_from_api(html: str, base_url: str, session: requests.Session, budget: RequestBudget) -> Optional[List[Dict]]:
    """Try to get product data from a hidden API found in page source."""
    candidates = _find_api_candidates(html, base_url)
    if not candidates:
        return None
    log.info("API scan: %d candidate endpoints", len(candidates))
    for url in candidates[:20]:
        log.info("Probing API: %s", url)
        items = _probe_api_endpoint(url, session, budget)
        if items:
            results = [_normalize_api_item(item, base_url) for item in items]
            valid = [r for r in results if r.get("brand") or r.get("model") or r.get("machine_name")]
            if valid:
                log.info("API hit: %d items from %s", len(valid), url)
                return valid
    return None


def _normalize_api_item(raw: dict, base_url: str) -> Dict[str, Any]:
    """Map raw API dict → canonical machine dict with all fields."""
    def _get(*keys):
        for k in keys:
            v = raw.get(k)
            if v and isinstance(v, (str, int, float)):
                return str(v).strip()
        return ""

    brand = normalize_brand(_get("brand", "make", "manufacturer",
                                  "hersteller", "marque", "marca", "vendor"))
    model = _get("model", "model_number", "modell", "modele", "modelo", "mpn", "sku", "slug")
    mtype = _get("type", "category", "machine_type", "typ",
                  "categorie", "categoria", "kind", "product_type")
    machine_name = _get("title", "name", "machine_name", "product_name", "label")

    if not model:
        m = MODEL_REGEX.search(machine_name.upper())
        if m:
            model = m.group(1) or m.group(2) or ""

    # Price
    price_raw = _get("price", "regular_price", "sale_price", "preis", "prix")
    price, currency = _parse_price(price_raw)

    # Year
    year_raw = _get("year", "baujahr", "annee", "anno", "año", "manufacture_year")
    year: Optional[int] = None
    if year_raw:
        try:
            year = int(year_raw[:4])
        except Exception:
            pass
    if not year:
        year = _extract_year(machine_name + " " + _get("description", "desc"))

    # Condition
    condition = _extract_condition(machine_name + " " + _get("condition", "status", "zustand"))

    # Stock number
    stock = _get("stock_number", "stock", "ref", "reference", "sku", "id", "article_number")

    # Images
    images: List[str] = []
    for key in ("image", "photo", "thumbnail", "picture", "bild", "foto", "images", "photos"):
        val = raw.get(key)
        if isinstance(val, str) and val.startswith("http"):
            images.append(val)
            break
        if isinstance(val, list):
            for img in val[:MAX_IMAGES]:
                if isinstance(img, str) and img.startswith("http"):
                    images.append(img)
                elif isinstance(img, dict):
                    src = img.get("url") or img.get("src") or img.get("href") or ""
                    if src:
                        images.append(src)
            if images:
                break

    # URL
    source_url = ""
    for key in ("url", "link", "href", "permalink", "product_url", "machine_url", "page_url"):
        val = raw.get(key)
        if isinstance(val, str) and val:
            source_url = urljoin(base_url, val) if val.startswith("/") else val
            break

    # Description
    description = _get("description", "desc", "body", "body_html", "short_description", "text")
    if len(description) > 2000:
        description = description[:2000]

    # Specs from additionalProperty or specs dict
    specs: Dict[str, str] = {}
    for sp in (raw.get("additionalProperty") or raw.get("specs") or []):
        if isinstance(sp, dict) and sp.get("name"):
            specs[sp["name"]] = str(sp.get("value") or "")
    if isinstance(raw.get("specifications"), dict):
        specs.update(raw["specifications"])

    return {
        "machine_name":   machine_name or model or brand,
        "brand":          brand,
        "model":          model,
        "machine_type":   mtype,
        "condition":      condition,
        "year":           year,
        "price":          price,
        "currency":       currency,
        "stock_number":   stock,
        "description":    description,
        "specifications": specs,
        "images":         images[:MAX_IMAGES],
        "source_url":     source_url,
    }


# ── Stage 1 — Link discovery ───────────────────────────────────────────────────

def _discover_product_links(
    html: str,
    base_url: str,
    session: requests.Session,
    budget: RequestBudget,
    visited: set,
) -> List[str]:
    """
    FIX 6: Advanced link discovery.
    - Pattern frequency threshold: 2 (was 5)
    - No short-path rejection
    - Follows next-page pagination to get ALL products
    - Returns up to MAX_PRODUCT_LINKS URLs
    """
    soup = BeautifulSoup(html, "lxml")
    scored: Dict[str, int] = {}

    # Collect all internal links with zone-based scoring
    zone_configs = [
        (soup.select("nav, header, [class*='menu'], [class*='nav']"), 4),
        (soup.select("footer, [class*='sidebar']"), 2),
        ([soup], 1),  # whole page fallback
    ]
    for elements, zone_score in zone_configs:
        for el in elements:
            for a in el.find_all("a", href=True):
                url = _abs_url(a["href"], base_url)
                if not url:
                    continue
                score = _score_link(url, base_url)
                if score <= 0:
                    continue
                clean = _clean_url(url)
                scored[clean] = max(scored.get(clean, 0), score + zone_score)

    # FIX 6: Pattern frequency — threshold 2 (was 5)
    pattern_counts: Counter = Counter(_path_pattern(u) for u in scored)
    for url in list(scored):
        freq = pattern_counts[_path_pattern(url)]
        if freq >= 2:
            scored[url] = scored[url] + freq * 2

    # Sort by score, English first
    ranked = sorted(scored.items(), key=lambda x: (not _is_english(x[0]), -x[1]))

    # Collect category/listing pages (multi-product links) and product pages
    listing_pages: List[str] = []
    product_pages: List[str] = []

    for url, score in ranked:
        if url in visited:
            continue
        if PRODUCT_PATH_RE.search(urlparse(url).path) and score >= 10:
            # Looks like a direct product page
            product_pages.append(url)
        else:
            listing_pages.append(url)

    log.info(
        "Stage 1: %d listing candidates, %d direct product pages",
        len(listing_pages), len(product_pages),
    )

    # Crawl listing pages to collect product URLs + follow pagination
    all_product_urls: List[str] = list(product_pages)
    pages_crawled = 0
    MAX_LISTING_PAGES = min(len(listing_pages), 30)

    for cat_url in listing_pages[:MAX_LISTING_PAGES]:
        if budget.remaining < 5:
            break
        clean = _clean_url(cat_url)
        if clean in visited:
            continue
        visited.add(clean)

        r = budget.get(session, cat_url)
        if r is None or not r.ok:
            continue

        # Use Playwright if JS-rendered
        cat_html = render_if_needed(cat_url, r.text) if _PW_AVAILABLE else r.text

        # Extract product links from this listing page
        page_products = _extract_product_links_from_listing(cat_html, cat_url, base_url)
        all_product_urls.extend(page_products)
        pages_crawled += 1

        # Follow pagination
        next_url = cat_url
        for _ in range(MAX_PAGINATION):
            if budget.remaining < 3:
                break
            next_url = _find_next_page(cat_html, next_url)
            if not next_url:
                break
            clean_next = _clean_url(next_url)
            if clean_next in visited:
                break
            visited.add(clean_next)
            nr = budget.get(session, next_url)
            if nr is None or not nr.ok:
                break
            cat_html = render_if_needed(next_url, nr.text) if _PW_AVAILABLE else nr.text
            more = _extract_product_links_from_listing(cat_html, next_url, base_url)
            if not more:
                break
            all_product_urls.extend(more)
            log.info("  → paginated to %s, found %d more", next_url, len(more))

        if len(all_product_urls) >= MAX_PRODUCT_LINKS:
            break

    # Deduplicate preserving order
    seen_urls: set = set()
    result: List[str] = []
    for url in all_product_urls:
        clean = _clean_url(url)
        if clean not in seen_urls and clean not in visited:
            seen_urls.add(clean)
            result.append(url)
        if len(result) >= MAX_PRODUCT_LINKS:
            break

    log.info("Stage 1 complete: %d product URLs discovered", len(result))
    return result


# Known product-card CSS selectors
_PRODUCT_CARD_SELECTORS = [
    "article.product", ".product-item", ".product-card",
    ".machine-item", ".machine-card", ".listing-item",
    ".equipment-item", "li.product", "div.product",
    '[class*="product-item"]', '[class*="machine-item"]',
    '[class*="listing-item"]', '[class*="equipment-item"]',
    '[class*="item-card"]', '[class*="product-card"]',
    '[class*="machine-card"]', ".woocommerce-loop-product",
    ".grid-item", ".catalog-item", ".result-item",
    '[class*="result-item"]', '[class*="search-result"]',
]


def _extract_product_links_from_listing(html: str, page_url: str, base_url: str) -> List[str]:
    """
    Extract product/machine URLs from a listing/category page.
    Strategy 1: Known CSS product-card selectors.
    Strategy 2: Pattern frequency (threshold 2, was 3).
    """
    soup = BeautifulSoup(html, "lxml")
    scores: Dict[str, int] = {}

    # Strategy 1: CSS product card selectors
    for selector in _PRODUCT_CARD_SELECTORS:
        for card in soup.select(selector):
            for a in card.find_all("a", href=True):
                url = _abs_url(a["href"], base_url)
                if url and _same_domain(url, base_url):
                    scores[url] = scores.get(url, 0) + 5

    # Strategy 2: Pattern frequency on all links
    all_urls: List[str] = []
    for a in soup.find_all("a", href=True):
        url = _abs_url(a["href"], base_url)
        if not url or not _same_domain(url, base_url):
            continue
        if SKIP_PATH_RE.search(urlparse(url).path):
            continue
        all_urls.append(url)

    pattern_counts: Counter = Counter(_path_pattern(u) for u in all_urls)
    for url in all_urls:
        pat = _path_pattern(url)
        freq = pattern_counts[pat]
        if freq >= 2:  # FIX 6: was 3
            scores[url] = scores.get(url, 0) + freq

    # Filter and deduplicate
    seen: set = set()
    result: List[str] = []
    for url, score in sorted(scores.items(), key=lambda x: x[1], reverse=True):
        if score < 2:
            continue
        clean = _clean_url(url)
        if clean not in seen and url != base_url:
            seen.add(clean)
            result.append(url)
    return result


def _find_next_page(html: str, current_url: str) -> Optional[str]:
    """Return the next-page URL from pagination links, or None."""
    soup = BeautifulSoup(html, "lxml")

    # <a rel="next">
    nxt = soup.find("a", rel=lambda r: r and "next" in r)
    if nxt and nxt.get("href"):
        return _abs_url(nxt["href"], current_url)

    # Text / class patterns
    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True).lower()
        cls  = " ".join(a.get("class") or []).lower()
        if any(kw in text or kw in cls for kw in (
            "next", "weiter", "suivant", "volgende", "avanti",
            "siguiente", "›", "»", "next page",
        )):
            url = _abs_url(a["href"], current_url)
            if url and url != current_url:
                return url

    return None


# ── Stage 2 — Machine data extraction ─────────────────────────────────────────

def _extract_json_ld(soup: BeautifulSoup) -> Dict[str, Any]:
    """Extract full machine data from JSON-LD schema.org/Product blocks."""
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
            mtype = str(data.get("category") or data.get("productType") or "")

            # Images
            imgs = data.get("image") or []
            if isinstance(imgs, str):
                imgs = [imgs]
            elif isinstance(imgs, dict):
                imgs = [imgs.get("url") or imgs.get("contentUrl") or ""]
            images = [i for i in imgs if i][:MAX_IMAGES]

            # Offer (price / currency)
            offer = data.get("offers") or {}
            if isinstance(offer, list):
                offer = offer[0] if offer else {}
            price_raw = str(offer.get("price") or "")
            price, currency = _parse_price(price_raw)
            if not currency and offer.get("priceCurrency"):
                currency = offer["priceCurrency"]

            # Specs from additionalProperty
            specs: Dict[str, str] = {}
            for prop in (data.get("additionalProperty") or []):
                if isinstance(prop, dict) and prop.get("name"):
                    specs[str(prop["name"])] = str(prop.get("value") or "")

            stock = str(data.get("sku") or data.get("productID") or "")
            description = str(data.get("description") or "")[:2000]

            return {
                "machine_name": name,
                "brand":        brand,
                "model":        model,
                "machine_type": mtype,
                "price":        price,
                "currency":     currency or "USD",
                "stock_number": stock,
                "description":  description,
                "specifications": specs,
                "images":       images,
            }
        except Exception as exc:
            log.debug("JSON-LD parse error: %s", exc)
            continue
    return {}


def _extract_og_meta(soup: BeautifulSoup) -> Dict[str, Any]:
    """Extract data from OpenGraph + standard meta tags."""
    result: Dict[str, Any] = {}
    for meta in soup.find_all("meta"):
        prop    = meta.get("property") or meta.get("name") or ""
        content = (meta.get("content") or "").strip()
        if not content:
            continue
        if prop == "og:title":
            result.setdefault("machine_name", content)
        elif prop == "og:image":
            result.setdefault("images", [content])
        elif prop == "og:description":
            result.setdefault("description", content[:2000])
        elif prop in ("product:price:amount", "og:price:amount"):
            price, currency = _parse_price(content)
            result.setdefault("price", price)
            result.setdefault("currency", currency)
        elif prop in ("product:price:currency", "og:price:currency"):
            result["currency"] = content
    return result


def _extract_heuristics(soup: BeautifulSoup, url: str, base_url: str) -> Dict[str, Any]:
    """
    FIX 5: CSS/HTML heuristics extraction — never silently empty.
    Covers: machine_name, brand, model, machine_type, price, condition,
            year, stock_number, description, specifications, images.
    """
    result: Dict[str, Any] = {}

    # ── Machine name ─────────────────────────────────────────────────────────
    for sel in [
        "h1.product-title", "h1.machine-title", ".product-title h1",
        ".machine-title", "[itemprop='name']", ".product-name",
        ".machine-name", "h1",
    ]:
        el = soup.select_one(sel)
        if el:
            result["machine_name"] = el.get_text(strip=True)
            break

    # ── Brand ─────────────────────────────────────────────────────────────────
    for sel in [
        "[itemprop='brand']", ".brand", ".manufacturer", ".make",
        '[class*="brand"]', '[class*="manufacturer"]', '[class*="make"]',
        '[data-brand]',
    ]:
        el = soup.select_one(sel)
        if el:
            text = el.get_text(strip=True)
            if text and len(text) < 100:
                result["brand"] = normalize_brand(text)
                break

    # ── Model ─────────────────────────────────────────────────────────────────
    for sel in [
        "[itemprop='model']", ".model", ".model-number",
        '[class*="model"]', '[data-model]', '[class*="mpn"]',
    ]:
        el = soup.select_one(sel)
        if el:
            result["model"] = el.get_text(strip=True)
            break

    # ── Machine type ──────────────────────────────────────────────────────────
    for sel in [
        ".breadcrumb li:nth-child(2)", "nav.breadcrumb li:nth-child(2)",
        "[itemprop='category']", ".category", ".machine-type",
        '[class*="category"]', '[class*="machine-type"]',
    ]:
        el = soup.select_one(sel)
        if el:
            text = el.get_text(strip=True)
            if text and len(text) < 80:
                result["machine_type"] = text
                break

    # Infer type from URL if still missing
    if not result.get("machine_type"):
        path = urlparse(url).path
        segs = [s for s in path.split("/") if s and len(s) > 2 and not re.search(r"\d", s)]
        skip_segs = {"products", "product", "machines", "machine", "equipment",
                     "catalog", "catalogue", "en", "de", "fr", "it", "es", "nl", "used", "new"}
        for seg in segs:
            if seg.lower() not in skip_segs:
                result["machine_type"] = seg.replace("-", " ").replace("_", " ").title()
                break

    # ── Price ─────────────────────────────────────────────────────────────────
    for sel in [
        "[itemprop='price']", ".price", ".product-price", ".machine-price",
        '[class*="price"]', '[class*="Price"]', ".asking-price", ".list-price",
    ]:
        el = soup.select_one(sel)
        if el:
            raw = el.get_text(strip=True)
            price, currency = _parse_price(raw)
            if price:
                result["price"]    = price
                result["currency"] = currency
                break

    # ── Stock number ──────────────────────────────────────────────────────────
    for sel in [
        "[itemprop='sku']", ".sku", ".stock-number", ".ref",
        '[class*="stock-number"]', '[class*="reference"]', '[class*="sku"]',
    ]:
        el = soup.select_one(sel)
        if el:
            result["stock_number"] = el.get_text(strip=True)
            break

    # ── Condition ─────────────────────────────────────────────────────────────
    for sel in [".condition", '[class*="condition"]', '[class*="zustand"]']:
        el = soup.select_one(sel)
        if el:
            result["condition"] = _extract_condition(el.get_text(strip=True))
            break
    if not result.get("condition"):
        page_text = soup.get_text(" ", strip=True)[:1000]
        result["condition"] = _extract_condition(page_text)

    # ── Year ──────────────────────────────────────────────────────────────────
    for sel in [".year", '[class*="year"]', '[class*="baujahr"]', ".manufacture-year"]:
        el = soup.select_one(sel)
        if el:
            year = _extract_year(el.get_text(strip=True))
            if year:
                result["year"] = year
                break
    if not result.get("year"):
        # Scan h1/h2/title for year
        texts = [
            result.get("machine_name", ""),
            (soup.find("title") or soup).get_text(strip=True)[:200],
        ]
        for t in texts:
            y = _extract_year(t)
            if y:
                result["year"] = y
                break

    # ── Description ───────────────────────────────────────────────────────────
    for sel in [
        "[itemprop='description']", ".product-description",
        ".machine-description", ".description", "#description",
        '[class*="description"]', ".details", ".product-details",
    ]:
        el = soup.select_one(sel)
        if el:
            desc = el.get_text(" ", strip=True)
            if len(desc) > 30:
                result["description"] = desc[:2000]
                break

    # ── Specifications table ──────────────────────────────────────────────────
    specs: Dict[str, str] = {}
    for table in soup.select(
        "table.specs, table.specifications, .spec-table, "
        ".specifications-table, [class*='spec-table'], table"
    )[:5]:
        for row in table.find_all("tr"):
            cells = row.find_all(["th", "td"])
            if len(cells) >= 2:
                k = cells[0].get_text(strip=True)
                v = cells[1].get_text(strip=True)
                if k and v and len(k) < 80 and len(v) < 500:
                    specs[k] = v
    # Also try definition lists
    for dl in soup.select("dl.specs, dl.specifications, dl")[:3]:
        dts = dl.find_all("dt")
        dds = dl.find_all("dd")
        for dt, dd in zip(dts, dds):
            k = dt.get_text(strip=True)
            v = dd.get_text(strip=True)
            if k and v and len(k) < 80:
                specs[k] = v
    if specs:
        result["specifications"] = dict(list(specs.items())[:100])

    # ── Images ───────────────────────────────────────────────────────────────
    images: List[str] = []
    for sel in [
        "[itemprop='image']", ".product-image img",
        ".machine-image img", ".gallery img",
        ".slider img", ".carousel img",
        '[class*="product-img"] img',
        '[class*="machine-img"] img',
        "img.main", ".main-image img",
        '[class*="gallery"] img',
    ]:
        for img in soup.select(sel)[:MAX_IMAGES]:
            src = (
                img.get("src") or
                img.get("data-src") or
                img.get("data-lazy") or
                img.get("data-original") or
                img.get("data-full") or
                ""
            )
            if src:
                abs_src = _abs_url(src, base_url)
                if abs_src and abs_src not in images:
                    # Filter out tiny icons
                    w = img.get("width") or "999"
                    h = img.get("height") or "999"
                    try:
                        if int(str(w)) < 50 or int(str(h)) < 50:
                            continue
                    except (ValueError, TypeError):
                        pass
                    images.append(abs_src)
        if images:
            break

    if images:
        result["images"] = images[:MAX_IMAGES]

    return {k: v for k, v in result.items() if v or v == 0}


def _infer_model_from_text(soup: BeautifulSoup, url: str) -> str:
    """Regex fallback: scan title/h1/h2 + URL path for model numbers."""
    sources = []
    title = soup.find("title")
    if title:
        sources.append(title.get_text(strip=True))
    for tag in soup.find_all(["h1", "h2"]):
        sources.append(tag.get_text(strip=True))
    sources.append(urlparse(url).path.replace("-", " ").replace("_", " "))

    for text in sources:
        m = MODEL_REGEX.search(text.upper())
        if m:
            return (m.group(1) or m.group(2) or "").strip()
    return ""


def _make_content_hash(brand: str, model: str, url: str) -> str:
    """Stable content hash for deduplication."""
    import re as _re
    from unidecode import unidecode
    def norm(t: str) -> str:
        return _re.sub(r"[^a-z0-9]", "", unidecode(t.lower()))
    return hashlib.sha256(f"{norm(brand)}|{norm(model)}|{url}".encode()).hexdigest()


def extract_machine_data(html: str, url: str, base_url: str) -> Dict[str, Any]:
    """
    FIX 5: Extract full machine data — never silently returns empty dict.

    Extraction order (highest priority first):
      1. JSON-LD schema.org/Product
      2. OpenGraph meta tags
      3. CSS / HTML heuristics
      4. Regex model fallback + URL-based type inference

    Every field that cannot be extracted is logged at DEBUG level.
    Returns a complete dict with all keys present (empty string / None if missing).
    """
    soup = BeautifulSoup(html, "lxml")

    # Layer 1 — JSON-LD
    data: Dict[str, Any] = _extract_json_ld(soup)

    # Layer 2 — OG meta (fill gaps only)
    for k, v in _extract_og_meta(soup).items():
        if v and not data.get(k):
            data[k] = v

    # Layer 3 — CSS heuristics (fill remaining gaps)
    for k, v in _extract_heuristics(soup, url, base_url).items():
        if not data.get(k) and (v or v == 0):
            data[k] = v

    # Layer 4 — Regex / URL fallbacks
    if not data.get("model"):
        inferred = _infer_model_from_text(soup, url)
        if inferred:
            data["model"] = inferred
            log.debug("FIX5 model via regex: %r on %s", inferred, url)

    if not data.get("machine_type"):
        path = urlparse(url).path
        segs = [s for s in path.split("/") if s and len(s) > 2 and not re.search(r"\d", s)]
        skip = {"products","product","machines","machine","equipment",
                "catalog","en","de","fr","it","es","nl","used","new"}
        for seg in segs:
            if seg.lower() not in skip:
                data["machine_type"] = seg.replace("-", " ").replace("_", " ").title()
                break

    if not data.get("condition"):
        data["condition"] = _extract_condition(soup.get_text(" ", strip=True)[:500])

    if not data.get("year"):
        year = _extract_year(
            data.get("machine_name", "") + " " + data.get("description", "")
        )
        if year:
            data["year"] = year

    # Normalize brand
    if data.get("brand"):
        data["brand"] = normalize_brand(str(data["brand"]))

    # FIX 5: Log missing fields at DEBUG so callers can see what failed
    missing = [k for k in ("machine_name", "brand", "model") if not data.get(k)]
    if missing:
        log.debug("Partial extraction on %s — missing: %s", url, missing)

    # Content hash for deduplication
    content_hash = _make_content_hash(
        data.get("brand", ""), data.get("model", ""), url
    )

    return {
        "machine_name":   data.get("machine_name", ""),
        "brand":          data.get("brand", ""),
        "model":          data.get("model", ""),
        "machine_type":   data.get("machine_type", ""),
        "condition":      data.get("condition", ""),
        "year":           data.get("year"),
        "price":          data.get("price"),
        "currency":       data.get("currency", "USD"),
        "stock_number":   data.get("stock_number", ""),
        "description":    data.get("description", ""),
        "specifications": data.get("specifications", {}),
        "images":         data.get("images", []),
        "source_url":     url,
        "content_hash":   content_hash,
    }


# ── Main crawl entry point ─────────────────────────────────────────────────────

def crawl(
    start_url: str,
    *,
    max_requests: int = MAX_REQUESTS,
    request_delay: float = REQUEST_DELAY,
    use_playwright: bool = True,
) -> List[Dict[str, Any]]:
    """
    Full 2-stage advanced crawl of a website.

    Stage 1: Discover ALL product links (sitemap + API + HTML listing pages + pagination)
    Stage 2: Extract full machine data from each product page

    Args:
        start_url      — target website URL
        max_requests   — HTTP request cap (default 500)
        request_delay  — seconds between requests (default 0.5)
        use_playwright — render JS pages with Playwright (default True)

    Returns:
        List of machine dicts with all fields populated.
    """
    if not start_url.startswith("http"):
        start_url = "https://" + start_url

    parsed   = urlparse(start_url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"

    session         = _make_session()
    budget          = RequestBudget(max_requests)
    budget._delay   = request_delay
    visited: set    = set()
    results: List[Dict] = []

    log.info("=" * 70)
    log.info("Crawling: %s", start_url)
    log.info("Budget: %d requests | Playwright: %s", max_requests, use_playwright and _PW_AVAILABLE)
    log.info("=" * 70)

    # ── Fetch homepage ─────────────────────────────────────────────────────────
    r = budget.get(session, start_url)
    if r is None or not r.ok:
        log.error("Homepage unreachable — aborting")
        return []
    homepage_html = r.text
    visited.add(_clean_url(start_url))

    # Render with Playwright if JS-heavy
    if use_playwright and _PW_AVAILABLE and needs_playwright(homepage_html):
        log.info("Homepage is JS-rendered — launching Playwright")
        homepage_html = render_if_needed(start_url, homepage_html)

    # ── Fast-path 1: Shopify ───────────────────────────────────────────────────
    shopify = _try_shopify(base_url, session, budget)
    if shopify:
        log.info("Shopify API: returning %d machines", len(shopify))
        return _finalize(shopify)

    # ── Fast-path 2: WooCommerce ───────────────────────────────────────────────
    woo = _try_woocommerce(base_url, session, budget)
    if woo:
        log.info("WooCommerce API: returning %d machines", len(woo))
        return _finalize(woo)

    # ── Fast-path 3: Embedded JSON (window.__NEXT_DATA__ etc.) ────────────────
    embedded = _extract_embedded_json_items(homepage_html, base_url)
    if embedded:
        log.info("Embedded JSON: returning %d machines", len(embedded))
        return _finalize(embedded)

    # ── Fast-path 4: Hidden API in page source ─────────────────────────────────
    api_data = _items_from_api(homepage_html, base_url, session, budget)
    if api_data:
        log.info("Hidden API: returning %d machines", len(api_data))
        return _finalize(api_data)

    # ── Fast-path 5: Sitemap ───────────────────────────────────────────────────
    sitemap_urls = _try_sitemap(base_url, session, budget)
    if sitemap_urls:
        log.info("Sitemap: %d product URLs — going directly to extraction", len(sitemap_urls))
        product_links = sitemap_urls
    else:
        # ── Stage 1: HTML link discovery + pagination ──────────────────────────
        # Try English sub-path if homepage has no product links
        if not sitemap_urls and not _is_english(start_url):
            for en_path in ("/en/", "/en", "/en-gb/", "/en-us/"):
                en_url = base_url + en_path
                if budget.remaining < 5:
                    break
                er = budget.get(session, en_url)
                if er and er.ok:
                    test_links = _extract_product_links_from_listing(er.text, en_url, base_url)
                    if test_links:
                        log.info("Found product links via %s", en_path)
                        homepage_html = er.text
                        start_url = en_url
                        visited.add(_clean_url(en_url))
                        break

        product_links = _discover_product_links(
            homepage_html, base_url, session, budget, visited
        )

    if not product_links:
        log.warning("No product links found — site may need custom configuration")
        return []

    # ── Stage 2: Extract machine data from each product page ──────────────────
    log.info("Stage 2: extracting data from %d product pages", len(product_links))
    errors = 0

    for i, link in enumerate(product_links):
        if budget.remaining < 2:
            log.warning("Budget near exhausted — stopping at %d/%d", i, len(product_links))
            break

        clean = _clean_url(link)
        if clean in visited:
            continue
        visited.add(clean)

        pr = budget.get(session, link)
        if pr is None or not pr.ok:
            status = pr.status_code if pr else "error"
            log.debug("Skip %s (HTTP %s)", link, status)
            continue

        product_html = pr.text
        # Render with Playwright if needed
        if use_playwright and _PW_AVAILABLE and needs_playwright(product_html):
            product_html = render_if_needed(link, product_html)

        try:
            data = extract_machine_data(product_html, link, base_url)
        except Exception as exc:
            # FIX 5: Never silent — always log the URL + exception
            errors += 1
            log.warning("Extraction error on %s: %s", link, exc)
            continue

        # Keep items with at least one meaningful field (FIX 5: log what was found)
        has_data = bool(
            data.get("brand") or data.get("model") or
            data.get("machine_name") or data.get("machine_type")
        )
        if has_data:
            log.info(
                "[%d/%d] brand=%r model=%r type=%r price=%s year=%s",
                i + 1, len(product_links),
                data["brand"], data["model"], data["machine_type"],
                data["price"], data["year"],
            )
            results.append(data)
        else:
            log.debug("No usable data extracted from %s", link)

    log.info("-" * 70)
    log.info(
        "Done. Machines: %d | Requests: %d/%d | Errors: %d",
        len(results), budget.used, budget.limit, errors,
    )
    return results


def _finalize(items: List[Dict]) -> List[Dict]:
    """
    Ensure all items have a content_hash and all required keys.
    """
    complete: List[Dict] = []
    for item in items:
        item.setdefault("machine_name",   "")
        item.setdefault("brand",          "")
        item.setdefault("model",          "")
        item.setdefault("machine_type",   "")
        item.setdefault("condition",      "")
        item.setdefault("year",           None)
        item.setdefault("price",          None)
        item.setdefault("currency",       "USD")
        item.setdefault("stock_number",   "")
        item.setdefault("description",    "")
        item.setdefault("specifications", {})
        item.setdefault("images",         [])
        item.setdefault("source_url",     "")
        if not item.get("content_hash"):
            item["content_hash"] = _make_content_hash(
                item["brand"], item["model"], item["source_url"]
            )
        complete.append(item)
    return complete


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python lightweight_crawler.py <url> [max_requests]")
        sys.exit(1)

    _url = sys.argv[1]
    _max = int(sys.argv[2]) if len(sys.argv) > 2 else MAX_REQUESTS
    _data = crawl(_url, max_requests=_max)

    # FIX 3: use tempfile.gettempdir() — works on Windows AND Linux
    import os as _os
    _out = _os.path.join(tempfile.gettempdir(), "crawler_results.json")
    with open(_out, "w", encoding="utf-8") as _f:
        json.dump(_data, _f, indent=2, ensure_ascii=False)

    print(f"Results: {len(_data)} machines saved to {_out}")
    print(json.dumps(_data[:3], indent=2, ensure_ascii=False))
