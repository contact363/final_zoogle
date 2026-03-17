"""
HTML Extractor — used by Phase 1 (discovery) and Phase 3 (machine detail).

Responsibilities:
  • Find category/listing page URLs from site navigation
  • Detect product card patterns on listing pages
  • Extract machine data from product detail pages
  • Detect and build pagination URLs
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse, urlencode, parse_qs, urlunparse

try:
    from bs4 import BeautifulSoup
except ImportError as _bs4_err:
    raise ImportError(
        "BeautifulSoup not installed — HTML parsing unavailable. "
        "Run: pip install beautifulsoup4 lxml"
    ) from _bs4_err

logger = logging.getLogger(__name__)

# ── Multilingual product keywords ────────────────────────────────────────────
PRODUCT_KEYWORDS = re.compile(
    r"\b(machine|machinery|product|equipment|tool|device|catalog|catalogue|"
    r"listing|inventory|stock|used|new|"
    # German
    r"maschine|produkt|werkzeug|geraet|ausruestung|gebraucht|"
    # Italian
    r"macchina|prodotto|attrezzatura|usato|"
    # Spanish
    r"maquina|producto|equipo|usado|"
    # French
    r"machine|produit|equipement|occasion|"
    # Dutch
    r"machine|product|apparaat|gebruikt)\b",
    re.IGNORECASE,
)

# Category/listing path patterns
CATEGORY_PATH_RE = re.compile(
    r"/(machines?|products?|equipment|catalog|catalogue|inventory|listings?|"
    r"used|new|stock|shop|store|category|categor[yi]|"
    r"maschinen|produkte|werkzeuge|gebraucht|"
    r"macchine|prodotti|usato|"
    r"maquinas|productos|"
    r"machines|produits|"
    r"machines|producten)/?",
    re.IGNORECASE,
)

# URLs to always skip
SKIP_PATH_RE = re.compile(
    r"/(cart|checkout|account|login|register|signin|signup|"
    r"password|reset|admin|api|wp-admin|wp-json|"
    r"contact|about|privacy|terms|faq|help|support|"
    r"blog|news|press|media|events|jobs|careers|"
    r"\.(pdf|jpg|jpeg|png|gif|svg|css|js|xml|zip))\b",
    re.IGNORECASE,
)

# Product detail page URL signals
PRODUCT_DETAIL_RE = re.compile(
    r"/(product|machine|equipment|item|detail|listing|used|gebraucht|"
    r"macchina|maquina|produit|produkt|apparaat)s?/[^/]+/?$",
    re.IGNORECASE,
)


def _soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "lxml")


def _abs(url: str, base: str) -> Optional[str]:
    """Resolve relative URL against base. Returns None for non-HTTP URLs."""
    if not url or url.startswith(("#", "javascript:", "mailto:", "tel:")):
        return None
    abs_url = urljoin(base, url.strip())
    parsed = urlparse(abs_url)
    if parsed.scheme not in ("http", "https"):
        return None
    return abs_url


def _same_domain(url: str, base: str) -> bool:
    return urlparse(url).netloc == urlparse(base).netloc


# ── Category URL discovery ────────────────────────────────────────────────────

def find_category_urls(html: str, base_url: str) -> List[str]:
    """
    Scan nav, header, footer, and sidebar for category/listing page links.
    Returns deduplicated list of same-domain category URLs.
    """
    soup = _soup(html)
    candidates: Dict[str, int] = {}  # url → score

    # Priority zones: nav > header > footer > body
    zones = [
        ("nav", 3), ("header", 2), ("footer", 1),
        ('[class*="menu"]', 2), ('[class*="nav"]', 2),
        ('[class*="sidebar"]', 1), ("body", 0),
    ]

    for selector, boost in zones:
        for container in soup.select(selector)[:3]:
            for a in container.find_all("a", href=True):
                url = _abs(a["href"], base_url)
                if not url or not _same_domain(url, base_url):
                    continue
                path = urlparse(url).path
                if SKIP_PATH_RE.search(path):
                    continue
                text = (a.get_text(" ", strip=True) + " " + path).lower()
                score = boost
                if CATEGORY_PATH_RE.search(path):
                    score += 5
                if PRODUCT_KEYWORDS.search(text):
                    score += 2
                if score > 0:
                    candidates[url] = max(candidates.get(url, 0), score)

    # Return top URLs sorted by score
    ranked = sorted(candidates.items(), key=lambda x: x[1], reverse=True)
    return [url for url, _ in ranked if _[:1] or True][:30]


# ── Product URL extraction from listing pages ─────────────────────────────────

# Common CSS selectors for product cards
PRODUCT_CARD_SELECTORS = [
    "article.product", ".product-item", ".product-card",
    ".machine-item", ".machine-card", ".listing-item",
    ".equipment-item", "li.product", "div.product",
    '[class*="product-item"]', '[class*="machine-item"]',
    '[class*="listing-item"]', '[class*="equipment-item"]',
    '[class*="item-card"]', '[class*="product-card"]',
    '[class*="machine-card"]', ".woocommerce-loop-product",
    ".grid-item", ".catalog-item",
]


def find_product_urls(html: str, base_url: str, product_link_pattern: str = "") -> List[str]:
    """
    Extract product/machine detail URLs from a listing/category page.
    Uses CSS pattern matching + URL structure heuristics.
    """
    soup = _soup(html)
    urls: Dict[str, int] = {}  # url → score

    # If custom pattern provided, use it
    custom_re = re.compile(product_link_pattern, re.IGNORECASE) if product_link_pattern else None

    # Strategy 1: known product card selectors
    for selector in PRODUCT_CARD_SELECTORS:
        for card in soup.select(selector):
            for a in card.find_all("a", href=True):
                url = _abs(a["href"], base_url)
                if url and _same_domain(url, base_url):
                    urls[url] = urls.get(url, 0) + 3

    # Strategy 2: links that match product URL pattern
    for a in soup.find_all("a", href=True):
        url = _abs(a["href"], base_url)
        if not url or not _same_domain(url, base_url):
            continue
        if SKIP_PATH_RE.search(urlparse(url).path):
            continue
        if custom_re and custom_re.search(url):
            urls[url] = urls.get(url, 0) + 5
            continue
        if PRODUCT_DETAIL_RE.search(url):
            urls[url] = urls.get(url, 0) + 2

    # Filter out listing/category pages themselves
    result = []
    for url, score in urls.items():
        if score >= 2 and url != base_url:
            path = urlparse(url).path
            # Skip if it looks like a category page
            if not re.search(r"/page/\d+", path):
                result.append(url)

    return list(dict.fromkeys(result))  # preserve order, deduplicate


# ── Pagination detection ──────────────────────────────────────────────────────

def find_next_page_url(html: str, current_url: str) -> Optional[str]:
    """Return the URL of the next page, or None if this is the last page."""
    soup = _soup(html)

    # 1. <a rel="next">
    next_link = soup.find("a", rel=lambda r: r and "next" in r)
    if next_link and next_link.get("href"):
        return _abs(next_link["href"], current_url)

    # 2. Common "next page" button text/class patterns
    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True).lower()
        cls  = " ".join(a.get("class") or []).lower()
        if any(kw in text or kw in cls for kw in ("next", "weiter", "suivant", "volgende", "avanti", "siguiente")):
            url = _abs(a["href"], current_url)
            if url and url != current_url:
                return url

    return None


def build_pagination_urls(base_url: str, current_url: str, html: str, max_pages: int = 200) -> List[str]:
    """
    Generate all pagination URLs for a listing page.
    Detects ?page=N, /page/N/, ?offset=N, ?start=N patterns.
    """
    soup = _soup(html)
    urls: List[str] = []

    # Find highest page number visible in pagination
    max_page = 1
    for a in soup.find_all("a", href=True):
        href = a["href"]
        # ?page=N
        m = re.search(r"[?&]page=(\d+)", href)
        if m:
            max_page = max(max_page, int(m.group(1)))
        # /page/N/
        m = re.search(r"/page/(\d+)", href)
        if m:
            max_page = max(max_page, int(m.group(1)))

    if max_page <= 1:
        return urls

    max_page = min(max_page, max_pages)

    # Detect URL pattern from current_url
    parsed = urlparse(current_url)
    qs = parse_qs(parsed.query)

    for page_num in range(2, max_page + 1):
        if "page" in qs:
            new_qs = {**qs, "page": [str(page_num)]}
            new_parsed = parsed._replace(query=urlencode(new_qs, doseq=True))
            urls.append(urlunparse(new_parsed))
        elif re.search(r"/page/\d+", current_url):
            new_url = re.sub(r"/page/\d+", f"/page/{page_num}", current_url)
            urls.append(new_url)
        else:
            # Append ?page=N
            sep = "&" if "?" in current_url else "?"
            urls.append(f"{current_url}{sep}page={page_num}")

    return urls


# ── Machine data extraction from product detail pages ─────────────────────────

def extract_machine_data(html: str, url: str) -> Dict[str, Any]:
    """
    Extract structured machine data from a product detail page.

    Tries in order:
    1. JSON-LD (schema.org/Product)
    2. OpenGraph meta tags
    3. CSS heuristic patterns
    """
    result: Dict[str, Any] = {"source_url": url}

    # 1. JSON-LD
    soup = _soup(html)
    ld_data = _extract_json_ld(soup)
    if ld_data:
        result.update(ld_data)

    # 2. OG meta
    og_data = _extract_og_meta(soup)
    for key, val in og_data.items():
        if key not in result or not result[key]:
            result[key] = val

    # 3. CSS heuristics (fill any remaining gaps)
    css_data = _extract_css_heuristics(soup, url)
    for key, val in css_data.items():
        if key not in result or not result[key]:
            result[key] = val

    # Compute content hash
    brand = str(result.get("brand") or "")
    model = str(result.get("model") or "")
    result["content_hash"] = hashlib.sha256(
        f"{brand}|{model}|{url}".encode()
    ).hexdigest()

    return result


def _extract_json_ld(soup: BeautifulSoup) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string or "{}")
            if isinstance(data, list):
                data = next((d for d in data if "Product" in str(d.get("@type", ""))), {})
            schema_type = str(data.get("@type", ""))
            if "Product" not in schema_type and "Offer" not in schema_type:
                continue

            result["machine_name"] = data.get("name") or data.get("title")
            result["description"]  = data.get("description")
            result["brand"]        = (data.get("brand") or {}).get("name") if isinstance(data.get("brand"), dict) else data.get("brand")
            result["model"]        = data.get("model") or data.get("mpn")
            result["stock_number"] = data.get("sku") or data.get("productID")

            # Price
            offer = data.get("offers") or {}
            if isinstance(offer, list):
                offer = offer[0] if offer else {}
            result["price"]    = offer.get("price")
            result["currency"] = offer.get("priceCurrency", "USD")

            # Images
            imgs = data.get("image") or []
            if isinstance(imgs, str):
                imgs = [imgs]
            elif isinstance(imgs, dict):
                imgs = [imgs.get("url") or imgs.get("contentUrl")]
            result["images"] = [i for i in imgs if i]

            # Specs
            specs: Dict[str, str] = {}
            for prop in data.get("additionalProperty") or []:
                if isinstance(prop, dict):
                    specs[prop.get("name", "")] = str(prop.get("value", ""))
            if specs:
                result["specifications"] = specs

            if result.get("machine_name"):
                break
        except (json.JSONDecodeError, AttributeError):
            continue
    return {k: v for k, v in result.items() if v}


def _extract_og_meta(soup: BeautifulSoup) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for meta in soup.find_all("meta"):
        prop = meta.get("property") or meta.get("name") or ""
        content = meta.get("content") or ""
        if prop == "og:title" and not result.get("machine_name"):
            result["machine_name"] = content
        elif prop == "og:description" and not result.get("description"):
            result["description"] = content
        elif prop == "og:image" and not result.get("images"):
            result["images"] = [content]
    return result


def _extract_css_heuristics(soup: BeautifulSoup, base_url: str) -> Dict[str, Any]:
    result: Dict[str, Any] = {}

    # Title / machine name
    for sel in [
        "h1.product-title", "h1.machine-title", "h1.product-name",
        ".product-title h1", ".machine-title", "[itemprop='name']",
        "h1",
    ]:
        el = soup.select_one(sel)
        if el:
            result["machine_name"] = el.get_text(strip=True)
            break

    # Price
    for sel in [
        "[itemprop='price']", ".price", ".product-price",
        ".machine-price", '[class*="price"]',
    ]:
        el = soup.select_one(sel)
        if el:
            raw_price = el.get_text(strip=True)
            m = re.search(r"[\d,\.]+", raw_price.replace(",", ""))
            if m:
                try:
                    result["price"] = float(m.group().replace(",", "."))
                except ValueError:
                    pass
            break

    # Description
    for sel in [
        "[itemprop='description']", ".product-description",
        ".machine-description", ".description", "#description",
    ]:
        el = soup.select_one(sel)
        if el:
            result["description"] = el.get_text(" ", strip=True)[:2000]
            break

    # Images
    images: List[str] = []
    for sel in [
        "[itemprop='image']", ".product-image img",
        ".machine-image img", ".gallery img", ".slider img",
        '[class*="product-img"]', '[class*="main-image"]',
    ]:
        for img in soup.select(sel)[:5]:
            src = img.get("src") or img.get("data-src") or img.get("data-lazy")
            if src:
                abs_src = _abs(src, base_url)
                if abs_src and abs_src not in images:
                    images.append(abs_src)
    if images:
        result["images"] = images[:5]

    # Specs table
    specs: Dict[str, str] = {}
    for table in soup.select("table.specs, table.specifications, .spec-table, .attributes-table, table")[:3]:
        for row in table.find_all("tr"):
            cells = row.find_all(["th", "td"])
            if len(cells) >= 2:
                key   = cells[0].get_text(strip=True)
                value = cells[1].get_text(strip=True)
                if key and value and len(key) < 80:
                    specs[key] = value
    if specs:
        result["specifications"] = specs

    # Brand from meta or common selectors
    for sel in ["[itemprop='brand']", ".brand", ".manufacturer", ".make"]:
        el = soup.select_one(sel)
        if el:
            result["brand"] = el.get_text(strip=True)
            break

    return {k: v for k, v in result.items() if v}
