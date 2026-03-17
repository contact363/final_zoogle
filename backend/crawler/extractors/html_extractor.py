"""
HTML Extractor — Phase 1 discovery + Phase 3 machine data extraction.

Detection is STRUCTURE-BASED, not keyword-based.

Category detection uses:
  • URL pattern frequency (if a pattern repeats >5 times → listing page)
  • Link density (if a page has >20 internal links → treat as listing)
  • Common path segments (/product/, /item/, /listing/, etc.)

Machine extraction uses:
  • JSON-LD schema.org/Product
  • OpenGraph meta tags
  • CSS heuristics (title, price, images, specs table)
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
from collections import Counter
from typing import Any, Dict, List, Optional
from urllib.parse import (
    urljoin, urlparse, urlencode, parse_qs,
    urlunparse, urldefrag,
)

try:
    from bs4 import BeautifulSoup
except ImportError as _e:
    raise ImportError(
        "BeautifulSoup not installed — run: pip install beautifulsoup4 lxml"
    ) from _e

logger = logging.getLogger(__name__)

# ── Paths to always skip ──────────────────────────────────────────────────────
SKIP_PATH_RE = re.compile(
    r"/(cart|checkout|account|login|register|signin|signup|"
    r"password|reset|admin|api|wp-admin|wp-json|"
    r"contact|about|privacy|terms|faq|help|support|"
    r"blog|news|press|media|events|jobs|careers|sitemap|"
    r"feed|rss|tag|author|search)"
    r"|\.(pdf|jpg|jpeg|png|gif|svg|css|js|xml|zip|webp|ico)(\?|$)",
    re.IGNORECASE,
)

# Common URL segments that strongly indicate product/listing pages
PRODUCT_SEGMENT_RE = re.compile(
    r"/(product|machine|equipment|item|listing|catalog|catalogue|"
    r"inventory|shop|store|used|new|stock|"
    r"maschine|produkt|macchina|maquina|produit|apparaat)s?",
    re.IGNORECASE,
)


def _soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "lxml")


def _abs(url: str, base: str) -> Optional[str]:
    """Resolve URL to absolute. Return None for non-HTTP or skip-worthy URLs."""
    if not url:
        return None
    url = url.strip()
    if url.startswith(("#", "javascript:", "mailto:", "tel:", "data:")):
        return None
    try:
        abs_url, _ = urldefrag(urljoin(base, url))
        parsed = urlparse(abs_url)
        if parsed.scheme not in ("http", "https"):
            return None
        return abs_url
    except Exception:
        return None


def _same_domain(url: str, base: str) -> bool:
    try:
        return urlparse(url).netloc == urlparse(base).netloc
    except Exception:
        return False


def _clean_url(url: str) -> str:
    """Remove query params and fragments for pattern matching."""
    p = urlparse(url)
    return urlunparse(p._replace(query="", fragment=""))


def _path_pattern(url: str) -> str:
    """
    Convert a URL path to a pattern by replacing variable segments with {slug}.
    /machines/used-volvo-2022  →  /machines/{slug}
    /product/123               →  /product/{slug}
    """
    path = urlparse(url).path.rstrip("/")
    parts = path.split("/")
    patterned = []
    for part in parts:
        # Variable segment: contains digits, dashes, or is long
        if re.search(r"\d", part) or len(part) > 20 or "-" in part:
            patterned.append("{slug}")
        else:
            patterned.append(part)
    return "/".join(patterned)


# ── Category URL discovery ────────────────────────────────────────────────────

def find_category_urls(html: str, base_url: str) -> List[str]:
    """
    Find listing/category page URLs using STRUCTURE-BASED detection.

    Strategy:
    1. Collect all internal links.
    2. Group by URL path pattern.
    3. Any pattern repeated >5 times → listing page group.
    4. Also include links with product path segments.
    5. Links from nav/header/footer get priority.
    Returns deduplicated list sorted by score.
    """
    soup = _soup(html)
    all_links: Dict[str, int] = {}   # url → score

    # Collect ALL internal links with zone-based scoring
    zone_scores = [
        (soup.select("nav"), 4),
        (soup.select("header"), 3),
        (soup.select('[class*="menu"]'), 3),
        (soup.select('[class*="nav"]'), 3),
        (soup.select("footer"), 2),
        (soup.select('[class*="sidebar"]'), 2),
        ([soup], 1),   # whole page as fallback
    ]

    for elements, zone_score in zone_scores:
        for el in elements:
            for a in el.find_all("a", href=True):
                url = _abs(a["href"], base_url)
                if not url or not _same_domain(url, base_url):
                    continue
                if SKIP_PATH_RE.search(urlparse(url).path):
                    continue
                if url == base_url or url == base_url + "/":
                    continue
                score = all_links.get(url, 0)
                # Boost for product-related path segments
                if PRODUCT_SEGMENT_RE.search(urlparse(url).path):
                    score += 5
                score += zone_score
                all_links[url] = score

    # Pattern frequency analysis: find repeating path patterns
    pattern_counts: Counter = Counter()
    url_to_pattern: Dict[str, str] = {}
    for url in all_links:
        pat = _path_pattern(url)
        pattern_counts[pat] += 1
        url_to_pattern[url] = pat

    # FIX 6: Boost URLs whose pattern repeats — threshold lowered from 5 → 2
    for url, score in list(all_links.items()):
        pat = url_to_pattern.get(url, "")
        freq = pattern_counts.get(pat, 1)
        if freq >= 2:  # was 5
            all_links[url] = score + freq * 2

    # FIX 6: Filter: keep only URLs with score >= 2 (was 3)
    candidates = [(url, score) for url, score in all_links.items() if score >= 2]
    candidates.sort(key=lambda x: x[1], reverse=True)

    # Deduplicate preserving order
    seen: set = set()
    result: List[str] = []
    for url, _ in candidates[:40]:
        clean = _clean_url(url)
        if clean not in seen:
            seen.add(clean)
            result.append(url)

    return result


def find_all_internal_links(html: str, base_url: str) -> List[str]:
    """
    Return ALL unique internal links from a page, filtered of skip-worthy URLs.
    Used for deep scanning.
    """
    soup = _soup(html)
    seen: set = set()
    result: List[str] = []
    for a in soup.find_all("a", href=True):
        url = _abs(a["href"], base_url)
        if not url or not _same_domain(url, base_url):
            continue
        if SKIP_PATH_RE.search(urlparse(url).path):
            continue
        clean = _clean_url(url)
        if clean not in seen and clean != _clean_url(base_url):
            seen.add(clean)
            result.append(url)
    return result


def count_internal_links(html: str, base_url: str) -> int:
    """Count unique internal links on a page (used to identify listing pages)."""
    return len(find_all_internal_links(html, base_url))


# ── Product URL extraction from listing pages ─────────────────────────────────

# Known product card CSS selectors
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
    Extract product/machine URLs from a listing/category page.

    Uses two strategies:
    1. Known CSS product card selectors.
    2. URL path pattern frequency — if a pattern repeats >3 times on the
       same page, those URLs are product detail pages.
    """
    soup = _soup(html)
    scores: Dict[str, int] = {}

    custom_re = re.compile(product_link_pattern, re.IGNORECASE) if product_link_pattern else None

    # Strategy 1: CSS product card selectors
    for selector in PRODUCT_CARD_SELECTORS:
        for card in soup.select(selector):
            for a in card.find_all("a", href=True):
                url = _abs(a["href"], base_url)
                if url and _same_domain(url, base_url):
                    scores[url] = scores.get(url, 0) + 5

    # Strategy 2: Pattern frequency on all links
    all_urls: List[str] = []
    for a in soup.find_all("a", href=True):
        url = _abs(a["href"], base_url)
        if not url or not _same_domain(url, base_url):
            continue
        if SKIP_PATH_RE.search(urlparse(url).path):
            continue
        all_urls.append(url)

    pattern_counts: Counter = Counter(_path_pattern(u) for u in all_urls)
    for url in all_urls:
        pat = _path_pattern(url)
        freq = pattern_counts[pat]
        if custom_re and custom_re.search(url):
            scores[url] = scores.get(url, 0) + 10
        elif freq >= 2:  # FIX 6: was 3
            scores[url] = scores.get(url, 0) + freq

    # Filter and return
    seen: set = set()
    result: List[str] = []
    for url, score in sorted(scores.items(), key=lambda x: x[1], reverse=True):
        if score < 2:  # FIX 6: was 3
            continue
        clean = _clean_url(url)
        if clean not in seen and url != base_url:
            seen.add(clean)
            result.append(url)

    return result


# ── Pagination detection ──────────────────────────────────────────────────────

def find_next_page_url(html: str, current_url: str) -> Optional[str]:
    """Return the URL of the next page, or None if last page."""
    soup = _soup(html)

    # <a rel="next">
    nxt = soup.find("a", rel=lambda r: r and "next" in r)
    if nxt and nxt.get("href"):
        return _abs(nxt["href"], current_url)

    # Text/class patterns
    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True).lower()
        cls  = " ".join(a.get("class") or []).lower()
        if any(kw in text or kw in cls for kw in (
            "next", "weiter", "suivant", "volgende", "avanti", "siguiente", "›", "»"
        )):
            url = _abs(a["href"], current_url)
            if url and url != current_url:
                return url

    return None


def build_pagination_urls(base_url: str, current_url: str, html: str, max_pages: int = 200) -> List[str]:
    """Generate all pagination URLs for a listing page."""
    soup = _soup(html)
    urls: List[str] = []
    max_page = 1

    for a in soup.find_all("a", href=True):
        href = a["href"]
        m = re.search(r"[?&]page=(\d+)", href)
        if m:
            max_page = max(max_page, int(m.group(1)))
        m = re.search(r"/page/(\d+)", href)
        if m:
            max_page = max(max_page, int(m.group(1)))

    if max_page <= 1:
        return urls

    max_page = min(max_page, max_pages)
    parsed = urlparse(current_url)
    qs = parse_qs(parsed.query)

    for page_num in range(2, max_page + 1):
        if "page" in qs:
            new_qs = {**qs, "page": [str(page_num)]}
            new_parsed = parsed._replace(query=urlencode(new_qs, doseq=True))
            urls.append(urlunparse(new_parsed))
        elif re.search(r"/page/\d+", current_url):
            urls.append(re.sub(r"/page/\d+", f"/page/{page_num}", current_url))
        else:
            sep = "&" if "?" in current_url else "?"
            urls.append(f"{current_url}{sep}page={page_num}")

    return urls


# ── Machine data extraction ───────────────────────────────────────────────────

def extract_machine_data(html: str, url: str) -> Dict[str, Any]:
    """
    Extract structured machine data from a product detail page.
    Tries JSON-LD → OG meta → CSS heuristics in order.
    """
    result: Dict[str, Any] = {"source_url": url}
    soup = _soup(html)

    ld = _extract_json_ld(soup)
    if ld:
        result.update(ld)

    og = _extract_og_meta(soup)
    for k, v in og.items():
        if k not in result or not result[k]:
            result[k] = v

    css = _extract_css_heuristics(soup, url)
    for k, v in css.items():
        if k not in result or not result[k]:
            result[k] = v

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
            if "Product" not in str(data.get("@type", "")):
                continue
            result["machine_name"] = data.get("name")
            result["description"]  = data.get("description")
            result["brand"]        = (data.get("brand") or {}).get("name") if isinstance(data.get("brand"), dict) else data.get("brand")
            result["model"]        = data.get("model") or data.get("mpn")
            result["stock_number"] = data.get("sku") or data.get("productID")
            offer = data.get("offers") or {}
            if isinstance(offer, list):
                offer = offer[0] if offer else {}
            result["price"]    = offer.get("price")
            result["currency"] = offer.get("priceCurrency", "USD")
            imgs = data.get("image") or []
            if isinstance(imgs, str):
                imgs = [imgs]
            elif isinstance(imgs, dict):
                imgs = [imgs.get("url") or imgs.get("contentUrl")]
            result["images"] = [i for i in imgs if i]
            specs: Dict[str, str] = {}
            for prop in data.get("additionalProperty") or []:
                if isinstance(prop, dict):
                    specs[prop.get("name", "")] = str(prop.get("value", ""))
            if specs:
                result["specifications"] = specs
            if result.get("machine_name"):
                break
        except Exception:
            continue
    return {k: v for k, v in result.items() if v}


def _extract_og_meta(soup: BeautifulSoup) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for meta in soup.find_all("meta"):
        prop    = meta.get("property") or meta.get("name") or ""
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

    for sel in ["h1.product-title", "h1.machine-title", ".product-title h1",
                ".machine-title", "[itemprop='name']", "h1"]:
        el = soup.select_one(sel)
        if el:
            result["machine_name"] = el.get_text(strip=True)
            break

    for sel in ["[itemprop='price']", ".price", ".product-price",
                ".machine-price", '[class*="price"]']:
        el = soup.select_one(sel)
        if el:
            raw = el.get_text(strip=True)
            m = re.search(r"[\d,\.]+", raw.replace(" ", ""))
            if m:
                try:
                    result["price"] = float(m.group().replace(",", "."))
                except ValueError:
                    pass
            break

    for sel in ["[itemprop='description']", ".product-description",
                ".machine-description", ".description", "#description"]:
        el = soup.select_one(sel)
        if el:
            result["description"] = el.get_text(" ", strip=True)[:2000]
            break

    images: List[str] = []
    for sel in ["[itemprop='image']", ".product-image img", ".machine-image img",
                ".gallery img", ".slider img", '[class*="product-img"]']:
        for img in soup.select(sel)[:5]:
            src = img.get("src") or img.get("data-src") or img.get("data-lazy")
            if src:
                abs_src = _abs(src, base_url)
                if abs_src and abs_src not in images:
                    images.append(abs_src)
    if images:
        result["images"] = images[:5]

    specs: Dict[str, str] = {}
    for table in soup.select("table.specs, table.specifications, .spec-table, table")[:3]:
        for row in table.find_all("tr"):
            cells = row.find_all(["th", "td"])
            if len(cells) >= 2:
                k = cells[0].get_text(strip=True)
                v = cells[1].get_text(strip=True)
                if k and v and len(k) < 80:
                    specs[k] = v
    if specs:
        result["specifications"] = specs

    for sel in ["[itemprop='brand']", ".brand", ".manufacturer", ".make"]:
        el = soup.select_one(sel)
        if el:
            result["brand"] = el.get_text(strip=True)
            break

    return {k: v for k, v in result.items() if v}
