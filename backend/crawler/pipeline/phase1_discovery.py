"""
Phase 1 — Smart Discovery Engine

Runs ALL detection methods and returns the BEST result.
Never stops at estimated_count=0 without trying everything.

Detection order (all run, best wins):
  1. Pre-configured training rules (API URL or category list)
  2. API auto-detection  (Supabase / Shopify / WooCommerce / REST)
  3. Embedded JSON in script tags (window.__DATA__, __NEXT_DATA__, etc.)
  4. Sitemap product URLs
  5. Category/nav scan  (with real count sample from first page)
  6. Deep internal link scan (depth-2 from homepage)
  7. Final fallback  (homepage as start URL — always returns success)

GUARANTEE: run_discovery() ALWAYS returns DiscoveryResult with success=True.
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from typing import List, Optional
from urllib.parse import urljoin, urlparse

import requests

logger = logging.getLogger(__name__)

DISCOVERY_TIMEOUT = 15
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/json,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
}

# Product-related path fragments for link scanning
PRODUCT_PATH_RE = re.compile(
    r"/(machine|product|equipment|item|listing|catalog|inventory|used|"
    r"maschine|produkt|macchina|maquina|produit|apparaat)s?/",
    re.IGNORECASE,
)


# ── Result ────────────────────────────────────────────────────────────────────

@dataclass
class DiscoveryResult:
    method: str
    category_urls: List[str] = field(default_factory=list)
    product_urls: List[str] = field(default_factory=list)
    estimated_count: int = 0
    api_config: object = None
    notes: List[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return True   # always

    @property
    def has_direct_urls(self) -> bool:
        return bool(self.product_urls) or self.api_config is not None


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def _get(url: str, timeout: int = DISCOVERY_TIMEOUT) -> Optional[requests.Response]:
    try:
        return requests.get(url, headers=REQUEST_HEADERS, timeout=timeout, allow_redirects=True)
    except Exception as exc:
        logger.debug("GET %s: %s", url, exc)
        return None


def _normalize_url(url: str) -> str:
    url = url.strip()
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    return url.rstrip("/")


def _same_domain(url: str, base: str) -> bool:
    return urlparse(url).netloc == urlparse(base).netloc


# ── Main discovery ────────────────────────────────────────────────────────────

def run_discovery(
    website_id: int,
    website_url: str,
    training_rules: Optional[dict] = None,
) -> DiscoveryResult:
    """
    Run ALL detection methods. Return the best (highest estimated_count) result.
    Never raises. Always returns a usable DiscoveryResult.
    """
    website_url = _normalize_url(website_url)
    notes: List[str] = []
    candidates: List[DiscoveryResult] = []

    logger.info("[Discovery] website_id=%d url=%s", website_id, website_url)

    def _add(result: Optional[DiscoveryResult]) -> None:
        if result:
            candidates.append(result)
            logger.info(
                "[Discovery] candidate: method=%s count=%d",
                result.method, result.estimated_count,
            )

    # ── Step 0: Training rules ─────────────────────────────────────────────
    if training_rules:
        try:
            r = _from_training_rules(website_url, training_rules, notes)
            if r:
                # Training rules take absolute priority
                logger.info("[Discovery] Training rules hit — returning immediately")
                return r
        except Exception as exc:
            notes.append(f"Training rules error: {exc}")
            logger.warning("[Discovery] training rules error: %s", exc)

    # ── Fetch homepage ─────────────────────────────────────────────────────
    homepage_html = ""
    try:
        resp = _get(website_url)
        if resp and resp.status_code < 400:
            homepage_html = resp.text
            notes.append(f"Homepage fetched ({len(homepage_html)} bytes, status {resp.status_code})")
        else:
            status = resp.status_code if resp else "timeout"
            notes.append(f"Homepage returned {status}")
            logger.warning("[Discovery] homepage issue: %s for %s", status, website_url)
    except Exception as exc:
        notes.append(f"Homepage fetch error: {exc}")
        logger.warning("[Discovery] homepage exception: %s", exc)

    # ── Step 1: API auto-detection ─────────────────────────────────────────
    try:
        from crawler.extractors.api_extractor import detect_api
        api_result = detect_api(website_url, homepage_html)
        if api_result.found and api_result.config:
            _add(DiscoveryResult(
                method="api",
                api_config=api_result.config,
                estimated_count=api_result.sample_count or 50,
                notes=notes,
            ))
            notes.append(f"API detected: {api_result.config.api_type}")
        else:
            notes.append("API detection: no API found")
    except Exception as exc:
        notes.append(f"API detection error: {exc}")
        logger.warning("[Discovery] API detection error: %s", exc)

    # ── Step 2: Embedded JSON in script tags ───────────────────────────────
    if homepage_html:
        try:
            r = _from_embedded_json(homepage_html, website_url, notes)
            _add(r)
        except Exception as exc:
            notes.append(f"Embedded JSON error: {exc}")
            logger.debug("[Discovery] embedded JSON error: %s", exc)

    # ── Step 3: Sitemap ────────────────────────────────────────────────────
    try:
        from crawler.extractors.sitemap_extractor import fetch_product_urls
        sitemap_urls = fetch_product_urls(website_url)
        if sitemap_urls:
            _add(DiscoveryResult(
                method="sitemap",
                product_urls=sitemap_urls,
                estimated_count=len(sitemap_urls),
                notes=notes,
            ))
            notes.append(f"Sitemap: {len(sitemap_urls)} product URLs")
        else:
            notes.append("Sitemap: no product URLs")
    except Exception as exc:
        notes.append(f"Sitemap error: {exc}")
        logger.warning("[Discovery] sitemap error: %s", exc)

    # ── Step 4: Category/nav scan ──────────────────────────────────────────
    if homepage_html:
        try:
            r = _from_category_scan(homepage_html, website_url, notes)
            _add(r)
        except Exception as exc:
            notes.append(f"Category scan error: {exc}")
            logger.warning("[Discovery] category scan error: %s", exc)

    # ── Step 5: Deep internal link scan (depth-2) ──────────────────────────
    if homepage_html and not any(c.estimated_count > 0 for c in candidates):
        try:
            r = _deep_link_scan(homepage_html, website_url, notes)
            _add(r)
        except Exception as exc:
            notes.append(f"Deep scan error: {exc}")
            logger.debug("[Discovery] deep scan error: %s", exc)

    # ── Pick best candidate ────────────────────────────────────────────────
    if candidates:
        best = max(candidates, key=lambda c: c.estimated_count)
        best.notes = notes
        logger.info(
            "[Discovery] Best: method=%s count=%d (from %d candidates)",
            best.method, best.estimated_count, len(candidates),
        )
        return best

    # ── Step 6: Final fallback — always succeeds ───────────────────────────
    notes.append("All methods found 0 — using homepage fallback")
    logger.info("[Discovery] fallback to homepage for website_id=%d", website_id)
    return DiscoveryResult(
        method="html",
        category_urls=[website_url],
        estimated_count=0,
        notes=notes,
    )


# ── Training rules helper ─────────────────────────────────────────────────────

def _from_training_rules(
    website_url: str,
    rules: dict,
    notes: List[str],
) -> Optional[DiscoveryResult]:
    from crawler.extractors.api_extractor import build_config_from_rules

    crawl_type = rules.get("crawl_type", "auto")

    if crawl_type == "api" or rules.get("api_url"):
        config = build_config_from_rules(rules)
        if config:
            notes.append(f"Pre-configured API: {config.api_type}")
            return DiscoveryResult(method="api", api_config=config, estimated_count=0, notes=notes)

    if rules.get("category_urls"):
        raw = rules["category_urls"]
        urls = [u.strip() for u in raw.split("\n") if u.strip()] if isinstance(raw, str) else list(raw)
        if urls:
            notes.append(f"Pre-configured {len(urls)} category URLs")
            return DiscoveryResult(
                method="category",
                category_urls=urls,
                estimated_count=len(urls) * 50,
                notes=notes,
            )

    return None


# ── Embedded JSON extraction ──────────────────────────────────────────────────

# Common patterns where sites embed product data in page scripts
_EMBEDDED_JSON_PATTERNS = [
    r'window\.__INITIAL_STATE__\s*=\s*(\{.+?\});',
    r'window\.__DATA__\s*=\s*(\{.+?\});',
    r'window\.__NUXT__\s*=\s*(\{.+?\})',
    r'<script id="__NEXT_DATA__"[^>]*>(\{.+?\})</script>',
    r'window\.initialData\s*=\s*(\{.+?\});',
    r'window\.pageData\s*=\s*(\{.+?\});',
    r'var productData\s*=\s*(\[.+?\]);',
    r'var machineData\s*=\s*(\[.+?\]);',
]

_PRODUCT_COUNT_KEYS = [
    "total", "totalCount", "total_count", "count", "totalItems",
    "total_items", "productCount", "machineCount", "num_found",
]


def _from_embedded_json(
    html: str,
    website_url: str,
    notes: List[str],
) -> Optional[DiscoveryResult]:
    """Extract product count from embedded page JSON (Next.js, Nuxt, custom)."""
    for pattern in _EMBEDDED_JSON_PATTERNS:
        for m in re.finditer(pattern, html, re.DOTALL):
            raw = m.group(1)
            if len(raw) > 500_000:
                continue  # skip huge blobs
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue

            count = _find_count_in_json(data)
            if count and count > 0:
                notes.append(f"Embedded JSON count: {count}")
                logger.info("[Discovery] Embedded JSON found count=%d", count)
                return DiscoveryResult(
                    method="embedded-json",
                    category_urls=[website_url],
                    estimated_count=count,
                    notes=notes,
                )
    return None


def _find_count_in_json(data, depth: int = 0) -> Optional[int]:
    """Recursively search for a product count value in a nested JSON structure."""
    if depth > 5:
        return None
    if isinstance(data, dict):
        for key in _PRODUCT_COUNT_KEYS:
            if key in data and isinstance(data[key], int) and data[key] > 5:
                return data[key]
        for val in data.values():
            found = _find_count_in_json(val, depth + 1)
            if found:
                return found
    elif isinstance(data, list) and len(data) > 5:
        return len(data)
    return None


# ── Category scan with real count sample ─────────────────────────────────────

def _from_category_scan(
    html: str,
    website_url: str,
    notes: List[str],
) -> Optional[DiscoveryResult]:
    from crawler.extractors.html_extractor import find_category_urls, find_product_urls

    category_urls = find_category_urls(html, website_url)
    if not category_urls:
        notes.append("Category scan: no listing pages found")
        return None

    notes.append(f"Category scan: {len(category_urls)} listing pages")

    # Sample first category page to get a real product count
    sample_count = 0
    try:
        resp = _get(category_urls[0], timeout=12)
        if resp and resp.status_code < 400:
            product_urls = find_product_urls(resp.text, category_urls[0])
            sample_count = len(product_urls)

            # Try to find total page count from pagination
            from crawler.extractors.html_extractor import build_pagination_urls
            pagination = build_pagination_urls(website_url, category_urls[0], resp.text, max_pages=500)
            total_pages = max(1, len(pagination) + 1)
            # Estimate: products per page × total pages × category count
            estimated = sample_count * total_pages * len(category_urls)
            notes.append(
                f"Category sample: {sample_count} products/page × "
                f"{total_pages} pages × {len(category_urls)} cats = {estimated}"
            )
        else:
            # Use rough estimate
            estimated = len(category_urls) * 50
            notes.append(f"Category sample failed, using estimate: {estimated}")
    except Exception as exc:
        estimated = len(category_urls) * 50
        notes.append(f"Category sample error ({exc}), estimate: {estimated}")

    return DiscoveryResult(
        method="category",
        category_urls=category_urls,
        estimated_count=max(estimated, len(category_urls)),
        notes=notes,
    )


# ── Deep internal link scan (depth-2) ────────────────────────────────────────

def _deep_link_scan(
    html: str,
    website_url: str,
    notes: List[str],
) -> Optional[DiscoveryResult]:
    """
    Follow internal links from homepage up to depth=2.
    Look for pages with repeated product card patterns.
    """
    from bs4 import BeautifulSoup
    from crawler.extractors.html_extractor import find_product_urls

    soup = BeautifulSoup(html, "lxml")
    seen_urls = {website_url}
    product_urls_found: set = set()
    listing_pages: List[str] = []

    # Collect all internal links from homepage
    depth1_links: List[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        url = urljoin(website_url, href)
        if _same_domain(url, website_url) and url not in seen_urls:
            seen_urls.add(url)
            depth1_links.append(url)

    # Visit first 20 internal links (avoid excessive requests)
    for url in depth1_links[:20]:
        try:
            resp = _get(url, timeout=8)
            if not resp or resp.status_code >= 400:
                continue

            products = find_product_urls(resp.text, url)
            if len(products) >= 3:
                listing_pages.append(url)
                product_urls_found.update(products)

            if len(listing_pages) >= 5:
                break
        except Exception:
            continue

    if not listing_pages:
        notes.append("Deep scan: no listing pages found at depth-2")
        return None

    notes.append(
        f"Deep scan: {len(listing_pages)} listing pages, "
        f"{len(product_urls_found)} product URLs found"
    )
    return DiscoveryResult(
        method="deep-html",
        category_urls=listing_pages,
        estimated_count=len(product_urls_found) * 3,  # rough: only sampled some pages
        notes=notes,
    )
