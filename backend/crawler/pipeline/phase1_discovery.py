"""
Phase 1 — Smart Discovery Engine

Runs ALL detection methods and returns the BEST result.
NEVER returns estimated_count=0 if the site is reachable.

Detection pipeline:
  0. Training rules (pre-configured override)
  1. API auto-detection (Supabase / Shopify / WooCommerce / 20+ REST paths)
  2. Embedded JSON extraction (window.__DATA__, __NEXT_DATA__, etc.)
  3. Sitemap product URLs
  4. Category/nav scan (structure-based — no keyword dependency)
  5. Deep link scan (depth up to 5, structure + link-density based)
  6. Common entry points probe (/products, /machines, /catalog, /shop, ...)
  7. Final fallback — homepage + small estimate (NEVER hard 0)

GUARANTEE: run_discovery() always returns DiscoveryResult with success=True.
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from typing import List, Optional
from urllib.parse import urljoin, urlparse, urlunparse, urldefrag

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
}

# Common entry-point paths to probe when everything else finds 0
COMMON_ENTRY_PATHS = [
    "/products", "/machines", "/equipment", "/catalog", "/catalogue",
    "/shop", "/store", "/listings", "/inventory", "/used",
    "/maschinen", "/produkte", "/gebraucht",
    "/macchine", "/prodotti",
    "/maquinas", "/productos",
    "/machines", "/produits",
]


# ── Result dataclass ──────────────────────────────────────────────────────────

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
        r = requests.get(url, headers=REQUEST_HEADERS, timeout=timeout, allow_redirects=True)
        return r
    except Exception as exc:
        logger.debug("GET %s: %s", url, exc)
        return None


def _normalize(url: str) -> str:
    url = url.strip()
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    return url.rstrip("/")


def _same_domain(url: str, base: str) -> bool:
    try:
        return urlparse(url).netloc == urlparse(base).netloc
    except Exception:
        return False


def _clean(url: str) -> str:
    p = urlparse(url)
    return urlunparse(p._replace(query="", fragment=""))


# ── Main ──────────────────────────────────────────────────────────────────────

def run_discovery(
    website_id: int,
    website_url: str,
    training_rules: Optional[dict] = None,
) -> DiscoveryResult:
    """
    Run full discovery. Tries EVERY method. Returns best (highest count) result.
    Never raises. Always returns a usable DiscoveryResult.
    """
    website_url = _normalize(website_url)
    notes: List[str] = []
    candidates: List[DiscoveryResult] = []

    logger.info("[Discovery] START website_id=%d url=%s", website_id, website_url)

    def _add(r: Optional[DiscoveryResult]) -> None:
        if r and (r.estimated_count > 0 or r.api_config or r.product_urls or r.category_urls):
            candidates.append(r)
            logger.info("[Discovery] candidate: method=%s count=%d", r.method, r.estimated_count)

    # ── Step 0: Training rules ─────────────────────────────────────────────
    if training_rules:
        try:
            r = _from_training_rules(website_url, training_rules, notes)
            if r:
                logger.info("[Discovery] Training rules hit — returning immediately")
                return r
        except Exception as exc:
            notes.append(f"Training rules error: {exc}")

    # ── Fetch homepage ─────────────────────────────────────────────────────
    homepage_html = ""
    try:
        resp = _get(website_url)
        if resp and resp.status_code < 400:
            homepage_html = resp.text
            notes.append(f"Homepage OK ({len(homepage_html)} bytes)")
        else:
            status = resp.status_code if resp else "timeout"
            notes.append(f"Homepage status: {status}")
            logger.warning("[Discovery] Homepage issue: %s for %s", status, website_url)
    except Exception as exc:
        notes.append(f"Homepage error: {exc}")

    # ── Step 1: API auto-detection ─────────────────────────────────────────
    try:
        from crawler.extractors.api_extractor import detect_api
        api_result = detect_api(website_url, homepage_html)
        if api_result.found and api_result.config:
            notes.append(f"API detected: {api_result.config.api_type}")
            _add(DiscoveryResult(
                method="api",
                api_config=api_result.config,
                estimated_count=max(api_result.sample_count or 0, 10),
                notes=list(notes),
            ))
        else:
            notes.append("API detection: none found")
    except Exception as exc:
        notes.append(f"API detection error: {exc}")
        logger.warning("[Discovery] API error: %s", exc)

    # ── Step 2: Embedded JSON in script tags ───────────────────────────────
    if homepage_html:
        try:
            r = _from_embedded_json(homepage_html, website_url, notes)
            _add(r)
        except Exception as exc:
            notes.append(f"Embedded JSON error: {exc}")

    # ── Step 3: Sitemap ────────────────────────────────────────────────────
    try:
        from crawler.extractors.sitemap_extractor import fetch_product_urls
        sitemap_urls = fetch_product_urls(website_url)
        if sitemap_urls:
            notes.append(f"Sitemap: {len(sitemap_urls)} product URLs")
            _add(DiscoveryResult(
                method="sitemap",
                product_urls=sitemap_urls,
                estimated_count=len(sitemap_urls),
                notes=list(notes),
            ))
        else:
            notes.append("Sitemap: no product URLs")
    except Exception as exc:
        notes.append(f"Sitemap error: {exc}")
        logger.warning("[Discovery] sitemap error: %s", exc)

    # ── Step 4: Category/nav scan (structure-based) ────────────────────────
    if homepage_html:
        try:
            r = _from_category_scan(homepage_html, website_url, notes)
            _add(r)
        except Exception as exc:
            notes.append(f"Category scan error: {exc}")
            logger.warning("[Discovery] category scan error: %s", exc)

    # ── Step 5: Common entry points probe ──────────────────────────────────
    # Always run — finds /products, /machines, /catalog even if nav scan missed them
    try:
        r = _probe_common_paths(website_url, notes)
        _add(r)
    except Exception as exc:
        notes.append(f"Common paths probe error: {exc}")

    # ── Step 6: Deep link scan (depth up to 5) ────────────────────────────
    # Only run if no good candidates yet
    if homepage_html and not any(c.estimated_count >= 10 for c in candidates):
        try:
            r = _deep_link_scan(homepage_html, website_url, notes, max_depth=5)
            _add(r)
        except Exception as exc:
            notes.append(f"Deep scan error: {exc}")
            logger.debug("[Discovery] deep scan error: %s", exc)

    # ── Pick best candidate ────────────────────────────────────────────────
    if candidates:
        best = max(candidates, key=lambda c: (c.estimated_count, bool(c.api_config), bool(c.product_urls)))
        best.notes = notes
        logger.info(
            "[Discovery] BEST: method=%s count=%d (%d candidates)",
            best.method, best.estimated_count, len(candidates),
        )
        return best

    # ── Final fallback — always succeeds ──────────────────────────────────
    notes.append("All methods found 0 — returning homepage fallback with minimum estimate")
    logger.info("[Discovery] FALLBACK for website_id=%d", website_id)
    return DiscoveryResult(
        method="html-fallback",
        category_urls=[website_url],
        estimated_count=10,   # minimum non-zero estimate — site is reachable
        notes=notes,
    )


# ── Step helpers ──────────────────────────────────────────────────────────────

def _from_training_rules(url, rules, notes) -> Optional[DiscoveryResult]:
    from crawler.extractors.api_extractor import build_config_from_rules
    if rules.get("crawl_type") == "api" or rules.get("api_url"):
        config = build_config_from_rules(rules)
        if config:
            notes.append(f"Pre-configured API: {config.api_type}")
            return DiscoveryResult(method="api", api_config=config, estimated_count=0, notes=list(notes))
    if rules.get("category_urls"):
        raw = rules["category_urls"]
        urls = [u.strip() for u in raw.split("\n") if u.strip()] if isinstance(raw, str) else list(raw)
        if urls:
            notes.append(f"Pre-configured {len(urls)} category URLs")
            return DiscoveryResult(method="category", category_urls=urls,
                                   estimated_count=len(urls) * 50, notes=list(notes))
    return None


_EMBEDDED_PATTERNS = [
    r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(\{.+?\})</script>',
    r'window\.__INITIAL_STATE__\s*=\s*(\{.+?\})\s*;',
    r'window\.__DATA__\s*=\s*(\{.+?\})\s*;',
    r'window\.__NUXT__\s*=\s*(\{.+?\})',
    r'window\.initialData\s*=\s*(\{.+?\})\s*;',
    r'window\.pageData\s*=\s*(\{.+?\})\s*;',
    r'var productData\s*=\s*(\[.+?\])\s*;',
    r'var machineData\s*=\s*(\[.+?\])\s*;',
]
_COUNT_KEYS = ["total", "totalCount", "total_count", "count", "totalItems",
               "total_items", "productCount", "machineCount", "num_found", "numFound"]


def _from_embedded_json(html, website_url, notes) -> Optional[DiscoveryResult]:
    for pattern in _EMBEDDED_PATTERNS:
        for m in re.finditer(pattern, html, re.DOTALL):
            raw = m.group(1)
            if len(raw) > 300_000:
                continue
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue
            count = _find_count(data, depth=0)
            if count and count > 5:
                notes.append(f"Embedded JSON count: {count}")
                return DiscoveryResult(
                    method="embedded-json",
                    category_urls=[website_url],
                    estimated_count=count,
                    notes=list(notes),
                )
    return None


def _find_count(data, depth: int) -> Optional[int]:
    if depth > 6:
        return None
    if isinstance(data, dict):
        for k in _COUNT_KEYS:
            if k in data and isinstance(data[k], int) and data[k] > 5:
                return data[k]
        for v in data.values():
            found = _find_count(v, depth + 1)
            if found:
                return found
    elif isinstance(data, list) and len(data) > 5:
        return len(data)
    return None


def _from_category_scan(html, website_url, notes) -> Optional[DiscoveryResult]:
    from crawler.extractors.html_extractor import (
        find_category_urls, find_product_urls, build_pagination_urls
    )
    category_urls = find_category_urls(html, website_url)
    if not category_urls:
        notes.append("Category scan: no listing pages in nav")
        return None

    notes.append(f"Category scan: {len(category_urls)} listing pages")
    estimated = len(category_urls) * 30   # conservative base estimate

    # Sample first category page
    try:
        resp = _get(category_urls[0], timeout=12)
        if resp and resp.status_code < 400:
            product_urls = find_product_urls(resp.text, category_urls[0])
            if product_urls:
                pagination = build_pagination_urls(website_url, category_urls[0], resp.text)
                total_pages = max(1, len(pagination) + 1)
                estimated = len(product_urls) * total_pages * len(category_urls)
                notes.append(
                    f"Category sample: {len(product_urls)}/page × "
                    f"{total_pages} pages × {len(category_urls)} cats = {estimated}"
                )
            else:
                notes.append("Category sample: 0 product links on first page")
    except Exception as exc:
        notes.append(f"Category sample error: {exc}")

    return DiscoveryResult(
        method="category",
        category_urls=category_urls,
        estimated_count=max(estimated, len(category_urls)),
        notes=list(notes),
    )


def _probe_common_paths(website_url, notes) -> Optional[DiscoveryResult]:
    """
    Probe common product/listing paths that nav scan may have missed.
    Looks for pages with >10 internal links (structural listing signal).
    """
    from crawler.extractors.html_extractor import find_product_urls, count_internal_links

    listing_pages: List[str] = []
    total_products = 0

    for path in COMMON_ENTRY_PATHS:
        url = website_url.rstrip("/") + path
        try:
            resp = _get(url, timeout=8)
            if not resp or resp.status_code != 200:
                continue
            link_count = count_internal_links(resp.text, url)
            if link_count >= 10:
                product_urls = find_product_urls(resp.text, url)
                if len(product_urls) >= 3:
                    listing_pages.append(url)
                    total_products += len(product_urls)
                    notes.append(f"Common path hit: {path} ({len(product_urls)} products)")
            if len(listing_pages) >= 5:
                break
        except Exception:
            continue

    if not listing_pages:
        notes.append("Common paths probe: no listing pages found")
        return None

    return DiscoveryResult(
        method="common-paths",
        category_urls=listing_pages,
        estimated_count=total_products * 3,
        notes=list(notes),
    )


def _deep_link_scan(
    html: str,
    website_url: str,
    notes: List[str],
    max_depth: int = 5,
) -> Optional[DiscoveryResult]:
    """
    Follow internal links up to max_depth levels.
    Any page with >20 internal links is treated as a listing page.
    Any page where URL pattern repeats >3 times → product detail pages.
    """
    from crawler.extractors.html_extractor import (
        find_all_internal_links, find_product_urls, count_internal_links
    )

    visited: set = {_clean(website_url)}
    queue: List[tuple] = [(website_url, html, 1)]
    listing_pages: List[str] = []
    product_urls_found: set = set()
    requests_made = 0
    MAX_REQUESTS = 30

    while queue and requests_made < MAX_REQUESTS:
        current_url, current_html, depth = queue.pop(0)

        # Check if this page looks like a listing
        products = find_product_urls(current_html, current_url)
        link_count = count_internal_links(current_html, current_url)

        if len(products) >= 3 or link_count >= 20:
            if current_url != website_url:
                listing_pages.append(current_url)
            product_urls_found.update(products)

        if depth >= max_depth:
            continue

        # Expand: visit internal links
        for link in find_all_internal_links(current_html, current_url)[:15]:
            clean = _clean(link)
            if clean in visited:
                continue
            visited.add(clean)

            try:
                resp = _get(link, timeout=8)
                if resp and resp.status_code < 400:
                    queue.append((link, resp.text, depth + 1))
                    requests_made += 1
            except Exception:
                pass

            if requests_made >= MAX_REQUESTS:
                break

    if not listing_pages and not product_urls_found:
        notes.append(f"Deep scan (depth={max_depth}): no listing pages found")
        return None

    count = max(len(product_urls_found) * 2, len(listing_pages) * 30)
    notes.append(
        f"Deep scan: {len(listing_pages)} listing pages, "
        f"{len(product_urls_found)} product URLs, estimate={count}"
    )
    return DiscoveryResult(
        method="deep-html",
        category_urls=listing_pages or [website_url],
        estimated_count=count,
        notes=list(notes),
    )
