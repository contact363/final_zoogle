"""
Phase 1 — Detection Engine  (API-FIRST)

Detection order:
  1. Training rules (pre-configured API or category URLs)
  2. API detection  — Supabase / Shopify / WooCommerce / REST / HTML-source-scan / GraphQL
     → If API found: return immediately, skip all HTML scanning
  3. Embedded JSON  (window.__NEXT_DATA__, __NUXT__, __INITIAL_STATE__)
  4. Sitemap
  5. Category/nav scan  ]
  6. Common entry paths  } HTML — only when no API/JSON found
  7. Deep link scan      ]

JS-rendered sites (< 10 internal links on homepage) are detected early:
  → API detection is prioritised even harder
  → HTML fallbacks are skipped when site is JS-rendered

estimated_count is only set from REAL data (never fabricated).
Phase 2 (URL collection) sets the authoritative count.

GUARANTEE: run_discovery() always returns DiscoveryResult.
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from typing import List, Optional
from urllib.parse import urlparse, urlunparse

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

COMMON_ENTRY_PATHS = [
    "/products", "/machines", "/equipment", "/catalog", "/catalogue",
    "/shop", "/store", "/listings", "/inventory", "/used",
    "/maschinen", "/produkte", "/gebraucht",
    "/macchine", "/prodotti",
    "/maquinas", "/productos",
    "/machines", "/produits",
]


# ── Result ────────────────────────────────────────────────────────────────────

@dataclass
class DiscoveryResult:
    method: str
    category_urls: List[str] = field(default_factory=list)
    product_urls: List[str] = field(default_factory=list)
    # Real count only — 0 means "unknown, Phase 2 will find out"
    estimated_count: int = 0
    api_config: object = None
    notes: List[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return True

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


def _normalize(url: str) -> str:
    url = url.strip()
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    return url.rstrip("/")


def _clean(url: str) -> str:
    p = urlparse(url)
    return urlunparse(p._replace(query="", fragment=""))


def _same_domain(url: str, base: str) -> bool:
    try:
        return urlparse(url).netloc == urlparse(base).netloc
    except Exception:
        return False


# ── JS-rendered site detection ───────────────────────────────────────────────

_JS_FRAMEWORK_RE = re.compile(
    r'(?:__NEXT_DATA__|__NUXT__|__INITIAL_STATE__|ng-version|data-reactroot'
    r'|vue-app|_app\.js|_next/static|nuxt\.js)',
    re.IGNORECASE,
)


def _is_js_rendered(html: str) -> bool:
    """
    True if this page is likely a JS-rendered SPA:
      • Very few anchor links (< 10), OR
      • Contains known JS framework markers.
    An SPA will have no useful HTML links → must use API.
    """
    if not html:
        return False
    if _JS_FRAMEWORK_RE.search(html):
        return True
    # Count <a href> tags roughly (quick, no BeautifulSoup needed here)
    link_count = html.lower().count("<a href")
    return link_count < 10


# ── Main ──────────────────────────────────────────────────────────────────────

def run_discovery(
    website_id: int,
    website_url: str,
    training_rules: Optional[dict] = None,
) -> DiscoveryResult:
    """
    Detect how to crawl the website.  API-FIRST.

    If an API is found, it is returned immediately — HTML scanning is skipped.
    HTML fallbacks only run when no API or JSON source is detected.

    estimated_count is REAL (from sitemap/API/actual URL scans) or 0.
    Never fabricates a number. Phase 2 sets the authoritative count.
    """
    website_url = _normalize(website_url)
    notes: List[str] = []
    candidates: List[DiscoveryResult] = []

    logger.info("[Detection] START website_id=%d url=%s", website_id, website_url)

    def _add(r: Optional[DiscoveryResult]) -> None:
        if r and (r.api_config or r.product_urls or r.category_urls):
            candidates.append(r)
            logger.info("[Detection] candidate: method=%s count=%d", r.method, r.estimated_count)

    # ── Step 0: Training rules ─────────────────────────────────────────────
    if training_rules:
        try:
            r = _from_training_rules(training_rules, notes)
            if r:
                logger.info("[Detection] Training rules — returning immediately")
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
            logger.warning("[Detection] Homepage issue: %s for %s", status, website_url)
    except Exception as exc:
        notes.append(f"Homepage error: {exc}")

    js_rendered = _is_js_rendered(homepage_html)
    if js_rendered:
        notes.append("JS-rendered site detected — prioritising API detection")
        logger.info("[Detection] JS-rendered site — skipping HTML-first paths")

    # ── Step 1: API auto-detection (MANDATORY — runs first always) ─────────
    try:
        from crawler.extractors.api_extractor import detect_api
        api_result = detect_api(website_url, homepage_html)
        if api_result.found and api_result.config:
            notes.append(f"API detected: {api_result.config.api_type}")
            logger.info("[API] detected: type=%s endpoint=%s sample=%d",
                        api_result.config.api_type,
                        api_result.config.endpoint,
                        api_result.sample_count or 0)
            # API found → return immediately, skip all HTML scanning
            return DiscoveryResult(
                method="api",
                api_config=api_result.config,
                estimated_count=api_result.sample_count or 0,
                notes=notes,
            )
        else:
            notes.append("API: none detected")
    except Exception as exc:
        notes.append(f"API detection error: {exc}")
        logger.warning("[Detection] API error: %s", exc)

    # ── Step 2: Embedded JSON (script-tag data stores) ─────────────────────
    # Covers: window.__NEXT_DATA__, window.__NUXT__, window.__INITIAL_STATE__
    if homepage_html:
        try:
            r = _from_embedded_json(homepage_html, website_url, notes)
            _add(r)
        except Exception as exc:
            notes.append(f"Embedded JSON error: {exc}")

    # ── If JS-rendered and no API/JSON found: HTML scanning won't help ─────
    if js_rendered and not candidates:
        notes.append("JS-rendered + no API found — returning homepage as sole entry point")
        logger.info("[Detection] JS-rendered with no API for website_id=%d", website_id)
        return DiscoveryResult(
            method="html-fallback",
            category_urls=[website_url],
            estimated_count=0,
            notes=notes,
        )

    # ── Step 3: Sitemap ────────────────────────────────────────────────────
    try:
        from crawler.extractors.sitemap_extractor import fetch_product_urls
        sitemap_urls = fetch_product_urls(website_url)
        if sitemap_urls:
            notes.append(f"Sitemap: {len(sitemap_urls)} real product URLs")
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
        logger.warning("[Detection] sitemap error: %s", exc)

    # ── Step 4: Category/nav scan (HTML only) ──────────────────────────────
    if homepage_html:
        try:
            r = _from_category_scan(homepage_html, website_url, notes)
            _add(r)
        except Exception as exc:
            notes.append(f"Category scan error: {exc}")
            logger.warning("[Detection] category scan error: %s", exc)

    # ── Step 5: Common entry paths (HTML only) ─────────────────────────────
    try:
        r = _probe_common_paths(website_url, notes)
        _add(r)
    except Exception as exc:
        notes.append(f"Common paths error: {exc}")

    # ── Step 6: Deep link scan — only when nothing else found ──────────────
    if homepage_html and not candidates:
        try:
            r = _deep_link_scan(homepage_html, website_url, notes, max_depth=5)
            _add(r)
        except Exception as exc:
            notes.append(f"Deep scan error: {exc}")
            logger.debug("[Detection] deep scan error: %s", exc)

    # ── Pick best candidate ────────────────────────────────────────────────
    if candidates:
        def _priority(c: DiscoveryResult) -> tuple:
            return (
                bool(c.api_config),
                len(c.product_urls),
                c.estimated_count,
                len(c.category_urls),
            )
        best = max(candidates, key=_priority)
        best.notes = notes
        logger.info(
            "[Detection] BEST: method=%s real_count=%d category_urls=%d",
            best.method, best.estimated_count, len(best.category_urls),
        )
        return best

    # ── Fallback ───────────────────────────────────────────────────────────
    notes.append("No structure detected — using homepage as fallback entry point")
    logger.info("[Detection] FALLBACK for website_id=%d", website_id)
    return DiscoveryResult(
        method="html-fallback",
        category_urls=[website_url],
        estimated_count=0,
        notes=notes,
    )


# ── Helpers ───────────────────────────────────────────────────────────────────

def _from_training_rules(rules: dict, notes: List[str]) -> Optional[DiscoveryResult]:
    from crawler.extractors.api_extractor import build_config_from_rules
    if rules.get("crawl_type") == "api" or rules.get("api_url"):
        config = build_config_from_rules(rules)
        if config:
            notes.append(f"Pre-configured API: {config.api_type}")
            return DiscoveryResult(method="api", api_config=config, estimated_count=0)
    raw = rules.get("category_urls")
    if raw:
        urls = [u.strip() for u in raw.split("\n") if u.strip()] if isinstance(raw, str) else list(raw)
        if urls:
            notes.append(f"Pre-configured {len(urls)} category URLs")
            return DiscoveryResult(method="category", category_urls=urls, estimated_count=0)
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
_COUNT_KEYS = [
    "total", "totalCount", "total_count", "count", "totalItems",
    "total_items", "productCount", "machineCount", "num_found", "numFound",
]


def _from_embedded_json(html: str, website_url: str, notes: List[str]) -> Optional[DiscoveryResult]:
    for pattern in _EMBEDDED_PATTERNS:
        for m in re.finditer(pattern, html, re.DOTALL):
            raw = m.group(1)
            if len(raw) > 300_000:
                continue
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue
            count = _find_count(data, 0)
            if count and count > 5:
                notes.append(f"Embedded JSON real count: {count}")
                return DiscoveryResult(
                    method="embedded-json",
                    category_urls=[website_url],
                    estimated_count=count,   # real from page data
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


def _from_category_scan(html: str, website_url: str, notes: List[str]) -> Optional[DiscoveryResult]:
    from crawler.extractors.html_extractor import find_category_urls
    category_urls = find_category_urls(html, website_url)
    if not category_urls:
        notes.append("Category scan: no listing pages in nav")
        return None
    notes.append(f"Category scan: {len(category_urls)} entry points found")
    # estimated_count = 0 — real count comes from Phase 2
    return DiscoveryResult(
        method="category",
        category_urls=category_urls,
        estimated_count=0,
    )


def _probe_common_paths(website_url: str, notes: List[str]) -> Optional[DiscoveryResult]:
    from crawler.extractors.html_extractor import find_product_urls, count_internal_links
    listing_pages: List[str] = []
    real_product_urls: set = set()

    for path in COMMON_ENTRY_PATHS:
        url = website_url.rstrip("/") + path
        try:
            resp = _get(url, timeout=8)
            if not resp or resp.status_code != 200:
                continue
            if count_internal_links(resp.text, url) < 8:
                continue
            products = find_product_urls(resp.text, url)
            if len(products) >= 3:
                listing_pages.append(url)
                real_product_urls.update(products)
                notes.append(f"Common path: {path} → {len(products)} product URLs")
            if len(listing_pages) >= 5:
                break
        except Exception:
            continue

    if not listing_pages:
        notes.append("Common paths: no listing pages found")
        return None

    # estimated_count = real URLs found on sampled pages (partial — Phase 2 gets full count)
    return DiscoveryResult(
        method="common-paths",
        category_urls=listing_pages,
        estimated_count=len(real_product_urls),   # real sampled count
    )


def _deep_link_scan(
    html: str,
    website_url: str,
    notes: List[str],
    max_depth: int = 5,
) -> Optional[DiscoveryResult]:
    from crawler.extractors.html_extractor import (
        find_all_internal_links, find_product_urls, count_internal_links,
    )

    visited: set = {_clean(website_url)}
    queue: list = [(website_url, html, 1)]
    listing_pages: List[str] = []
    product_urls_found: set = set()
    requests_made = 0
    MAX_REQUESTS = 30

    while queue and requests_made < MAX_REQUESTS:
        current_url, current_html, depth = queue.pop(0)

        products = find_product_urls(current_html, current_url)
        link_count = count_internal_links(current_html, current_url)

        if len(products) >= 3 or link_count >= 20:
            if current_url != website_url:
                listing_pages.append(current_url)
            product_urls_found.update(products)

        if depth >= max_depth:
            continue

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
        notes.append(f"Deep scan (depth={max_depth}): nothing found")
        return None

    real_count = len(product_urls_found)
    notes.append(
        f"Deep scan: {len(listing_pages)} listing pages, "
        f"{real_count} real product URLs found"
    )
    return DiscoveryResult(
        method="deep-html",
        category_urls=listing_pages or [website_url],
        estimated_count=real_count,   # real URLs found during scan
    )
