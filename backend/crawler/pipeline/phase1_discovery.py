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
    True ONLY if the page is a CLIENT-SIDE-RENDERED SPA shell.

    KEY DISTINCTION:
      SSR sites (Next.js SSR, Nuxt SSR, Angular Universal) — fully rendered
      server-side — their HTML contains 30-200+ links and ALL product content.
      They happen to have _next/static, __nuxt etc. in the HTML, but that
      does NOT mean they need JS to render. BeautifulSoup can parse them fine.

      CSR sites (Create React App, Vue CLI, plain SPA) — send a minimal HTML
      shell with just <div id="root"></div> and load everything via JS.
      These genuinely need Playwright.

    RULE: use LINK COUNT as the primary discriminator.
      >= 15 links in initial HTML → content is already rendered → NOT a SPA
      <  15 links                 → likely a blank SPA shell → IS JS-rendered
    """
    if not html:
        return True
    # Link count is the reliable signal — SSR pages have many links, SPAs have few
    link_count = html.lower().count("<a href")
    if link_count >= 15:
        return False   # fully rendered (SSR), even if it uses Next.js/Nuxt/Vue
    # Very few links AND looks like a blank SPA shell
    return True


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

    # ── If JS-rendered and no API/JSON found: multi-strategy recovery ────────
    if js_rendered and not candidates:
        notes.append("JS-rendered + no API — running multi-strategy recovery")
        logger.info("[Detection] JS-rendered, no API — recovery for website_id=%d", website_id)

        # Strategy 0: Next.js RSC — request server component payload directly (no browser)
        rsc_result = _try_nextjs_rsc_pages(website_url, homepage_html, notes)
        if rsc_result:
            _add(rsc_result)

        # Strategy A: Playwright with XHR interception (captures API calls live)
        if not candidates:
            xhr_result = _try_playwright_intercept(website_url, notes)
            if xhr_result:
                _add(xhr_result)

        # Strategy B: Playwright HTML render + category scan
        if not candidates:
            rendered_html = _render_with_playwright(website_url, notes)
            if rendered_html:
                try:
                    r = _from_embedded_json(rendered_html, website_url, notes)
                    _add(r)
                except Exception:
                    pass
                if not candidates:
                    try:
                        r = _from_category_scan(rendered_html, website_url, notes)
                        _add(r)
                    except Exception:
                        pass

        # Strategy C: Brute-force API probe — try 50+ common endpoints without needing HTML
        if not candidates:
            r = _brute_force_api_probe(website_url, notes)
            _add(r)

        # Strategy D: Playwright probes common paths
        if not candidates:
            try:
                r = _probe_common_paths_playwright(website_url, notes)
                _add(r)
            except Exception as exc:
                notes.append(f"Playwright common paths error: {exc}")

        # All strategies exhausted → route to lightweight_crawler which handles everything
        if not candidates:
            notes.append("All recovery strategies exhausted — routing to lightweight_crawler")
            logger.info("[Detection] playwright-fallback for website_id=%d", website_id)
            return DiscoveryResult(
                method="playwright-fallback",
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


# ── Playwright helpers ────────────────────────────────────────────────────────

def _try_playwright_intercept(website_url: str, notes: List[str]) -> Optional[DiscoveryResult]:
    """
    Use Playwright XHR interception to capture live API calls during page load.
    This works even when the HTML is completely empty (React/Vue root div only).
    """
    try:
        from crawler.playwright_renderer import render_and_intercept
        from crawler.extractors.html_extractor import find_category_urls, find_product_urls

        # Visit homepage + common product pages to maximise XHR capture
        extra = ["/products", "/machines", "/equipment", "/inventory",
                 "/catalog", "/shop", "/listings", "/used"]
        html, api_responses = render_and_intercept(
            website_url, timeout_ms=30_000, extra_paths=extra
        )

        product_urls: List[str] = []
        category_urls: List[str] = []

        # Walk captured API responses for product data
        for resp in api_responses:
            data = resp["data"]
            # Check if it's a product list
            if isinstance(data, list) and len(data) >= 2:
                keys = set()
                for item in data[:3]:
                    if isinstance(item, dict):
                        keys.update(k.lower() for k in item.keys())
                product_keys = {"brand","model","price","name","title","sku","mpn",
                                "machine_name","stock_number","manufacturer","url","href"}
                if keys & product_keys:
                    # Extract URLs from items
                    for item in data:
                        if not isinstance(item, dict):
                            continue
                        for key in ("url","link","href","permalink","product_url"):
                            v = item.get(key)
                            if isinstance(v, str) and v.startswith("http"):
                                product_urls.append(v)
                                break
                    if product_urls:
                        notes.append(
                            f"Playwright XHR: found {len(data)} items in {resp['url']}"
                        )
                        break
            # Check wrapped list
            elif isinstance(data, dict):
                for key in ("data","results","items","products","machines",
                            "listings","equipment","records","hits","entries"):
                    val = data.get(key)
                    if isinstance(val, list) and len(val) >= 2:
                        for item in val:
                            if not isinstance(item, dict):
                                continue
                            for ukey in ("url","link","href","permalink","product_url"):
                                v = item.get(ukey)
                                if isinstance(v, str) and v.startswith("http"):
                                    product_urls.append(v)
                                    break
                        if product_urls:
                            notes.append(
                                f"Playwright XHR: found {len(val)} items "
                                f"(key={key}) in {resp['url']}"
                            )
                            break

        # HTML category scan on rendered page
        if html and not product_urls:
            category_urls = find_category_urls(html, website_url)
            if not category_urls:
                product_urls = find_product_urls(html, website_url)

        if product_urls:
            notes.append(f"Playwright XHR result: {len(product_urls)} product URLs")
            return DiscoveryResult(
                method="playwright-xhr",
                product_urls=list(dict.fromkeys(product_urls))[:2000],
                estimated_count=len(product_urls),
            )
        if category_urls:
            notes.append(f"Playwright XHR result: {len(category_urls)} category URLs")
            return DiscoveryResult(
                method="playwright-category",
                category_urls=category_urls,
                estimated_count=0,
            )
        notes.append(
            f"Playwright XHR: {len(api_responses)} responses captured "
            f"but no product data found"
        )

    except ImportError:
        notes.append("Playwright: not installed — skipping XHR interception")
    except Exception as exc:
        notes.append(f"Playwright XHR error: {exc}")
        logger.warning("[Detection] Playwright XHR error: %s", exc)

    return None


# Known API paths to probe on any site (REST, Shopify, WooCommerce, etc.)
_BRUTE_FORCE_API_PATHS = [
    "/products.json", "/products.json?limit=250",
    "/wp-json/wc/v3/products", "/wp-json/wc/v3/products?per_page=100",
    "/api/products", "/api/v1/products", "/api/v2/products",
    "/api/machines", "/api/equipment", "/api/inventory",
    "/api/listings", "/api/catalog", "/api/items",
    "/api/v1/machines", "/api/v2/machines",
    "/api/products/list", "/api/machine/list",
    "/rest/v1/products", "/rest/products",
    "/graphql",  # will POST introspection
    "/.netlify/functions/products",
    "/api/search?q=&type=product",
    "/search/suggest.json?q=machine&resources[type]=product",
    "/collections.json",
    "/collections/all/products.json",
    "/en/api/products", "/en/api/machines",
    "/api/public/products", "/api/public/machines",
    "/api/catalog/products", "/api/catalog/machines",
    "/api/stock", "/api/stock/list",
    "/api/vehicles", "/api/fleet",
    "/json/products", "/json/machines",
    "/data/products.json", "/data/machines.json",
    "/feeds/products.json",
]


def _brute_force_api_probe(website_url: str, notes: List[str]) -> Optional[DiscoveryResult]:
    """
    Try every known API path without needing to see it in page source.
    Works for sites that don't expose their API URLs in HTML.
    """
    product_urls: List[str] = []
    found_endpoint = ""

    for path in _BRUTE_FORCE_API_PATHS:
        url = website_url.rstrip("/") + path
        try:
            r = _get(url, timeout=8)
            if not r or r.status_code != 200:
                continue
            ct = r.headers.get("content-type", "")
            if "json" not in ct:
                continue
            try:
                data = r.json()
            except Exception:
                continue

            # Flatten to list
            items = None
            if isinstance(data, list) and data:
                items = data
            elif isinstance(data, dict):
                for k in ("data","results","items","products","machines",
                          "listings","equipment","records","hits","entries"):
                    v = data.get(k)
                    if isinstance(v, list) and v:
                        items = v
                        break

            if not items or not isinstance(items[0], dict):
                continue

            # Extract URLs
            for item in items:
                for ukey in ("url","link","href","permalink","product_url","machine_url"):
                    v = item.get(ukey)
                    if isinstance(v, str) and v.startswith("/"):
                        v = website_url.rstrip("/") + v
                    if isinstance(v, str) and v.startswith("http"):
                        product_urls.append(v)
                        break

            if product_urls:
                found_endpoint = path
                notes.append(
                    f"Brute-force API: {path} → {len(items)} items, "
                    f"{len(product_urls)} URLs"
                )
                break
            elif items:
                # Found data but no URL field — return as category entry point
                notes.append(f"Brute-force API: {path} → {len(items)} items (no URL field)")
                return DiscoveryResult(
                    method="api-brute",
                    category_urls=[website_url],
                    estimated_count=len(items),
                )
        except Exception:
            continue

    if product_urls:
        return DiscoveryResult(
            method="api-brute",
            product_urls=list(dict.fromkeys(product_urls))[:2000],
            estimated_count=len(product_urls),
        )

    notes.append(f"Brute-force API: none of {len(_BRUTE_FORCE_API_PATHS)} paths returned data")
    return None


def _render_with_playwright(url: str, notes: List[str]) -> str:
    """
    Render a URL with Playwright and return fully-resolved HTML.
    Returns empty string if Playwright is not installed or rendering fails.
    """
    try:
        from crawler.playwright_renderer import render_page, get_last_error
        html = render_page(url, timeout_ms=30_000, scroll=True, retries=1)
        if html:
            notes.append(f"Playwright: rendered {len(html)} bytes from {url}")
            return html
        err = get_last_error()
        if err:
            notes.append(f"Playwright: render returned empty — {err}")
            if "executable" in err.lower() or "doesn't exist" in err.lower():
                notes.append("Playwright FIX: run  playwright install chromium  on server")
        else:
            notes.append("Playwright: render returned empty")
    except ImportError:
        notes.append("Playwright: not installed (pip install playwright && playwright install chromium)")
    except Exception as exc:
        notes.append(f"Playwright render error: {exc}")
    return ""


# ── Next.js RSC (React Server Components) extraction ─────────────────────────
# Next.js App Router sites respond to `RSC: 1` header with a streaming JSON
# payload of server component data — no browser needed.
# Format: self.__next_f.push([...]) lines; hrefs embedded as "href":"/path"

_NEXTJS_SIGNALS = re.compile(
    r'(?:_next/static|__NEXT_DATA__|next/dist|/_next/|"__nextjs)',
    re.IGNORECASE,
)

_RSC_PATHS = [
    "/",
    "/products", "/machines", "/equipment", "/inventory",
    "/catalog", "/catalogue", "/shop", "/listings", "/used",
    "/usedmachines", "/usedmachinestocklist", "/stock", "/stocklist",
    "/for-sale", "/sale", "/all-machines", "/all-equipment",
    "/maschinen", "/produkte", "/macchine", "/maquinas",
]

_RSC_HEADERS = {
    "User-Agent": REQUEST_HEADERS["User-Agent"],
    "Accept": "text/x-component",
    "Accept-Language": "en-US,en;q=0.9",
    "RSC": "1",
    "Next-Router-Prefetch": "1",
    "Next-Url": "/",
}

_RSC_HREF_RE = re.compile(r'"href"\s*:\s*"(/[^"]{3,})"')
_RSC_URL_RE  = re.compile(r'"(?:url|permalink|machine_url|product_url)"\s*:\s*"(https?://[^"]{8,})"')
_RSC_SLUG_RE = re.compile(r'"slug"\s*:\s*"([a-z0-9][a-z0-9\-]{3,})"')


def _try_nextjs_rsc_pages(
    website_url: str,
    homepage_html: str,
    notes: List[str],
) -> Optional[DiscoveryResult]:
    """
    For Next.js App Router sites: request pages with RSC:1 header to get
    server component data without needing a browser.

    This is the fix for sites like corelmachine.com where:
     - HTML has lots of links (SSR-like) but product pages load data via JS
     - Playwright fails (Chromium not installed) or captures 0 XHR
     - RSC payload contains all product hrefs as server-rendered JSON fragments
    """
    # Only attempt if site looks like Next.js
    if not homepage_html or not _NEXTJS_SIGNALS.search(homepage_html):
        return None

    notes.append("Next.js detected — trying RSC payload extraction")
    logger.info("[Detection] Next.js RSC probe for %s", website_url)

    base = website_url.rstrip("/")
    product_urls: List[str] = []
    category_urls: List[str] = []
    paths_tried = 0

    for path in _RSC_PATHS:
        url = base + path
        try:
            r = requests.get(
                url,
                headers={**_RSC_HEADERS, "Next-Url": path},
                timeout=12,
                allow_redirects=True,
            )
            if r.status_code not in (200, 304):
                continue

            ct = r.headers.get("content-type", "")
            # RSC response may be text/x-component or text/html with RSC payload
            if "html" in ct and "x-component" not in ct:
                # Not an RSC response on this path — try next
                continue

            text = r.text
            paths_tried += 1

            # Extract product hrefs from RSC stream
            hrefs = _RSC_HREF_RE.findall(text)
            full_urls = _RSC_URL_RE.findall(text)

            for href in hrefs:
                # Filter to likely product/machine detail pages
                # (short slugs or contain common patterns)
                lh = href.lower()
                if any(seg in lh for seg in (
                    "/product", "/machine", "/equipment", "/item",
                    "/detail", "/listing", "/stock", "/used",
                    "/inventory", "/maschine", "/macchine", "/maquina",
                )):
                    product_urls.append(base + href)
                elif href.count("/") == 1 and len(href) > 5:
                    # Root-level slug — could be a product or category
                    category_urls.append(base + href)

            for full_url in full_urls:
                if _same_domain(full_url, base):
                    product_urls.append(full_url)

            if product_urls or full_urls:
                notes.append(
                    f"RSC {path}: {len(hrefs)} hrefs, "
                    f"{len(product_urls)} product URLs extracted"
                )
                break

        except Exception as exc:
            logger.debug("[RSC] %s error: %s", url, exc)
            continue

    if not product_urls and not category_urls:
        if paths_tried > 0:
            notes.append(f"RSC: {paths_tried} paths returned RSC but no product hrefs found")
        else:
            notes.append("RSC: site has Next.js markers but did not respond to RSC:1 header")
        return None

    product_urls = list(dict.fromkeys(product_urls))[:2000]
    category_urls = list(dict.fromkeys(category_urls))[:200]

    if product_urls:
        notes.append(f"RSC extraction: {len(product_urls)} product URLs")
        return DiscoveryResult(
            method="nextjs-rsc",
            product_urls=product_urls,
            estimated_count=len(product_urls),
        )

    notes.append(f"RSC extraction: {len(category_urls)} category URLs (no direct product URLs)")
    return DiscoveryResult(
        method="nextjs-rsc",
        category_urls=category_urls,
        estimated_count=0,
    )


def _probe_common_paths_playwright(website_url: str, notes: List[str]) -> Optional[DiscoveryResult]:
    """
    Probe common entry paths using Playwright rendering.
    Called only when the site is JS-rendered and requests-based probing failed.
    """
    from crawler.extractors.html_extractor import find_product_urls, find_category_urls

    listing_pages: List[str] = []
    real_product_urls: set = set()

    try:
        from crawler.playwright_renderer import render_page
    except ImportError:
        notes.append("Playwright: not installed — skipping common path probe")
        return None

    for path in COMMON_ENTRY_PATHS[:12]:   # limit to avoid spending too long
        url = website_url.rstrip("/") + path
        try:
            html = render_page(url, timeout_ms=20_000, scroll=False, retries=0)
            if not html:
                continue
            products = find_product_urls(html, url)
            cats     = find_category_urls(html, url)
            if products or cats:
                listing_pages.append(url)
                real_product_urls.update(products)
                notes.append(
                    f"Playwright path {path}: {len(products)} products, "
                    f"{len(cats)} categories"
                )
            if len(listing_pages) >= 5:
                break
        except Exception as exc:
            notes.append(f"Playwright path {path} error: {exc}")
            continue

    if not listing_pages:
        notes.append("Playwright common paths: nothing found")
        return None

    return DiscoveryResult(
        method="playwright-category",
        category_urls=listing_pages,
        estimated_count=len(real_product_urls),
    )
