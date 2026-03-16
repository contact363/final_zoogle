"""
Celery tasks that launch Scrapy crawls in a subprocess and record logs.
run_crawl_direct() is a Redis-free fallback for when Celery is unavailable.

Architecture
────────────
• ONE spider ("generic") handles ALL websites.
• Per-website behaviour is controlled by WebsiteTrainingRules rows in the DB.
• No dedicated per-site spider files — the rule system makes that unnecessary.

Self-healing
────────────
  1. Pre-flight: run 'scrapy list' to verify spider loads before crawling.
  2. Full scrapy output is stored in crawl_log.log_output on every run.
  3. Errors are parsed from output and stored in crawl_log.error_details.
  4. machine_count on Website is updated after every crawl.
"""
import json
import subprocess
import sys
import os
import re
import threading
from datetime import datetime, timezone
from urllib.parse import urlparse

from celery import shared_task
from loguru import logger

from tasks.celery_app import celery_app
from app.config import settings

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

_sync_engine = create_engine(settings.DATABASE_SYNC_URL, pool_pre_ping=True)
SyncSession = sessionmaker(bind=_sync_engine, expire_on_commit=False)


def get_sync_db() -> Session:
    return SyncSession()


_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_CRAWLER_DIR = os.path.join(_BACKEND_DIR, "crawler")


def _build_subprocess_env() -> dict:
    env = os.environ.copy()
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = _BACKEND_DIR + (os.pathsep + existing if existing else "")
    return env


# ─────────────────────────────────────────────────────────────────────────────
# Pre-flight: verify spider loads before wasting time crawling
# ─────────────────────────────────────────────────────────────────────────────

def _preflight_check() -> tuple[bool, str]:
    """
    Run 'scrapy list' to verify that the spider can be imported without errors.
    Returns (ok: bool, output: str).
    """
    try:
        result = subprocess.run(
            [sys.executable, "-m", "scrapy", "list"],
            cwd=_CRAWLER_DIR,
            capture_output=True,
            text=True,
            timeout=30,
            env=_build_subprocess_env(),
        )
        combined = (result.stdout or "") + (result.stderr or "")
        if result.returncode != 0 or not result.stdout.strip():
            return False, f"Spider pre-flight FAILED (returncode={result.returncode}):\n{combined}"
        return True, "Spider loaded OK"
    except Exception as e:
        return False, f"Pre-flight exception: {e}"


# ─────────────────────────────────────────────────────────────────────────────
# Build full training-rules payload for the spider
# ─────────────────────────────────────────────────────────────────────────────

_RULE_FIELDS = (
    # HTML selectors
    "listing_selector", "title_selector", "url_selector",
    "description_selector", "image_selector", "price_selector",
    "category_selector", "pagination_selector",
    # Crawl mode
    "crawl_type", "use_playwright",
    # API config
    "api_url", "api_key", "api_headers_json", "api_data_path",
    "api_pagination_param", "api_page_size", "field_map_json",
    # URL filtering
    "product_link_pattern", "skip_url_patterns",
    # Request control
    "request_delay", "max_items",
)


def _load_training_rules(db: Session, website_id: int) -> str | None:
    """
    Load all rule fields from WebsiteTrainingRules for this website.
    Returns a compact JSON string (or None if no rules / all empty).
    """
    from app.models.training_rules import WebsiteTrainingRules

    row = db.query(WebsiteTrainingRules).filter(
        WebsiteTrainingRules.website_id == website_id
    ).first()

    if not row:
        logger.info(f"No training rules for website {website_id} — using auto-discovery")
        return None

    rules = {}
    for field in _RULE_FIELDS:
        val = getattr(row, field, None)
        if val is not None:
            # Numeric types (Decimal) → convert to native Python type
            if hasattr(val, "__float__"):
                val = float(val)
            rules[field] = val

    if not rules:
        logger.info(f"Training rules row exists for website {website_id} but all fields empty")
        return None

    logger.info(f"Training rules for website {website_id}: {list(rules.keys())}")
    return json.dumps(rules)


# ─────────────────────────────────────────────────────────────────────────────
# Run scrapy subprocess — always uses the "generic" spider
# ─────────────────────────────────────────────────────────────────────────────

_DEDICATED_SPIDERS = {
    "zatpatmachines.com":  "zatpatmachines",
    "zatpatestimate.com":  "zatpatmachines",   # same Supabase spider, auto-discovers table
    "corelmachine.com":    "corelmachine",
}


def _spider_for_url(url: str) -> str:
    """Return the dedicated spider name for known sites, else 'generic'."""
    for domain, spider in _DEDICATED_SPIDERS.items():
        if domain in url:
            return spider
    return "generic"


def _run_scrapy(
    website_id: int,
    start_url: str,
    crawl_log_id: int,
    training_rules_json: str | None = None,
) -> subprocess.CompletedProcess:
    """
    Launch scrapy crawl <spider> -a website_id=N ...

    If a URL file from Phase 2 URL Collection exists for this website,
    it is passed to the generic spider via -a url_file=... so the spider
    crawls exactly those pre-collected URLs instead of re-discovering them.
    """
    spider = _spider_for_url(start_url)
    cmd = [
        sys.executable, "-m", "scrapy", "crawl", spider,
        "-a", f"website_id={website_id}",
        "-a", f"start_url={start_url}",
        "-a", f"crawl_log_id={crawl_log_id}",
        "--set", "LOG_LEVEL=INFO",
    ]
    if training_rules_json and spider == "generic":
        cmd += ["-a", f"training_rules={training_rules_json}"]

    # Pass pre-collected URL file to generic spider (Phase 2 output)
    fpath = url_file_path(website_id)
    if spider == "generic" and os.path.exists(fpath) and os.path.getsize(fpath) > 0:
        cmd += ["-a", f"url_file={fpath}"]
        logger.info(f"URL-file mode: passing {fpath} to spider")

    logger.info(f"Running spider={spider!r} for website_id={website_id} url={start_url}")
    return subprocess.run(
        cmd,
        cwd=_CRAWLER_DIR,
        capture_output=True,
        text=True,
        timeout=3600,
        env=_build_subprocess_env(),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Output parsing
# ─────────────────────────────────────────────────────────────────────────────

def _parse_scrapy_stats(output: str) -> dict:
    """Parse Scrapy's final stats block from combined stdout+stderr."""
    stats = {}

    block_match = re.search(r"Dumping Scrapy stats.*?(\{.*?\})", output, re.DOTALL)
    if block_match:
        block = block_match.group(1)
        for m in re.finditer(r"'([\w/]+)':\s*(\d+)", block):
            stats[m.group(1)] = int(m.group(2))

    if not stats:
        for line in output.split("\n"):
            for key in ("item_scraped_count", "spider_exceptions_count",
                        "downloader/response_count", "item_dropped_count"):
                m = re.search(rf"'{key}':\s*(\d+)", line)
                if m:
                    stats[key] = int(m.group(1))

    return stats


def _extract_error_summary(output: str) -> str | None:
    """Pull the most useful error lines from scrapy output."""
    if not output:
        return "No output captured"

    error_lines = []
    lines = output.split("\n")

    for i, line in enumerate(lines):
        if any(m in line for m in [
            "Traceback", "ERROR", "CRITICAL", "Error:", "Exception:",
            "ImportError", "ModuleNotFoundError",
        ]):
            error_lines.extend(lines[i:i + 6])

    if error_lines:
        return "\n".join(dict.fromkeys(error_lines))[:3000]

    return None


# ─────────────────────────────────────────────────────────────────────────────
# Phase 1 — Discovery: count machines without a full crawl
# ─────────────────────────────────────────────────────────────────────────────

_PRODUCT_URL_KEYWORDS = (
    "/machine/", "/machines/", "/product/", "/products/", "/equipment/",
    "/item/", "/listing/", "/crane/", "/lathe/", "/mill/", "/grinder/",
    "/press/", "/robot/", "/hoist/", "/compressor/", "/conveyor/",
    "/machinetools/", "/machine-tools/", "/used-", "/second-hand/",
    "/gebraucht/", "/occasion/", "/catalog/", "/stock/", "/inventory/",
    "/usedmachine/", "/usedmachines/", "/usedmachinestocklist/",
)

_LISTING_PATHS = (
    "/machines", "/products", "/equipment", "/machine-tools", "/inventory",
    "/catalog", "/used", "/stock", "/shop", "/listings", "/for-sale",
    "/used-machines", "/used-equipment", "/cranes", "/lathes", "/mills",
    "/hoists", "/compressors", "/robots", "/grinders", "/presses",
    "/maschinen", "/occasions", "/gebrauchtmaschinen",
    # Additional category paths
    "/product-category", "/product-categories", "/categories",
    "/machine-category", "/machine-categories", "/all-machines",
    "/all-products", "/buy", "/sale", "/items", "/tools",
)

# CSS class patterns indicating product card/grid structures
_PRODUCT_CARD_CLASSES = (
    "product-card", "product-item", "product-grid", "product-listing",
    "product-list", "products-grid", "item-card", "machine-card",
    "machine-item", "machine-listing", "listing-item", "catalog-item",
    "grid-item", "woocommerce-loop-product", "wc-block-grid__product",
    "product_type_", "type-product", "post-type-archive-product",
)

_JSON_API_PATHS = (
    "/api/subcategory/all", "/api/subcategories",
    "/api/products", "/api/machines", "/api/machine-types",
    "/api/stock", "/api/inventory", "/api/categories",
)


def _is_product_url(url: str) -> bool:
    """Quick heuristic: does this URL look like a machine/product detail page?"""
    path = urlparse(url).path.lower()
    # Skip pagination, filters, assets
    if re.search(r"\.(css|js|jpg|jpeg|png|gif|svg|ico|woff|pdf)$", path):
        return False
    if re.search(r"/(page|tag|category|author|feed|wp-content|wp-admin)/", path):
        return False
    # Positive signals: contains a numeric ID or product keyword
    has_keyword = any(kw in path for kw in _PRODUCT_URL_KEYWORDS)
    has_numeric_id = bool(re.search(r"/\d{3,}(/|$|-)", path))
    return has_keyword or has_numeric_id


def _extract_product_links(html: str, base: str) -> set:
    """Extract all product-like links from an HTML page."""
    netloc = urlparse(base).netloc
    links = set()
    for href in re.findall(r'href=["\']([^"\'#?][^"\']*)["\']', html):
        try:
            full = href if href.startswith("http") else f"{base.rstrip('/')}/{href.lstrip('/')}"
            if urlparse(full).netloc == netloc and _is_product_url(full):
                links.add(full.split("?")[0].split("#")[0])
        except Exception:
            pass
    return links


def _max_page_number(html: str) -> int:
    """Detect the highest page number from pagination links in HTML."""
    max_page = 1
    for m in re.finditer(r'[?&/]page[/=](\d+)', html, re.IGNORECASE):
        max_page = max(max_page, int(m.group(1)))
    # WordPress /page/N/ style
    for m in re.finditer(r'/page/(\d+)', html, re.IGNORECASE):
        max_page = max(max_page, int(m.group(1)))
    # ?p=N or &p=N numeric pagination
    for m in re.finditer(r'[?&]p=(\d+)', html):
        try:
            max_page = max(max_page, int(m.group(1)))
        except Exception:
            pass
    return max_page


def _find_dominant_url_pattern(urls: set) -> tuple[str, int]:
    """
    Given a set of internal links, find the most repeated first-level path
    prefix — signals a product listing pattern (e.g. '/product/' repeated 80×).
    Returns (prefix, count) or ("", 0) if no dominant pattern found.
    """
    from collections import Counter
    prefix_counter: Counter = Counter()
    for url in urls:
        path = urlparse(url).path
        parts = [p for p in path.split("/") if p]
        if len(parts) >= 2:
            prefix_counter[f"/{parts[0]}/"] += 1
    if not prefix_counter:
        return "", 0
    top_prefix, count = prefix_counter.most_common(1)[0]
    return top_prefix, count


def _collect_all_internal_links(html: str, base: str, netloc: str) -> set:
    """Extract all same-domain non-asset links from an HTML page."""
    netloc_norm = netloc.lstrip("www.")
    links: set = set()
    for href in re.findall(r'href=["\']([^"\'#?][^"\']*)["\']', html):
        try:
            full = href if href.startswith("http") else f"{base}/{href.lstrip('/')}"
            fu = urlparse(full)
            if fu.netloc.lower().lstrip("www.") != netloc_norm:
                continue
            path = fu.path.lower()
            if re.search(r"\.(css|js|jpg|jpeg|png|gif|svg|ico|woff|pdf|zip)$", path):
                continue
            if re.search(r"/(login|register|cart|checkout|wp-admin|wp-content|wp-includes)/", path):
                continue
            links.add(full.split("?")[0].split("#")[0])
        except Exception:
            pass
    return links


_UA_POOL = [
    # Chrome on Windows (most common)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    # Chrome on Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    # Firefox on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    # Safari on Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
    # Edge on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
]


def _make_session(ua_index: int = 0):
    """
    Build a requests.Session that closely mimics a real browser:
    - Rotatable User-Agent pool (ua_index 0..4)
    - Full browser headers including Referer, Sec-Fetch-*, Cache-Control
    - Cookie jar enabled (Cloudflare and many CDNs set cookies on first visit)
    - SSL verification disabled (many supplier sites have expired/self-signed certs)
    - Warnings suppressed
    """
    try:
        import requests as _req
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    except Exception:
        return None, None

    ua = _UA_POOL[ua_index % len(_UA_POOL)]
    s = _req.Session()
    s.verify = False
    # Cookie jar is enabled by default in requests.Session — nothing extra needed
    s.headers.update({
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
    })
    return s, _req


def _safe_get(session, url: str, timeout: int = 20, json_mode: bool = False,
              retries: int = 2, retry_delay: float = 2.5):
    """
    Fetch a URL and return the response. Never raises.

    Retry logic:
    - On 403 (bot-blocked) or 429 (rate-limited): wait retry_delay seconds,
      swap to a different User-Agent, and retry up to `retries` times.
    - On connection error / timeout: retry once immediately.
    """
    import time as _time

    if json_mode:
        extra_hdrs = {
            "Accept": "application/json, text/plain, */*",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
        }
    else:
        extra_hdrs = {}

    last_r = None
    for attempt in range(retries + 1):
        try:
            r = session.get(url, timeout=timeout, headers=extra_hdrs, allow_redirects=True)
            last_r = r
            if r.status_code not in (403, 429, 503):
                return r
            # Blocked — rotate UA and wait before retry
            if attempt < retries:
                logger.debug(f"[_safe_get] {r.status_code} on {url} (attempt {attempt+1}) — rotating UA and retrying")
                _time.sleep(retry_delay)
                # Swap User-Agent
                new_ua = _UA_POOL[(attempt + 1) % len(_UA_POOL)]
                session.headers.update({
                    "User-Agent": new_ua,
                    "Referer": f"{url.split('/')[0]}//{url.split('/')[2]}/",
                    "Sec-Fetch-Site": "same-origin",
                })
        except Exception as exc:
            logger.debug(f"[_safe_get] Connection error on {url} (attempt {attempt+1}): {exc}")
            if attempt < retries:
                _time.sleep(1.0)
            last_r = None
    return last_r


def _resolve_base(session, start_url: str, timeout: int = 20) -> tuple[str, str, str | None]:
    """
    Resolve the real base URL for a website and return the homepage HTML.
    Returns (actual_base, actual_netloc, homepage_html_or_None).

    'Reachable' = any HTTP response (even 403) — only None means truly down.

    Strategy:
      1. Try original URL (with built-in _safe_get retry on 403/429)
      2. Try www/non-www alternate
      3. Try http:// if https:// failed
      4. If homepage is blocked (403/429/503), try /sitemap.xml and common listing
         paths to get any usable HTML from the same origin.
    """
    import time as _time

    parsed = urlparse(start_url)
    base = start_url.rstrip("/")

    # Build list of candidates: original → www/non-www → http fallback
    candidates = [start_url]
    if "://www." in start_url:
        candidates.append(start_url.replace("://www.", "://"))
    else:
        candidates.append(start_url.replace("://", "://www."))
    if start_url.startswith("https://"):
        candidates.append(start_url.replace("https://", "http://"))

    best_r = None
    for candidate in candidates:
        r = _safe_get(session, candidate, timeout=timeout, retries=2, retry_delay=2.5)
        if r is None:
            continue
        if best_r is None:
            best_r = r
        if r.status_code == 200:
            best_r = r
            break

    if best_r is None:
        # Completely unreachable
        return base, parsed.netloc.lower(), None

    final_url = getattr(best_r, "url", start_url)
    fp = urlparse(final_url)
    actual_base = f"{fp.scheme}://{fp.netloc}"
    actual_netloc = fp.netloc.lower()

    if best_r.status_code == 200:
        return actual_base, actual_netloc, best_r.text

    # Homepage returned 4xx/5xx — site is UP but blocking us.
    # Try alternative paths that are less likely to be Cloudflare-protected:
    # static files, sitemaps, and plain listing pages often bypass WAF rules.
    fallback_paths = [
        "/sitemap.xml", "/sitemap_index.xml", "/robots.txt",
        "/products", "/machines", "/machine-tools", "/equipment",
        "/product-category", "/inventory", "/used-machines", "/catalog",
    ]
    for fp_path in fallback_paths:
        r2 = _safe_get(session, f"{actual_base}{fp_path}", timeout=timeout, retries=1, retry_delay=1.5)
        if r2 and r2.status_code == 200:
            logger.info(f"[resolve_base] Homepage blocked ({best_r.status_code}) but {fp_path} returned 200")
            # Return the fallback HTML; caller treats this as partial homepage
            return actual_base, actual_netloc, r2.text

    # All paths blocked — return None for html but keep actual_base correct
    logger.info(f"[resolve_base] {actual_base} returned {best_r.status_code} on all paths — site is UP but heavily blocked")
    return actual_base, actual_netloc, None


def _discover_count(start_url: str) -> tuple[int, str]:
    """
    HTTP-based machine count. No Scrapy, no imports from zoogle_crawler.

    Tries in order (fastest → slowest):
      1. Shopify   /products/count.json
      2. WooCommerce /wp-json/wc/v3/products  (X-WP-Total header)
      3. WordPress  /wp-json/wp/v2/posts       (X-WP-Total — generic WP count)
      4. Supabase  PostgREST count=exact      (Content-Range header)
      5. JSON API  /api/subcategory/all etc.  (sum products across subcategories)
      6. Sitemap   /sitemap.xml               (count product <loc> entries)
      7. HTML scan homepage + nav links + listing pages + pagination estimation

    Returns (count, method_name).
      count = -1 ONLY when the site cannot be reached at all.
      All other failures return estimated count or 0 with "html-scan" method.
    """
    session, _req = _make_session()
    if session is None:
        return -1, "requests-unavailable"

    timeout = 20

    # ── Resolve real base URL first (handles www/non-www, http/https, redirects)
    # Any HTTP response (even 403) means site is UP; only None means truly unreachable.
    actual_base, actual_netloc, homepage_html_cached = _resolve_base(session, start_url, timeout=timeout)
    if actual_base == start_url.rstrip("/") and homepage_html_cached is None:
        # _resolve_base got no response at all → site is completely unreachable
        # But don't give up yet — try one more time with a longer timeout
        actual_base, actual_netloc, homepage_html_cached = _resolve_base(session, start_url, timeout=30)

    base = actual_base
    domain = actual_netloc.lstrip("www.")

    logger.info(f"[discovery] Resolved base: {base} (domain={domain})")

    # ── 1. Shopify ───────────────────────────────────────────────────────────
    r = _safe_get(session, f"{base}/products/count.json", timeout=timeout, json_mode=True)
    if r and r.status_code == 200:
        try:
            data = r.json()
            if isinstance(data, dict) and "count" in data:
                return int(data["count"]), "shopify"
        except Exception:
            pass

    # ── 2. WooCommerce ───────────────────────────────────────────────────────
    r = _safe_get(session, f"{base}/wp-json/wc/v3/products?per_page=1", timeout=timeout, json_mode=True)
    if r and r.status_code == 200:
        total = r.headers.get("X-WP-Total")
        if total:
            try:
                n = int(total)
                if n > 0:
                    return n, "woocommerce"
            except Exception:
                pass

    # ── 3. WordPress REST API (generic — works even without WooCommerce) ──────
    #    /wp-json/wp/v2/posts?per_page=1 returns X-WP-Total for all published posts
    #    Many machine dealer WordPress sites list products as custom post types
    for wp_type in ("product", "machine", "equipment", "listing", "posts"):
        r = _safe_get(
            session,
            f"{base}/wp-json/wp/v2/{wp_type}?per_page=1&status=publish",
            timeout=timeout, json_mode=True,
        )
        if r and r.status_code == 200:
            total = r.headers.get("X-WP-Total")
            if total:
                try:
                    n = int(total)
                    if n > 0:
                        return n, f"wordpress:{wp_type}"
                except Exception:
                    pass

    # ── 3b. corelmachine.com — count via /api/subcategory/all + sample 3 slugs ──
    # We only sample 3 subcategories to avoid hitting rate-limits before URL collection.
    if "corelmachine.com" in domain:
        _COREL_BASE = "https://corelmachine.com"
        _NON_CAT_D = {"", "selltous", "aboutus", "contactus", "blogs",
                      "privacy-policy", "terms-conditions", "cookie-policy",
                      "marketing-opt-out", "sitemap"}
        r = _safe_get(session, f"{_COREL_BASE}/api/subcategory/all", timeout=timeout, json_mode=True)
        if r and r.status_code == 200:
            try:
                subcats = r.json()
                if isinstance(subcats, list):
                    valid_slugs = [
                        (entry.get("url") or entry.get("slug") or "").strip()
                        for entry in subcats
                        if (entry.get("url") or entry.get("slug") or "").strip()
                        and (entry.get("url") or entry.get("slug") or "").strip() not in _NON_CAT_D
                    ]
                    total_cats = len(valid_slugs)
                    if total_cats > 0:
                        # Sample at most 3 subcategories to estimate total count
                        _SAMPLE_SIZE = 3
                        sample_slugs = valid_slugs[:_SAMPLE_SIZE]
                        total_corel = 0
                        sampled = 0
                        for slug in sample_slugs:
                            pr = _safe_get(session, f"{_COREL_BASE}/api/product/{slug}",
                                           timeout=timeout, json_mode=True)
                            if pr and pr.status_code == 200:
                                try:
                                    products = pr.json()
                                    if isinstance(products, dict):
                                        for k in ("data", "results", "products"):
                                            if isinstance(products.get(k), list):
                                                products = products[k]
                                                break
                                    if isinstance(products, list):
                                        total_corel += len(products)
                                        sampled += 1
                                except Exception:
                                    pass
                        if sampled > 0:
                            # Extrapolate to all categories if we only sampled a subset
                            if sampled < total_cats:
                                avg = total_corel / sampled
                                estimated = int(avg * total_cats)
                                return estimated, f"corelmachine-api(~estimated,{sampled}/{total_cats} subcategories)"
                            return total_corel, f"corelmachine-api({sampled} subcategories)"
            except Exception:
                pass

    # ── 4. Supabase (inline credentials) ─────────────────────────────────────
    _ZATPAT_SUPABASE_URL = "https://aqhgorgilxwrhzleztby.supabase.co"
    _ZATPAT_SUPABASE_KEY = (
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
        ".eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImFxaGdvcmdpbHh3cmh6bGV6dGJ5Iiw"
        "icm9sZSI6ImFub24iLCJpYXQiOjE3NjU0MjMxNDEsImV4cCI6MjA4MDk5OTE0MX0"
        ".GD_HVD-98oUUM9RteG_DxPD3Deg8lyqLpq9d8tgYA5A"
    )
    if "zatpat" in domain:
        sb_tables = ["machines", "products", "listings", "items",
                     "inventory", "estimates", "used_machines", "machine_listings"]
        session.headers.update({
            "apikey": _ZATPAT_SUPABASE_KEY,
            "Authorization": f"Bearer {_ZATPAT_SUPABASE_KEY}",
            "Prefer": "count=exact",
            "Range": "0-0",
        })
        for table in sb_tables:
            r = _safe_get(
                session,
                f"{_ZATPAT_SUPABASE_URL}/rest/v1/{table}?select=id",
                timeout=timeout,
            )
            if r and r.status_code in (200, 206):
                cr = r.headers.get("Content-Range", "")
                m = re.search(r"/(\d+)$", cr)
                if m and int(m.group(1)) > 0:
                    return int(m.group(1)), f"supabase:{table}"
        # Remove Supabase headers for subsequent requests
        for h in ("apikey", "Authorization", "Prefer", "Range"):
            session.headers.pop(h, None)

    # ── 5. JSON API probe ─────────────────────────────────────────────────────
    for api_path in _JSON_API_PATHS:
        r = _safe_get(session, f"{base}{api_path}", timeout=timeout, json_mode=True)
        if not r or r.status_code != 200:
            continue
        body = r.text.strip()
        if not body or body[0] not in ("[", "{"):
            continue
        try:
            data = r.json()
        except Exception:
            continue

        if isinstance(data, dict):
            for key in ("data", "results", "items", "products", "machines"):
                if isinstance(data.get(key), list):
                    data = data[key]
                    break

        if not isinstance(data, list) or not data:
            continue

        first = data[0]
        is_subcategory = (
            isinstance(first, dict)
            and ("url" in first or "slug" in first)
            and "price" not in first
            and len(data) <= 300
        )
        if is_subcategory:
            logger.info(f"[discovery] JSON subcategory list at {api_path}: {len(data)} categories")
            total = 0
            sampled = 0
            for entry in data:
                slug = entry.get("url") or entry.get("slug") or entry.get("id")
                if not slug:
                    continue
                pr = _safe_get(
                    session,
                    f"{base}/api/product/{slug}",
                    timeout=timeout, json_mode=True,
                )
                if pr and pr.status_code == 200:
                    try:
                        pdata = pr.json()
                        if isinstance(pdata, dict):
                            for k in ("data", "results", "products", "machines"):
                                if isinstance(pdata.get(k), list):
                                    pdata = pdata[k]
                                    break
                        if isinstance(pdata, list):
                            total += len(pdata)
                            sampled += 1
                    except Exception:
                        pass
            if sampled > 0:
                if sampled < len(data):
                    total = int(total / sampled * len(data))
                    return total, f"json-api:subcategories(~estimated,{sampled}/{len(data)})"
                return total, f"json-api:subcategories({sampled})"
        else:
            if len(data) > 0:
                return len(data), f"json-api:{api_path}"

    # ── 6. Sitemap ────────────────────────────────────────────────────────────
    for sitemap_path in ("/sitemap.xml", "/sitemap_index.xml",
                         "/product-sitemap.xml", "/page-sitemap.xml",
                         "/sitemap-products.xml"):
        r = _safe_get(session, f"{base}{sitemap_path}", timeout=timeout)
        if not r or r.status_code != 200 or "<loc>" not in r.text:
            continue

        child_sitemaps = re.findall(r"<sitemap>\s*<loc>(.*?)</loc>", r.text, re.DOTALL)
        if child_sitemaps:
            best = 0
            for child_url in child_sitemaps[:5]:
                cr = _safe_get(session, child_url.strip(), timeout=timeout)
                if cr and cr.status_code == 200 and "<loc>" in cr.text:
                    urls = re.findall(r"<loc>(.*?)</loc>", cr.text, re.DOTALL)
                    cnt = sum(1 for u in urls if _is_product_url(u.strip()))
                    best += cnt
            if best > 0:
                return best, "sitemap"
            continue

        urls = re.findall(r"<loc>(.*?)</loc>", r.text, re.DOTALL)
        cnt = sum(1 for u in urls if _is_product_url(u.strip()))
        if cnt > 0:
            return cnt, "sitemap"

    # ── 7. HTML scan ──────────────────────────────────────────────────────────
    # homepage_html_cached may already contain HTML from _resolve_base (which
    # tried fallback paths like /sitemap.xml and /products if homepage was 403).
    # If it's still None → the site was truly unreachable at this point.

    if homepage_html_cached is None:
        # Last-ditch: try with a fresh session using a different UA
        session2, _ = _make_session(ua_index=2)
        if session2:
            actual_base2, actual_netloc2, html2 = _resolve_base(session2, start_url, timeout=30)
            if html2:
                homepage_html_cached = html2
                base = actual_base2
                actual_netloc = actual_netloc2
        if homepage_html_cached is None:
            return -1, "site-unreachable"

    homepage_html = homepage_html_cached or ""
    netloc_norm = actual_netloc.lstrip("www.")

    all_product_urls: set = set()
    all_product_urls.update(_extract_product_links(homepage_html, base))

    # ── [Enhancement 3] HTML Structure Detection ─────────────────────────────
    # Detect product card/grid CSS patterns even before crawling listing pages.
    product_card_detected = any(cls in homepage_html for cls in _PRODUCT_CARD_CLASSES)
    if product_card_detected:
        logger.info("[discovery] Product card structure detected on homepage HTML")

    # ── [Enhancement 1] Category Page Detection ───────────────────────────────
    # Scan nav/menu links + blind-probe all known listing paths.
    category_urls: list = []
    seen_cat: set = set()

    def _add_cat(url):
        clean = url.split("?")[0].split("#")[0].rstrip("/")
        if clean not in seen_cat:
            seen_cat.add(clean)
            category_urls.append(clean)

    for href in re.findall(r'href=["\']([^"\'#][^"\']*)["\']', homepage_html):
        try:
            full = href if href.startswith("http") else f"{base}/{href.lstrip('/')}"
            fu = urlparse(full)
            if fu.netloc.lower().lstrip("www.") != netloc_norm:
                continue
            path = fu.path.lower().rstrip("/")
            if any(path == lp or path.endswith(lp) or lp.rstrip("/") in path
                   for lp in _LISTING_PATHS):
                _add_cat(full)
        except Exception:
            pass

    # Blind-probe all known listing paths (may not appear in nav)
    for lp in _LISTING_PATHS:
        _add_cat(f"{base}{lp}")

    # ── Crawl category/listing pages ─────────────────────────────────────────
    total_estimated = 0
    methods_used = []
    cat_pages_checked = 0
    cat_pages_with_products = 0

    for cat_url in category_urls[:20]:
        r = _safe_get(session, cat_url, timeout=timeout)
        if not r or r.status_code != 200:
            continue
        cat_pages_checked += 1
        cat_html = r.text

        # Check for product card structures on category pages too
        if any(cls in cat_html for cls in _PRODUCT_CARD_CLASSES):
            product_card_detected = True

        page_products = _extract_product_links(cat_html, base)
        if not page_products:
            continue

        cat_pages_with_products += 1
        all_product_urls.update(page_products)
        per_page = len(page_products)
        max_page = _max_page_number(cat_html)

        if max_page > 1:
            est = per_page * max_page
            total_estimated += est
            methods_used.append(f"{urlparse(cat_url).path}(~{est})")
            logger.info(f"[discovery] {cat_url}: {per_page}/page × {max_page} pages ≈ {est}")
        else:
            total_estimated += per_page
            methods_used.append(f"{urlparse(cat_url).path}({per_page})")

    # ── [Enhancement 2] Product Link Pattern Detection ────────────────────────
    # Collect all internal links from homepage and detect repeating URL prefixes
    # (e.g. /product/ appearing 80× signals a product listing pattern).
    homepage_internal = _collect_all_internal_links(homepage_html, base, actual_netloc)
    dominant_pattern, pattern_count = _find_dominant_url_pattern(homepage_internal)
    if dominant_pattern and pattern_count >= 5:
        logger.info(f"[discovery] Dominant URL pattern: {dominant_pattern} (×{pattern_count})")
        # Count those internal links as product URLs if they match product keywords
        if any(kw in dominant_pattern for kw in ("/product", "/machine", "/equipment",
                                                   "/item", "/listing", "/catalog",
                                                   "/stock", "/used", "/tool")):
            for link in homepage_internal:
                if dominant_pattern in urlparse(link).path.lower():
                    all_product_urls.add(link)

    direct = len(all_product_urls)
    final  = max(direct, total_estimated)

    # ── [Enhancement 4] Fallback Depth-2 Scan ────────────────────────────────
    # If still no products found, scan first-level internal links (depth=2).
    if final == 0:
        logger.info("[discovery] No products found on surface scan — starting depth-2 fallback")
        depth2_candidates = list(homepage_internal)[:15]
        depth2_products: set = set()
        depth2_cat_found: list = []

        for d2_url in depth2_candidates:
            r = _safe_get(session, d2_url, timeout=timeout)
            if not r or r.status_code != 200:
                continue
            d2_html = r.text

            # Check for product card structures at depth-2
            if any(cls in d2_html for cls in _PRODUCT_CARD_CLASSES):
                product_card_detected = True

            found = _extract_product_links(d2_html, base)
            if found:
                depth2_products.update(found)
                depth2_cat_found.append(urlparse(d2_url).path)
                # Check pagination on this depth-2 page
                max_page = _max_page_number(d2_html)
                if max_page > 1:
                    est = len(found) * max_page
                    total_estimated += est
                    methods_used.append(f"depth2:{urlparse(d2_url).path}(~{est})")
                else:
                    methods_used.append(f"depth2:{urlparse(d2_url).path}({len(found)})")

            if len(depth2_products) >= 30:
                break  # sufficient evidence

        if depth2_products:
            all_product_urls.update(depth2_products)
            logger.info(f"[discovery] Depth-2 scan found {len(depth2_products)} product links on: {depth2_cat_found}")

        direct = len(all_product_urls)
        final  = max(direct, total_estimated)

    # ── Return result ─────────────────────────────────────────────────────────
    if final > 0:
        detail     = ", ".join(methods_used[:3]) if methods_used else "url-scan"
        is_est     = total_estimated > direct
        method_tag = f"html-scan~estimated({detail})" if is_est else f"html-scan({detail})"
        return final, method_tag

    # ── [Enhancement 5] Improved zero-result logging ──────────────────────────
    # Even when count=0, log what was detected for debugging.
    diag_parts = []
    if cat_pages_checked:
        diag_parts.append(f"checked {cat_pages_checked} category pages")
    if dominant_pattern and pattern_count >= 3:
        diag_parts.append(f"url-pattern:{dominant_pattern}(×{pattern_count})")
    if product_card_detected:
        diag_parts.append("product-card-html-detected")

    if diag_parts:
        diag = ", ".join(diag_parts)
        logger.info(f"[discovery] 0 products found but signals detected: {diag}")
        return 0, f"html-scan(0-found; {diag})"

    # Site reachable but truly no product signals detected
    return 0, "html-scan(no-products-detected)"


def _execute_discovery(website_id: int, db: Session) -> None:
    """
    Phase 1 full lifecycle:
      load website → count machines → create discovery log → update website
    """
    from app.models.website import Website
    from app.models.crawl_log import CrawlLog

    website = db.query(Website).filter(Website.id == website_id).first()
    if not website:
        logger.error(f"Discovery: website {website_id} not found")
        return

    # Mark discovery running
    website.discovery_status = "running"
    db.commit()

    log = CrawlLog(
        website_id=website_id,
        task_id=f"discovery-{website_id}-{int(datetime.now().timestamp())}",
        status="running",
        log_type="discovery",
    )
    db.add(log)
    db.commit()

    try:
        count, method = _discover_count(website.url)
        logger.info(f"Discovery: website={website_id} count={count} method={method}")

        # count == -1 means the site itself could not be reached
        # count == 0  means reachable but no products detected
        # count > 0   means we found machines (exact or estimated)
        site_unreachable = count == -1
        is_estimated = "estimated" in method or "~" in method
        is_blocked = "blocked" in method or "site-up-but-blocked" in method

        if not site_unreachable:
            if count == 0 and is_blocked:
                count_label = "blocked by server (WAF/Cloudflare)"
            elif is_estimated and count > 0:
                count_label = f"{count} (estimated)"
            else:
                count_label = str(count)

            # Build rich log output — include pattern/signal detail when count=0
            log_lines = [f"Discovery method: html pattern scan"]
            if "html-scan" in method:
                # Extract any detected pattern info embedded in the method string
                if "url-pattern:" in method:
                    m_pat = re.search(r"url-pattern:([^\s,)]+)", method)
                    if m_pat:
                        log_lines.append(f"Product pattern detected: {m_pat.group(1)}")
                if "product-card-html-detected" in method:
                    log_lines.append("Product card HTML structures detected")
                if "depth2:" in method:
                    log_lines.append("Products found via depth-2 page scan")
                # Underlying detection method for transparency
                log_lines.append(f"Detection detail: {method}")
            else:
                log_lines[0] = f"Discovery method: {method}"

            log_lines.append(f"Machines estimated: {count_label}")
            log_output = "\n".join(log_lines) + "\n"

            log.status = "success"
            log.machines_found = max(count, 0)
            log.log_output = log_output
            log.error_details = None
            website.discovered_count = count if count > 0 else None
            website.discovery_status = "done"
        else:
            log.status = "error"
            log.machines_found = 0
            log.log_output = f"Discovery method: {method}\nMachines found on site: unreachable"
            log.error_details = "Website could not be reached (connection refused, timeout, or DNS failure)"
            website.discovered_count = None
            website.discovery_status = "error"

        log.finished_at = datetime.now(timezone.utc)

    except Exception as exc:
        logger.exception(f"Discovery failed: website={website_id} error={exc}")
        log.status = "error"
        log.error_details = str(exc)
        log.finished_at = datetime.now(timezone.utc)
        website.discovery_status = "error"

    db.commit()

    # ── Auto-chain Phase 2 — URL Collection ───────────────────────────────────
    if website.discovery_status == "done":
        logger.info(f"Discovery done for website={website_id} — auto-starting URL collection")
        _execute_url_collection(website_id, db)


def run_discovery_direct(website_id: int):
    """Run Phase 1 → auto-chains Phase 2 URL collection."""
    db = get_sync_db()
    try:
        _execute_discovery(website_id, db)
    except Exception as exc:
        logger.exception(f"run_discovery_direct failed: website={website_id} error={exc}")
    finally:
        db.close()


# ─────────────────────────────────────────────────────────────────────────────
# Phase 2 — URL Collection: gather all machine/product URLs without full crawl
# ─────────────────────────────────────────────────────────────────────────────

_URL_FILE_DIR = "/tmp"


def url_file_path(website_id: int) -> str:
    return os.path.join(_URL_FILE_DIR, f"zoogle_urls_{website_id}.txt")


def _collect_urls(start_url: str, target_count: int = 0) -> tuple[list[str], str]:
    """
    Collect all machine/product URLs from a website without a full Scrapy crawl.

    Tries in order:
      1. Supabase  — query the API for all slugs/urls
      2. Shopify   — paginate /products.json
      3. WooCommerce — paginate /wp-json/wc/v3/products
      4. WordPress REST — paginate /wp-json/wp/v2/{product,machine,...}
      5. Sitemap   — extract all product <loc> URLs
      6. HTML scan — crawl listing pages + follow all pagination

    Returns (urls: list[str], method: str).
    """
    session, _ = _make_session()
    if session is None:
        return [], "requests-unavailable"

    timeout = 20

    # ── 0. Fast-path: site-specific API collectors BEFORE _resolve_base ───────
    # For known API-backed sites we skip the homepage fetch entirely — it saves
    # a request and avoids burning through rate-limit budget before the real work.
    parsed_start = urlparse(start_url)
    domain_early = parsed_start.netloc.lower().lstrip("www.")

    # corelmachine.com — Next.js App Router, JSON API at /api/subcategory/all
    if "corelmachine.com" in domain_early:
        _COREL_BASE = "https://corelmachine.com"  # non-www — API 404s on www variant
        _NON_CAT = {"", "selltous", "aboutus", "contactus", "blogs",
                    "privacy-policy", "terms-conditions", "cookie-policy",
                    "marketing-opt-out", "sitemap"}
        r = _safe_get(session, f"{_COREL_BASE}/api/subcategory/all", timeout=timeout, json_mode=True)
        if r and r.status_code == 200:
            try:
                subcats = r.json()
                if isinstance(subcats, list):
                    corel_urls: list[str] = []
                    for entry in subcats:
                        slug = (entry.get("url") or entry.get("slug") or "").strip()
                        if not slug or slug in _NON_CAT:
                            continue
                        # Try multiple product API patterns (same as the spider)
                        for api_path in (
                            f"{_COREL_BASE}/api/product/{slug}",
                            f"{_COREL_BASE}/api/product/subcategory/{slug}",
                            f"{_COREL_BASE}/api/products/{slug}",
                            f"{_COREL_BASE}/api/products?subcategory={slug}&status=true",
                        ):
                            pr = _safe_get(session, api_path, timeout=timeout, json_mode=True)
                            if not pr or pr.status_code != 200:
                                continue
                            try:
                                products = pr.json()
                                if isinstance(products, dict):
                                    for k in ("data", "results", "products", "machines", "items"):
                                        if isinstance(products.get(k), list):
                                            products = products[k]
                                            break
                                if not isinstance(products, list) or not products:
                                    continue
                                # Check it's a product list not a subcategory list
                                first = products[0] if isinstance(products[0], dict) else {}
                                if not any(f in first for f in ("price", "year", "reference_no",
                                                                 "capacity", "product_status",
                                                                 "stock_number", "condition", "url")) \
                                        and len(products) <= 50:
                                    continue
                                for p in products:
                                    if not isinstance(p, dict):
                                        continue
                                    url_slug = (p.get("url") or p.get("slug") or "").strip()
                                    if url_slug:
                                        if url_slug.startswith("http"):
                                            corel_urls.append(url_slug)
                                        else:
                                            corel_urls.append(
                                                f"{_COREL_BASE}/usedmachinestocklist/{slug}/{url_slug.lstrip('/')}"
                                            )
                                if corel_urls:
                                    break  # found products for this slug, move to next
                            except Exception:
                                continue
                    if corel_urls:
                        deduped = list(dict.fromkeys(corel_urls))
                        logger.info(f"[collect_urls] corelmachine API: {len(deduped)} URLs")
                        return deduped, "corelmachine-api"
            except Exception as e:
                logger.warning(f"[collect_urls] corelmachine API error: {e}")

    # ── Resolve real base URL for all other methods ───────────────────────────
    actual_base, actual_netloc, homepage_html_cached = _resolve_base(session, start_url, timeout=timeout)
    base = actual_base
    domain = actual_netloc.lstrip("www.")
    urls: list[str] = []

    # ── 1. Supabase (zatpatmachines / zatpatestimate) ─────────────────────────
    _ZATPAT_SUPABASE_URL = "https://aqhgorgilxwrhzleztby.supabase.co"
    _ZATPAT_SUPABASE_KEY = (
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
        ".eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImFxaGdvcmdpbHh3cmh6bGV6dGJ5Iiw"
        "icm9sZSI6ImFub24iLCJpYXQiOjE3NjU0MjMxNDEsImV4cCI6MjA4MDk5OTE0MX0"
        ".GD_HVD-98oUUM9RteG_DxPD3Deg8lyqLpq9d8tgYA5A"
    )
    if "zatpat" in domain:
        sb_tables = ["machines", "products", "listings", "items",
                     "inventory", "estimates", "used_machines", "machine_listings"]
        session.headers.update({
            "apikey": _ZATPAT_SUPABASE_KEY,
            "Authorization": f"Bearer {_ZATPAT_SUPABASE_KEY}",
        })

        # Priority order for URL-identifier columns (first found wins)
        _URL_FIELDS = ("url", "slug", "page_url", "product_url", "machine_url",
                       "listing_url", "link", "permalink", "source_url")
        _ID_FIELDS  = ("sku_number", "id_prefix", "sku", "product_code",
                       "reference_no", "id")

        for table in sb_tables:
            # ── Step A: probe one row to discover actual columns ──────────────
            probe = _safe_get(
                session,
                f"{_ZATPAT_SUPABASE_URL}/rest/v1/{table}?select=*&limit=1",
                timeout=timeout,
            )
            if not probe or probe.status_code != 200:
                continue
            try:
                probe_rows = probe.json()
            except Exception:
                continue
            if not isinstance(probe_rows, list) or not probe_rows:
                continue

            available = set(probe_rows[0].keys()) if isinstance(probe_rows[0], dict) else set()
            url_cols = [c for c in _URL_FIELDS if c in available]
            id_cols  = [c for c in _ID_FIELDS  if c in available]
            if not url_cols and not id_cols:
                continue

            select_cols = ",".join(dict.fromkeys(url_cols + id_cols))
            logger.info(f"[collect_urls] supabase table={table} select={select_cols}")

            # ── Step B: paginate and collect all URLs ─────────────────────────
            offset = 0
            page_size = 1000
            table_urls: list[str] = []
            while True:
                r = _safe_get(
                    session,
                    f"{_ZATPAT_SUPABASE_URL}/rest/v1/{table}"
                    f"?select={select_cols}&limit={page_size}&offset={offset}",
                    timeout=timeout,
                )
                if not r or r.status_code not in (200, 206):
                    break
                try:
                    rows = r.json()
                except Exception:
                    break
                if not isinstance(rows, list) or not rows:
                    break

                for row in rows:
                    # Try explicit URL/slug fields first
                    url_built = None
                    for uf in url_cols:
                        val = str(row.get(uf) or "").strip()
                        if val and val.lower() not in ("none", "null", ""):
                            if val.startswith("http"):
                                url_built = val
                            elif not val.isdigit():
                                url_built = f"{base}/{val.lstrip('/')}"
                            break
                    if not url_built:
                        # Fall back to ID-based URL construction
                        for idf in id_cols:
                            uid = str(row.get(idf) or "").strip()
                            if uid and uid.lower() not in ("none", "null", ""):
                                url_built = f"{base}/machine/{uid}"
                                break
                    if url_built:
                        table_urls.append(url_built)

                if len(rows) < page_size:
                    break
                offset += page_size

            if table_urls:
                for h in ("apikey", "Authorization"):
                    session.headers.pop(h, None)
                deduped = list(dict.fromkeys(table_urls))
                logger.info(f"[collect_urls] supabase:{table} → {len(deduped)} URLs")
                return deduped, f"supabase:{table}"

        for h in ("apikey", "Authorization"):
            session.headers.pop(h, None)

    # ── 2. Shopify ────────────────────────────────────────────────────────────
    page = 1
    shopify_urls: list[str] = []
    while True:
        r = _safe_get(session, f"{base}/products.json?limit=250&page={page}", timeout=timeout)
        if not r or r.status_code != 200:
            break
        try:
            products = r.json().get("products", [])
        except Exception:
            break
        if not products:
            break
        for p in products:
            handle = p.get("handle")
            if handle:
                shopify_urls.append(f"{base}/products/{handle}")
        if len(products) < 250:
            break
        page += 1
    if shopify_urls:
        return shopify_urls, "shopify"

    # ── 3. WooCommerce ────────────────────────────────────────────────────────
    page = 1
    wc_urls: list[str] = []
    while True:
        r = _safe_get(
            session,
            f"{base}/wp-json/wc/v3/products?per_page=100&page={page}",
            timeout=timeout, json_mode=True,
        )
        if not r or r.status_code != 200:
            break
        try:
            products = r.json()
        except Exception:
            break
        if not isinstance(products, list) or not products:
            break
        for p in products:
            link = p.get("permalink") or p.get("link")
            if link:
                wc_urls.append(link)
        total_pages = int(r.headers.get("X-WP-TotalPages", 1))
        if page >= total_pages:
            break
        page += 1
    if wc_urls:
        return wc_urls, "woocommerce"

    # ── 4. WordPress custom post types ────────────────────────────────────────
    for wp_type in ("product", "machine", "equipment", "listing"):
        page = 1
        wp_urls: list[str] = []
        while True:
            r = _safe_get(
                session,
                f"{base}/wp-json/wp/v2/{wp_type}?per_page=100&page={page}&status=publish",
                timeout=timeout, json_mode=True,
            )
            if not r or r.status_code != 200:
                break
            try:
                items = r.json()
            except Exception:
                break
            if not isinstance(items, list) or not items:
                break
            for item in items:
                link = item.get("link") or item.get("permalink")
                if link:
                    wp_urls.append(link)
            total_pages = int(r.headers.get("X-WP-TotalPages", 1))
            if page >= total_pages:
                break
            page += 1
        if wp_urls:
            return wp_urls, f"wordpress:{wp_type}"

    # ── 5. Generic JSON API probe ─────────────────────────────────────────────
    # Try common REST API paths for sites that serve machine data via JSON.
    # For each endpoint: extract items, build URLs from slug/id fields,
    # and follow pagination via "next", "page", or offset patterns.
    _GENERIC_API_PATHS = (
        "/api/machines", "/api/products", "/api/inventory", "/api/items",
        "/api/stock", "/api/listings", "/api/catalog",
        "/api/v1/machines", "/api/v1/products", "/api/v2/machines",
        "/machines.json", "/products.json",
        "/rest/v1/machines", "/rest/v1/products",
    )
    _API_URL_FIELDS = ("url", "slug", "permalink", "link", "machine_url",
                       "product_url", "listing_url", "page_url", "source_url")
    _API_ID_FIELDS  = ("sku_number", "sku", "id_prefix", "reference_no",
                       "product_code", "id")
    _API_WRAPPER_KEYS = ("data", "results", "items", "products", "machines",
                         "listings", "records", "inventory")

    def _build_url_from_row(row: dict, site_base: str) -> str | None:
        """Extract or construct a URL from a JSON row dict."""
        for uf in _API_URL_FIELDS:
            val = str(row.get(uf) or "").strip()
            if val and val.lower() not in ("none", "null", ""):
                if val.startswith("http"):
                    return val
                if not val.isdigit():
                    return f"{site_base}/{val.lstrip('/')}"
        for idf in _API_ID_FIELDS:
            uid = str(row.get(idf) or "").strip()
            if uid and uid.lower() not in ("none", "null", ""):
                return f"{site_base}/machine/{uid}"
        return None

    for api_path in _GENERIC_API_PATHS:
        r = _safe_get(session, f"{base}{api_path}", timeout=timeout, json_mode=True)
        if not r or r.status_code != 200:
            continue
        try:
            data = r.json()
        except Exception:
            continue

        # Unwrap common envelope keys
        total_available = None
        if isinstance(data, dict):
            total_available = (data.get("total") or data.get("count")
                               or data.get("total_count") or data.get("totalCount"))
            for wk in _API_WRAPPER_KEYS:
                if isinstance(data.get(wk), list):
                    data = data[wk]
                    break

        if not isinstance(data, list) or not data:
            continue

        # Check the first item looks like a machine record (has price/year/id/model)
        first = data[0] if isinstance(data[0], dict) else {}
        machine_signals = ("price", "year", "model", "brand", "condition",
                           "id", "sku", "sku_number", "reference_no", "machine_type")
        if not any(k in first for k in machine_signals):
            continue

        api_urls: list[str] = []
        for row in data:
            if isinstance(row, dict):
                u = _build_url_from_row(row, base)
                if u:
                    api_urls.append(u)

        if not api_urls:
            continue

        # Follow pagination — try offset/page patterns up to target_count
        page_size = len(data)
        offset = page_size
        max_pages = 500
        for _ in range(max_pages):
            if total_available and len(api_urls) >= int(total_available):
                break
            if target_count > 0 and len(api_urls) >= target_count:
                break
            sep = "&" if "?" in api_path else "?"
            next_url = f"{base}{api_path}{sep}offset={offset}&limit={page_size}"
            rp = _safe_get(session, next_url, timeout=timeout, json_mode=True)
            if not rp or rp.status_code != 200:
                break
            try:
                pdata = rp.json()
            except Exception:
                break
            if isinstance(pdata, dict):
                for wk in _API_WRAPPER_KEYS:
                    if isinstance(pdata.get(wk), list):
                        pdata = pdata[wk]
                        break
            if not isinstance(pdata, list) or not pdata:
                break
            for row in pdata:
                if isinstance(row, dict):
                    u = _build_url_from_row(row, base)
                    if u:
                        api_urls.append(u)
            if len(pdata) < page_size:
                break
            offset += page_size

        if api_urls:
            deduped = list(dict.fromkeys(api_urls))
            logger.info(f"[collect_urls] json-api:{api_path} → {len(deduped)} URLs")
            return deduped, f"json-api:{api_path}"

    # ── 5b. JavaScript data extraction ────────────────────────────────────────
    # Many SPA/Next.js sites embed all product data in the initial HTML as JSON
    # blobs: __NEXT_DATA__, window.__INITIAL_STATE__, Apollo cache, etc.
    if homepage_html_cached:
        js_urls: list[str] = []

        # Pattern 1: Next.js SSR payload (<script id="__NEXT_DATA__">)
        m = re.search(r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
                      homepage_html_cached, re.DOTALL)
        if m:
            try:
                nd = json.loads(m.group(1))
                # Walk the props tree looking for arrays of machine objects
                def _walk_nd(obj, depth=0):
                    if depth > 8 or not isinstance(obj, (dict, list)):
                        return
                    if isinstance(obj, list) and len(obj) > 2:
                        for item in obj:
                            if isinstance(item, dict):
                                u = _build_url_from_row(item, base)
                                if u:
                                    js_urls.append(u)
                        return
                    if isinstance(obj, dict):
                        for v in obj.values():
                            _walk_nd(v, depth + 1)
                _walk_nd(nd)
            except Exception:
                pass

        # Pattern 2: window.__DATA__ = {...} or window.INITIAL_STATE = {...}
        for js_pat in (r'window\.__(?:DATA|INITIAL_STATE|APP_STATE|STATE)__\s*=\s*(\{.*?\});',
                       r'var\s+__PRELOADED_STATE__\s*=\s*(\{.*?\});'):
            for m in re.finditer(js_pat, homepage_html_cached, re.DOTALL):
                try:
                    obj = json.loads(m.group(1))
                    def _walk_state(o, depth=0):
                        if depth > 6 or not isinstance(o, (dict, list)):
                            return
                        if isinstance(o, list):
                            for item in o:
                                if isinstance(item, dict):
                                    u = _build_url_from_row(item, base)
                                    if u:
                                        js_urls.append(u)
                            return
                        if isinstance(o, dict):
                            for v in o.values():
                                _walk_state(v, depth + 1)
                    _walk_state(obj)
                except Exception:
                    pass

        if js_urls:
            deduped = list(dict.fromkeys(js_urls))
            logger.info(f"[collect_urls] js-data-extraction → {len(deduped)} URLs")
            return deduped, "js-data-extraction"

    # ── 6. Sitemap — collect all product URLs ─────────────────────────────────
    sitemap_urls: list[str] = []
    for sitemap_path in ("/sitemap.xml", "/sitemap_index.xml",
                         "/product-sitemap.xml", "/sitemap-products.xml"):
        r = _safe_get(session, f"{base}{sitemap_path}", timeout=timeout)
        if not r or r.status_code != 200 or "<loc>" not in r.text:
            continue

        # Sitemap index — fetch all child sitemaps
        children = re.findall(r"<sitemap>\s*<loc>(.*?)</loc>", r.text, re.DOTALL)
        if children:
            for child_url in children:
                cr = _safe_get(session, child_url.strip(), timeout=timeout)
                if cr and cr.status_code == 200 and "<loc>" in cr.text:
                    locs = re.findall(r"<loc>(.*?)</loc>", cr.text, re.DOTALL)
                    sitemap_urls.extend(u.strip() for u in locs if _is_product_url(u.strip()))
            if sitemap_urls:
                break
            continue

        locs = re.findall(r"<loc>(.*?)</loc>", r.text, re.DOTALL)
        sitemap_urls.extend(u.strip() for u in locs if _is_product_url(u.strip()))
        if sitemap_urls:
            break

    # Deduplicate and check if sitemap coverage is sufficient
    sitemap_urls = list(dict.fromkeys(sitemap_urls))
    if sitemap_urls and (target_count <= 0 or len(sitemap_urls) >= target_count * 0.7):
        return sitemap_urls, "sitemap"

    # ── 6. HTML pagination scan — follow ALL listing pages ───────────────────
    # Use the homepage HTML already fetched by _resolve_base
    if homepage_html_cached is None:
        # Homepage was blocked — retry with a different UA
        session2, _ = _make_session(ua_index=2)
        if session2:
            actual_base2, actual_netloc2, html2 = _resolve_base(session2, start_url, timeout=30)
            if html2:
                homepage_html_cached = html2
                base = actual_base2
                actual_netloc = actual_netloc2
        if homepage_html_cached is None:
            # Use whatever we got from sitemap, or give up
            if sitemap_urls:
                return sitemap_urls, "sitemap(partial)"
            return [], "site-unreachable"

    homepage_html = homepage_html_cached or ""

    collected: set = set()
    # Add any product URLs found directly on homepage
    collected.update(_extract_product_links(homepage_html, base))

    # Discover all listing/category pages from homepage navigation
    listing_queue: list = []
    seen_listing: set = set()

    def _enqueue(url: str):
        clean = url.split("?")[0].split("#")[0].rstrip("/")
        if clean not in seen_listing:
            seen_listing.add(clean)
            listing_queue.append(clean)

    # From homepage links
    for href in re.findall(r'href=["\']([^"\'#][^"\']*)["\']', homepage_html):
        try:
            full = href if href.startswith("http") else f"{base}/{href.lstrip('/')}"
            if urlparse(full).netloc.lower().lstrip("www.") != actual_netloc.lstrip("www."):
                continue
            path = urlparse(full).path.lower().rstrip("/")
            if any(path == lp or lp.rstrip("/") in path for lp in _LISTING_PATHS):
                _enqueue(full)
        except Exception:
            pass

    # Probe standard listing paths
    for lp in _LISTING_PATHS:
        _enqueue(f"{base}{lp}")

    # For each listing page, collect product URLs and follow ALL pagination
    pages_crawled = 0
    for cat_url in listing_queue[:20]:           # max 20 categories
        for page_num in range(1, 201):           # max 200 pages per category
            if page_num == 1:
                page_url = cat_url
            else:
                # Try common pagination patterns
                sep   = "&" if "?" in cat_url else "?"
                page_url = f"{cat_url}{sep}page={page_num}"

            r = _safe_get(session, page_url, timeout=timeout)
            if not r or r.status_code != 200:
                break

            page_products = _extract_product_links(r.text, base)
            if not page_products:
                break

            before = len(collected)
            collected.update(page_products)
            pages_crawled += 1

            # Stop if no new URLs were added (we've seen all of these already)
            if len(collected) == before and page_num > 1:
                break

            # Check /page/N/ style pagination exists in the page
            if page_num == 1 and _max_page_number(r.text) == 1:
                break  # single page category

            # Stop if we've collected enough
            if target_count > 0 and len(collected) >= target_count:
                break

        if target_count > 0 and len(collected) >= target_count:
            break

    # Merge with any partial sitemap results
    for u in sitemap_urls:
        collected.add(u)

    result = list(dict.fromkeys(collected))   # deduplicated, order preserved
    if result:
        return result, f"html-scan({pages_crawled} pages)"
    return [], "no-urls-found"


def _execute_url_collection(website_id: int, db: Session) -> None:
    """
    Phase 2 full lifecycle:
      load website → collect URLs → write to file → create log → update website
    """
    from app.models.website import Website
    from app.models.crawl_log import CrawlLog

    website = db.query(Website).filter(Website.id == website_id).first()
    if not website:
        return

    website.url_collection_status = "running"
    db.commit()

    log = CrawlLog(
        website_id=website_id,
        task_id=f"urlcollect-{website_id}-{int(datetime.now().timestamp())}",
        status="running",
        log_type="url_collection",
    )
    db.add(log)
    db.commit()

    try:
        target = website.discovered_count or 0
        urls, method = _collect_urls(website.url, target_count=target)
        url_count = len(urls)
        logger.info(f"URL collection: website={website_id} count={url_count} method={method}")

        # Write URLs to temp file for spider to consume
        fpath = url_file_path(website_id)
        if urls:
            with open(fpath, "w", encoding="utf-8") as f:
                f.write("\n".join(urls))
            logger.info(f"Wrote {url_count} URLs to {fpath}")

        is_blocked = "blocked" in method or "site-up-but-blocked" in method
        is_unreachable = method == "site-unreachable"
        is_no_urls = method == "no-urls-found"
        log.status = "success" if urls else "error"
        log.machines_found = url_count

        if is_unreachable:
            status_note = "Site could not be reached (connection refused, timeout, or DNS failure)"
        elif is_blocked and not urls:
            status_note = "Site is online but blocking automated requests (WAF/Cloudflare) — try training rules or manual crawl"
        elif is_no_urls:
            status_note = (
                "No product URLs detected. Site may use JavaScript rendering or "
                "a non-standard API. Consider adding a dedicated spider."
            )
        else:
            status_note = None

        coverage_pct = f"{int(url_count/target*100)}%" if target > 0 else "?"
        # Determine the human-readable method name for the log
        if method.startswith("supabase:"):
            method_label = f"Supabase API ({method.split(':', 1)[1]} table)"
        elif method.startswith("json-api:"):
            method_label = f"JSON API endpoint ({method.split(':', 1)[1]})"
        elif method.startswith("js-data"):
            method_label = "JavaScript data extraction"
        elif method.startswith("html-scan"):
            method_label = f"HTML pagination scan"
        elif method.startswith("corelmachine"):
            method_label = "CoreMachine JSON API"
        else:
            method_label = method

        log.log_output = (
            f"URL collection method: {method_label}\n"
            f"URLs collected: {url_count}\n"
            f"Target count:   {target or 'unknown'}\n"
            f"Coverage:       {coverage_pct}\n"
        )
        log.error_details = status_note if not urls else None
        log.finished_at = datetime.now(timezone.utc)

        website.urls_collected          = url_count if urls else None
        website.url_collection_status   = "done" if urls else "error"

    except Exception as exc:
        logger.exception(f"URL collection failed: website={website_id} error={exc}")
        log.status = "error"
        log.error_details = str(exc)
        log.finished_at = datetime.now(timezone.utc)
        website.url_collection_status = "error"

    db.commit()


def run_url_collection_direct(website_id: int):
    """Run Phase 2 URL collection without Celery."""
    db = get_sync_db()
    try:
        _execute_url_collection(website_id, db)
    except Exception as exc:
        logger.exception(f"run_url_collection_direct failed: website={website_id} error={exc}")
    finally:
        db.close()


# ─────────────────────────────────────────────────────────────────────────────
# Core crawl runner (shared by direct and celery)
# ─────────────────────────────────────────────────────────────────────────────

def _execute_crawl(website_id: int, db: Session) -> None:
    """
    Full crawl lifecycle:
      preflight → load rules → create log → run scrapy → parse output → update DB
    """
    from app.models.website import Website
    from app.models.crawl_log import CrawlLog
    from app.models.machine import Machine
    from sqlalchemy import func

    website = db.query(Website).filter(Website.id == website_id).first()
    if not website:
        logger.error(f"Website {website_id} not found")
        return

    # Load full training rules (all fields) for this website
    training_rules_json = _load_training_rules(db, website_id)

    # ── Pre-flight ─────────────────────────────────────────────────────────
    ok, preflight_msg = _preflight_check()
    logger.info(f"Pre-flight: {preflight_msg}")

    crawl_log = CrawlLog(
        website_id=website_id,
        task_id=f"direct-{website_id}-{int(datetime.now().timestamp())}",
        status="running",
    )
    db.add(crawl_log)
    db.commit()

    website.crawl_status = "running"
    db.commit()

    if not ok:
        crawl_log.status = "error"
        crawl_log.error_details = preflight_msg
        crawl_log.log_output = preflight_msg
        crawl_log.finished_at = datetime.now(timezone.utc)
        website.crawl_status = "error"
        db.commit()
        logger.error(f"Pre-flight failed for website {website_id}: {preflight_msg}")
        return

    # ── Run scrapy ─────────────────────────────────────────────────────────
    logger.info(f"Crawl start: website={website_id} url={website.url} spider=generic")

    result = _run_scrapy(website_id, website.url, crawl_log.id, training_rules_json)
    combined = (result.stdout or "") + (result.stderr or "")

    logger.info(
        f"Scrapy done: website={website_id} returncode={result.returncode} "
        f"output={len(combined)} chars"
    )

    # ── Parse results ──────────────────────────────────────────────────────
    stats = _parse_scrapy_stats(combined)
    machines_found   = stats.get("item_scraped_count", 0)
    machines_dropped = stats.get("item_dropped_count", 0)
    requests_made    = stats.get("downloader/request_count", 0)

    status = "success" if result.returncode == 0 else "error"
    inline_errors = _extract_error_summary(combined)

    if status == "error":
        error_summary = inline_errors or f"[returncode={result.returncode}]\n{combined[-1500:]}"
    elif machines_found == 0:
        stats_snippet = (
            f"requests={requests_made} "
            f"items={machines_found} "
            f"dropped={machines_dropped} "
            f"errors={stats.get('spider_exceptions_count', 0)}"
        )
        if inline_errors:
            error_summary = f"[0 machines — errors found]\n{stats_snippet}\n\n{inline_errors}"
        else:
            error_summary = f"[0 machines — no errors logged]\n{stats_snippet}\n\n{combined[-1200:]}"
    else:
        error_summary = None

    # ── Update crawl log ───────────────────────────────────────────────────
    crawl_log.status          = status
    crawl_log.machines_found  = machines_found
    crawl_log.machines_new    = machines_found       # pipeline sets real new/updated counts
    crawl_log.machines_skipped = machines_dropped
    crawl_log.errors_count    = stats.get("spider_exceptions_count", 0)
    crawl_log.error_details   = error_summary
    crawl_log.log_output      = combined[-5000:]
    crawl_log.finished_at     = datetime.now(timezone.utc)
    db.commit()

    # ── Mark machines not seen in this crawl as inactive ───────────────────
    # Any machine from this website whose last_crawled_at is older than
    # when this crawl started is no longer listed on the supplier site.
    if status == "success" and machines_found > 0:
        stale_updated = db.query(Machine).filter(
            Machine.website_id == website_id,
            Machine.is_active == True,
            (Machine.last_crawled_at == None) | (Machine.last_crawled_at < crawl_log.started_at),
        ).update({"is_active": False}, synchronize_session=False)
        if stale_updated:
            logger.info(f"Marked {stale_updated} stale machines inactive for website={website_id}")
        db.commit()

    # ── Update website ─────────────────────────────────────────────────────
    count = db.query(func.count(Machine.id)).filter(
        Machine.website_id == website_id, Machine.is_active == True
    ).scalar()
    website.machine_count   = count or 0
    website.crawl_status    = status
    website.last_crawled_at = crawl_log.finished_at
    db.commit()

    logger.info(
        f"Crawl complete: website={website_id} status={status} "
        f"scraped={machines_found} dropped={machines_dropped} total_in_db={website.machine_count}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def run_crawl_direct(website_id: int):
    """Direct crawl without Celery — called in a background thread."""
    db = get_sync_db()
    try:
        _execute_crawl(website_id, db)
    except Exception as exc:
        logger.exception(f"run_crawl_direct failed: website={website_id} error={exc}")
    finally:
        db.close()


@celery_app.task(bind=True, name="tasks.crawl_tasks.crawl_website_task")
def crawl_website_task(self, website_id: int):
    db = get_sync_db()
    try:
        _execute_crawl(website_id, db)
    except Exception as exc:
        logger.exception(f"Celery crawl failed: website={website_id} error={exc}")
        raise
    finally:
        db.close()


@celery_app.task(name="tasks.crawl_tasks.crawl_all_websites_task")
def crawl_all_websites_task():
    """Queue every active+enabled website for crawling (immediate, no schedule)."""
    from app.models.website import Website
    db = get_sync_db()
    try:
        websites = (
            db.query(Website)
            .filter(Website.is_active == True, Website.crawl_enabled == True)
            .all()
        )
        for site in websites:
            crawl_website_task.delay(site.id)
        return {"queued": len(websites)}
    finally:
        db.close()
