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

    Uses a dedicated spider for known sites (zatpatmachines, corelmachine),
    falls back to the generic spider for all others.
    No CLOSESPIDER_ITEMCOUNT cap — each spider controls its own limit.
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
    "/machine/", "/product/", "/equipment/", "/item/", "/listing/",
    "/crane/", "/lathe/", "/mill/", "/grinder/", "/press/", "/robot/",
    "/hoist/", "/compressor/", "/conveyor/", "/machinetools/",
    "/used-", "/second-hand/", "/gebraucht/", "/occasion/",
)

_LISTING_PATHS = (
    "/machines", "/products", "/equipment", "/machine-tools", "/inventory",
    "/catalog", "/used", "/stock", "/shop", "/listings", "/for-sale",
    "/used-machines", "/used-equipment", "/cranes", "/lathes", "/mills",
    "/hoists", "/compressors", "/robots", "/grinders", "/presses",
    "/maschinen", "/occasions", "/gebrauchtmaschinen",
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


def _discover_count(start_url: str) -> tuple[int, str]:
    """
    Quick HTTP-based machine count. No Scrapy, no imports from zoogle_crawler.

    Tries in order (fastest → slowest):
      1. Shopify   /products/count.json
      2. WooCommerce /wp-json/wc/v3/products  (X-WP-Total header)
      3. Supabase  PostgREST count=exact      (Content-Range header)
      4. JSON API  /api/subcategory/all etc.  (sum products across subcategories)
      5. Sitemap   /sitemap.xml               (count product <loc> entries)
      6. HTML scan homepage + listing pages  (collect product URLs + pagination)

    Returns (count, method_name). count = -1 only if site unreachable.
    Estimated counts are returned as positive integers with method="estimated:...".
    """
    try:
        import requests as _req
    except ImportError:
        return -1, "requests-unavailable"

    base = start_url.rstrip("/")
    parsed = urlparse(start_url)
    domain = parsed.netloc.lower()
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; Zooglebot/1.0)",
        "Accept": "text/html,application/json,*/*",
    }
    timeout = 12

    # ── 1. Shopify ───────────────────────────────────────────────────────────
    try:
        r = _req.get(f"{base}/products/count.json", headers=headers, timeout=timeout)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, dict) and "count" in data:
                return int(data["count"]), "shopify"
    except Exception:
        pass

    # ── 2. WooCommerce ───────────────────────────────────────────────────────
    try:
        r = _req.get(
            f"{base}/wp-json/wc/v3/products?per_page=1",
            headers=headers, timeout=timeout,
        )
        if r.status_code == 200:
            total = r.headers.get("X-WP-Total")
            if total and int(total) > 0:
                return int(total), "woocommerce"
    except Exception:
        pass

    # ── 3. Supabase (inline credentials — no zoogle_crawler import needed) ───
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
        sb_hdrs = {
            "apikey": _ZATPAT_SUPABASE_KEY,
            "Authorization": f"Bearer {_ZATPAT_SUPABASE_KEY}",
            "Prefer": "count=exact",
            "Range": "0-0",
        }
        for table in sb_tables:
            try:
                r = _req.get(
                    f"{_ZATPAT_SUPABASE_URL}/rest/v1/{table}?select=id",
                    headers=sb_hdrs, timeout=timeout,
                )
                if r.status_code in (200, 206):
                    cr = r.headers.get("Content-Range", "")
                    m = re.search(r"/(\d+)$", cr)
                    if m and int(m.group(1)) > 0:
                        return int(m.group(1)), f"supabase:{table}"
            except Exception:
                continue

    # ── 4. JSON API probe — sum product counts across subcategories ───────────
    #    Handles sites like corelmachine.com that expose /api/subcategory/all
    for api_path in _JSON_API_PATHS:
        try:
            r = _req.get(
                f"{base}{api_path}",
                headers={**headers, "Accept": "application/json"},
                timeout=timeout,
            )
            if r.status_code != 200:
                continue
            body = r.text.strip()
            if not body or body[0] not in ("[", "{"):
                continue
            data = r.json()

            # Unwrap envelope
            if isinstance(data, dict):
                for key in ("data", "results", "items", "products", "machines"):
                    if isinstance(data.get(key), list):
                        data = data[key]
                        break

            if not isinstance(data, list) or not data:
                continue

            first = data[0] if data else {}
            # If it's a subcategory list (has url/slug but no price), fetch each subcategory
            is_subcategory = (
                isinstance(first, dict)
                and ("url" in first or "slug" in first)
                and "price" not in first
                and len(data) <= 200
            )
            if is_subcategory:
                logger.info(f"[discovery] JSON subcategory list at {api_path}: {len(data)} categories")
                total = 0
                sampled = 0
                for entry in data:
                    slug = entry.get("url") or entry.get("slug") or entry.get("id")
                    if not slug:
                        continue
                    try:
                        pr = _req.get(
                            f"{base}/api/product/{slug}",
                            headers={**headers, "Accept": "application/json"},
                            timeout=timeout,
                        )
                        if pr.status_code == 200:
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
                    # Extrapolate if we couldn't fetch all subcategories
                    if sampled < len(data):
                        total = int(total / sampled * len(data))
                        return total, f"json-api:subcategories(estimated,{sampled}/{len(data)} sampled)"
                    return total, f"json-api:subcategories({sampled} categories)"
            else:
                # It's a direct product list
                count = len(data)
                if count > 0:
                    return count, f"json-api:{api_path}"
        except Exception:
            continue

    # ── 5. Sitemap — count product <loc> entries ─────────────────────────────
    #    Inline URL filter — no zoogle_crawler import needed
    try:
        for sitemap_path in ("/sitemap.xml", "/sitemap_index.xml",
                             "/product-sitemap.xml", "/page-sitemap.xml"):
            try:
                r = _req.get(f"{base}{sitemap_path}", headers=headers, timeout=timeout)
            except Exception:
                continue
            if r.status_code != 200 or "<loc>" not in r.text:
                continue

            # Handle sitemap index — recurse into first product sitemap
            child_sitemaps = re.findall(r"<sitemap>\s*<loc>(.*?)</loc>", r.text, re.DOTALL)
            if child_sitemaps:
                for child_url in child_sitemaps[:3]:
                    try:
                        cr = _req.get(child_url.strip(), headers=headers, timeout=timeout)
                        if cr.status_code == 200 and "<loc>" in cr.text:
                            urls = re.findall(r"<loc>(.*?)</loc>", cr.text, re.DOTALL)
                            count = sum(1 for u in urls if _is_product_url(u.strip()))
                            if count > 0:
                                return count, "sitemap"
                    except Exception:
                        continue

            urls = re.findall(r"<loc>(.*?)</loc>", r.text, re.DOTALL)
            count = sum(1 for u in urls if _is_product_url(u.strip()))
            if count > 0:
                return count, "sitemap"
    except Exception:
        pass

    # ── 6. HTML scan — homepage + listing pages + pagination estimation ───────
    all_product_urls: set = set()
    homepage_html = ""

    try:
        r = _req.get(start_url, headers=headers, timeout=timeout)
        if r.status_code != 200:
            return -1, "site-unreachable"
        homepage_html = r.text
    except Exception:
        return -1, "site-unreachable"

    # 6a. Collect product URLs directly on homepage
    all_product_urls.update(_extract_product_links(homepage_html, base))

    # 6b. Find category/listing page links on homepage
    category_urls: list = []
    for href in re.findall(r'href=["\']([^"\'#][^"\']*)["\']', homepage_html):
        try:
            full = href if href.startswith("http") else f"{base.rstrip('/')}/{href.lstrip('/')}"
            if urlparse(full).netloc != parsed.netloc:
                continue
            path = urlparse(full).path.lower().rstrip("/")
            if any(path == lp or path.endswith(lp) for lp in _LISTING_PATHS):
                if full not in category_urls:
                    category_urls.append(full)
        except Exception:
            pass

    # Also probe standard listing paths that might not be linked from homepage
    for lp in _LISTING_PATHS[:10]:
        url = f"{base}{lp}"
        if url not in category_urls:
            category_urls.append(url)

    # 6c. Crawl category pages — collect product links + detect pagination
    pages_checked = 0
    total_estimated = 0
    methods_used = []

    for cat_url in category_urls[:8]:
        try:
            r = _req.get(cat_url, headers=headers, timeout=timeout)
            if r.status_code != 200:
                continue
            cat_html = r.text
            page_products = _extract_product_links(cat_html, base)
            if not page_products:
                continue

            pages_checked += 1
            all_product_urls.update(page_products)
            products_per_page = len(page_products)

            # Detect pagination to estimate total for this category
            max_page = _max_page_number(cat_html)
            if max_page > 1:
                estimated = products_per_page * max_page
                total_estimated += estimated
                methods_used.append(f"{urlparse(cat_url).path}(~{estimated})")
                logger.info(f"[discovery] {cat_url}: {products_per_page}/page × {max_page} pages = ~{estimated}")
            else:
                total_estimated += products_per_page
                methods_used.append(f"{urlparse(cat_url).path}({products_per_page})")
        except Exception:
            continue

    direct_count = len(all_product_urls)
    final_count = max(direct_count, total_estimated)

    if final_count > 0:
        method_detail = ", ".join(methods_used[:3]) if methods_used else "url-scan"
        is_estimated = total_estimated > direct_count
        method = f"estimated:html-scan({method_detail})" if is_estimated else f"html-scan({method_detail})"
        return final_count, method

    return -1, "unknown"


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

        site_unreachable = count == -1 and method in ("site-unreachable", "requests-unavailable", "unknown")
        is_estimated = "estimated" in method

        if count >= 0:
            count_label = f"{count} (estimated)" if is_estimated else str(count)
            log.status = "success"
            log.machines_found = count
            log.log_output = (
                f"Discovery method: {method}\n"
                f"Machines found on site: {count_label}\n"
            )
            log.error_details = None
            website.discovered_count = count
            website.discovery_status = "done"
        else:
            log.status = "error"
            log.machines_found = 0
            log.log_output = f"Discovery method: {method}\nMachines found on site: unknown"
            log.error_details = (
                "Website could not be reached" if site_unreachable
                else f"Could not determine count (method={method})"
            )
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


def run_discovery_direct(website_id: int):
    """Run Phase 1 discovery without Celery — called in a background thread."""
    db = get_sync_db()
    try:
        _execute_discovery(website_id, db)
    except Exception as exc:
        logger.exception(f"run_discovery_direct failed: website={website_id} error={exc}")
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
