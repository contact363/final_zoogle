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

def _discover_count(start_url: str) -> tuple[int, str]:
    """
    Quick HTTP-based machine count — no Scrapy needed.

    Tries in order:
      1. Shopify  /products/count.json           → {"count": N}
      2. WooCommerce /wp-json/wc/v3/products     → X-WP-Total header
      3. Supabase PostgREST HEAD with count=exact → Content-Range: 0-0/N
      4. Sitemap.xml — count product-looking URLs

    Returns (count, method_name).  count = -1 means unknown.
    """
    try:
        import requests as _req
    except ImportError:
        return -1, "requests-unavailable"

    base = start_url.rstrip("/")
    headers = {"User-Agent": "Zooglebot/1.0"}
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
            if total:
                return int(total), "woocommerce"
    except Exception:
        pass

    # ── 3. Supabase (zatpatmachines / zatpatestimate) ────────────────────────
    domain = urlparse(start_url).netloc.lower()
    if "zatpat" in domain:
        from zoogle_crawler.spiders.zatpatmachines_spider import (
            _ZATPAT_SUPABASE_URL, _ZATPAT_SUPABASE_KEY, _TABLE_PROBES,
        )
        supabase_url = _ZATPAT_SUPABASE_URL
        anon_key     = _ZATPAT_SUPABASE_KEY
        sb_headers   = {
            "apikey": anon_key,
            "Authorization": f"Bearer {anon_key}",
            "Prefer": "count=exact",
            "Range": "0-0",
        }
        for table in _TABLE_PROBES:
            try:
                r = _req.get(
                    f"{supabase_url}/rest/v1/{table}?select=id",
                    headers=sb_headers, timeout=timeout,
                )
                if r.status_code in (200, 206):
                    # Content-Range: 0-0/5003
                    cr = r.headers.get("Content-Range", "")
                    m = re.search(r"/(\d+)$", cr)
                    if m:
                        return int(m.group(1)), f"supabase:{table}"
            except Exception:
                continue

    # ── 4. Sitemap — count product URLs ──────────────────────────────────────
    try:
        for sitemap_path in ("/sitemap.xml", "/sitemap_index.xml", "/product-sitemap.xml"):
            r = _req.get(f"{base}{sitemap_path}", headers=headers, timeout=timeout)
            if r.status_code == 200 and "<loc>" in r.text:
                from zoogle_crawler.page_analyzer import is_detail_url
                urls = re.findall(r"<loc>(.*?)</loc>", r.text)
                count = sum(1 for u in urls if is_detail_url(u.strip()))
                if count > 0:
                    return count, "sitemap"
                break
    except Exception:
        pass

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

        log.status = "success" if count >= 0 else "error"
        log.machines_found = max(count, 0)
        log.log_output = f"Discovery method: {method}\nMachines found on site: {count if count >= 0 else 'unknown'}"
        log.error_details = None if count >= 0 else f"Could not determine count (method={method})"
        log.finished_at = datetime.now(timezone.utc)

        website.discovered_count = count if count >= 0 else None
        website.discovery_status = "done" if count >= 0 else "error"

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
