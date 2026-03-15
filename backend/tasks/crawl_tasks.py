"""
Celery tasks that launch Scrapy crawls in a subprocess and record logs.
run_crawl_direct() is a Redis-free fallback for when Celery is unavailable.

Self-healing approach:
  1. Pre-flight check: run 'scrapy list' to verify spider loads before crawling
  2. Full scrapy output is stored in crawl_log.log_output on every run
  3. Errors are parsed from output and stored in crawl_log.error_details
  4. machine_count on Website is updated after every crawl
"""
import json
import subprocess
import sys
import os
import re
from datetime import datetime, timezone

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
        # Accept any output that lists at least one spider (generic or dedicated)
        if result.returncode != 0 or not result.stdout.strip():
            return False, f"Spider pre-flight FAILED (returncode={result.returncode}):\n{combined}"
        return True, "Spider loaded OK"
    except Exception as e:
        return False, f"Pre-flight exception: {e}"


# ─────────────────────────────────────────────────────────────────────────────
# Run scrapy subprocess
# ─────────────────────────────────────────────────────────────────────────────

def _select_spider(start_url: str) -> str:
    """
    Pick the best Scrapy spider for a given start URL.
    Dedicated spiders take priority over the generic one because they use
    site-specific extraction logic (e.g. Playwright-aware CSR handling).
    """
    from urllib.parse import urlparse
    domain = urlparse(start_url).netloc.lower().lstrip("www.")
    _DEDICATED = {
        "corelmachine.com":   "corelmachine",
        "zatpatmachines.com": "zatpatmachines",
    }
    return _DEDICATED.get(domain, "generic")


def _run_scrapy(
    website_id: int,
    start_url: str,
    crawl_log_id: int,
    training_rules_json: str | None = None,
) -> subprocess.CompletedProcess:
    """
    Run scrapy — capture all output directly (no --logfile).
    Full stdout+stderr captured so we can store it and parse stats from it.
    Passes training_rules as a JSON string if the website has been trained.
    """
    spider_name = _select_spider(start_url)
    cmd = [
        sys.executable, "-m", "scrapy", "crawl", spider_name,
        "-a", f"website_id={website_id}",
        "-a", f"start_url={start_url}",
        "-a", f"crawl_log_id={crawl_log_id}",
        "--set", "LOG_LEVEL=INFO",
        "--set", "CLOSESPIDER_ITEMCOUNT=5000",
    ]
    if training_rules_json:
        cmd += ["-a", f"training_rules={training_rules_json}"]
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

    # Scrapy dumps: {'item_scraped_count': 42, 'downloader/request_count': 100, ...}
    block_match = re.search(r"Dumping Scrapy stats.*?(\{.*?\})", output, re.DOTALL)
    if block_match:
        block = block_match.group(1)
        for m in re.finditer(r"'([\w/]+)':\s*(\d+)", block):
            stats[m.group(1)] = int(m.group(2))

    # Fallback: scan lines
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
        if any(m in line for m in ["Traceback", "ERROR", "CRITICAL", "Error:", "Exception:", "ImportError", "ModuleNotFoundError"]):
            # Grab this line + next 5 lines for context
            error_lines.extend(lines[i:i+6])

    if error_lines:
        return "\n".join(dict.fromkeys(error_lines))[:3000]  # deduplicate + cap

    return None


# ─────────────────────────────────────────────────────────────────────────────
# Core crawl runner (shared by direct and celery)
# ─────────────────────────────────────────────────────────────────────────────

def _execute_crawl(website_id: int, db: Session) -> None:
    """
    Full crawl lifecycle:
      preflight → create log → run scrapy → parse output → update DB
    """
    from app.models.website import Website
    from app.models.crawl_log import CrawlLog
    from app.models.machine import Machine
    from app.models.training_rules import WebsiteTrainingRules
    from sqlalchemy import func

    website = db.query(Website).filter(Website.id == website_id).first()
    if not website:
        logger.error(f"Website {website_id} not found")
        return

    # Load training rules (if the admin has configured them for this website)
    rules_row = db.query(WebsiteTrainingRules).filter(
        WebsiteTrainingRules.website_id == website_id
    ).first()
    training_rules_json: str | None = None
    if rules_row:
        rules_dict = {
            k: getattr(rules_row, k)
            for k in (
                "listing_selector", "title_selector", "url_selector",
                "description_selector", "image_selector", "price_selector",
                "category_selector", "pagination_selector",
            )
            if getattr(rules_row, k)
        }
        if rules_dict:
            training_rules_json = json.dumps(rules_dict)
            logger.info(f"Training rules loaded for website {website_id}: {list(rules_dict.keys())}")
        else:
            logger.info(f"Training rules row exists for website {website_id} but all selectors are empty — using auto-discovery")
    else:
        logger.info(f"No training rules for website {website_id} — using auto-discovery")

    # ── Pre-flight ────────────────────────────────────────────────────────────
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
        # Spider can't even load — fail immediately with the real error
        crawl_log.status = "error"
        crawl_log.error_details = preflight_msg
        crawl_log.log_output = preflight_msg
        crawl_log.finished_at = datetime.now(timezone.utc)
        website.crawl_status = "error"
        db.commit()
        logger.error(f"Pre-flight failed for website {website_id}: {preflight_msg}")
        return

    # ── Run scrapy ────────────────────────────────────────────────────────────
    logger.info(f"Crawl start: website={website_id} url={website.url}")

    result = _run_scrapy(website_id, website.url, crawl_log.id, training_rules_json)
    combined = (result.stdout or "") + (result.stderr or "")

    logger.info(
        f"Scrapy done: website={website_id} returncode={result.returncode} "
        f"output={len(combined)} chars"
    )

    # ── Parse results ─────────────────────────────────────────────────────────
    stats = _parse_scrapy_stats(combined)
    machines_found = stats.get("item_scraped_count", 0)
    requests_made  = stats.get("downloader/request_count", 0)

    # Determine status
    status = "success" if result.returncode == 0 else "error"

    # Always extract ERROR/WARNING lines from the full output
    inline_errors = _extract_error_summary(combined)

    if status == "error":
        error_summary = inline_errors or f"[returncode={result.returncode}]\n{combined[-1500:]}"
    elif machines_found == 0:
        # Show extracted errors if any, plus a stats summary for debugging
        stats_snippet = (
            f"requests={requests_made} "
            f"items={machines_found} "
            f"dropped={stats.get('item_dropped_count', 0)} "
            f"errors={stats.get('spider_exceptions_count', 0)}"
        )
        if inline_errors:
            error_summary = f"[0 machines — errors found]\n{stats_snippet}\n\n{inline_errors}"
        else:
            error_summary = f"[0 machines — no errors logged]\n{stats_snippet}\n\n{combined[-1200:]}"
    else:
        error_summary = None

    # ── Update crawl log ──────────────────────────────────────────────────────
    crawl_log.status = status
    crawl_log.machines_found = machines_found
    crawl_log.machines_new = machines_found
    crawl_log.errors_count = stats.get("spider_exceptions_count", 0)
    crawl_log.error_details = error_summary
    crawl_log.log_output = combined[-5000:]   # store last 5000 chars of full output
    crawl_log.finished_at = datetime.now(timezone.utc)
    db.commit()

    # ── Update website ────────────────────────────────────────────────────────
    count = db.query(func.count(Machine.id)).filter(Machine.website_id == website_id).scalar()
    website.machine_count = count or 0
    website.crawl_status = status
    website.last_crawled_at = crawl_log.finished_at
    db.commit()

    logger.info(
        f"Crawl complete: website={website_id} status={status} "
        f"scraped={machines_found} total_in_db={website.machine_count}"
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
