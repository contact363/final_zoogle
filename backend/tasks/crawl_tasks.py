"""
Celery tasks that launch Scrapy crawls in a subprocess and record logs.
run_crawl_direct() is a Redis-free fallback for when Celery is unavailable.
"""
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


# backend/ directory — needed as PYTHONPATH for the scrapy subprocess
_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_CRAWLER_DIR = os.path.join(_BACKEND_DIR, "crawler")


def _build_subprocess_env() -> dict:
    """Put backend/ on PYTHONPATH so 'app' is importable inside the spider."""
    env = os.environ.copy()
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = _BACKEND_DIR + (os.pathsep + existing if existing else "")
    return env


def _run_scrapy(website_id: int, start_url: str, crawl_log_id: int) -> subprocess.CompletedProcess:
    """
    Run scrapy in a subprocess.
    NO --logfile: capture stdout+stderr directly so we can see real errors.
    Scrapy writes its stats summary to stdout at the end.
    """
    return subprocess.run(
        [
            sys.executable, "-m", "scrapy", "crawl", "generic",
            "-a", f"website_id={website_id}",
            "-a", f"start_url={start_url}",
            "-a", f"crawl_log_id={crawl_log_id}",
            "--set", "LOG_LEVEL=INFO",
            "--set", f"CLOSESPIDER_ITEMCOUNT=5000",
        ],
        cwd=_CRAWLER_DIR,
        capture_output=True,
        text=True,
        timeout=3600,
        env=_build_subprocess_env(),
    )


def _parse_scrapy_stats(output: str) -> dict:
    """
    Parse Scrapy's final stats block from combined stdout+stderr.

    Scrapy prints something like:
      {'downloader/request_count': 42, 'item_scraped_count': 17, ...}
    """
    stats = {}

    # Try to find the stats dict block (scrapy dumps it as a Python dict literal)
    stats_block_match = re.search(
        r"Dumping Scrapy stats.*?(\{.*?\})",
        output,
        re.DOTALL,
    )
    if stats_block_match:
        block = stats_block_match.group(1)
        # Extract individual key-value pairs
        for m in re.finditer(r"'([\w/]+)':\s*(\d+)", block):
            key, val = m.group(1), int(m.group(2))
            stats[key] = val

    # Fallback: scan line by line
    if not stats:
        for line in output.split("\n"):
            for key in ("item_scraped_count", "spider_exceptions_count",
                        "downloader/response_count", "item_dropped_count"):
                if key in line:
                    m = re.search(rf"'{key}':\s*(\d+)", line)
                    if m:
                        stats[key] = int(m.group(1))

    return stats


def _get_error_summary(result: subprocess.CompletedProcess) -> str | None:
    """Extract the most useful error info from scrapy output."""
    combined = (result.stdout or "") + (result.stderr or "")
    if not combined:
        return "No output captured from scrapy process"

    # Look for Python tracebacks
    lines = combined.split("\n")
    error_lines = []
    in_traceback = False

    for line in lines:
        if "Traceback (most recent call last)" in line:
            in_traceback = True
        if in_traceback:
            error_lines.append(line)
            if len(error_lines) > 30:
                break
        elif any(marker in line for marker in ["ERROR", "CRITICAL", "Error:", "Exception:"]):
            error_lines.append(line)

    if error_lines:
        return "\n".join(error_lines)[-3000:]

    # Return last 2000 chars as fallback
    return combined[-2000:] or None


# ─────────────────────────────────────────────────────────────────────────────
# Direct crawl (no Celery — runs in background thread)
# ─────────────────────────────────────────────────────────────────────────────

def run_crawl_direct(website_id: int):
    """
    Run a crawl without Celery/Redis.
    Called in a background thread from the admin endpoint.
    Runs sequentially — one website at a time to avoid memory issues.
    """
    from app.models.website import Website
    from app.models.crawl_log import CrawlLog

    db = get_sync_db()
    crawl_log = None
    try:
        website = db.query(Website).filter(Website.id == website_id).first()
        if not website:
            logger.error(f"Website {website_id} not found")
            return

        crawl_log = CrawlLog(
            website_id=website_id,
            task_id=f"direct-{website_id}-{int(datetime.now().timestamp())}",
            status="running",
        )
        db.add(crawl_log)
        db.commit()

        website.crawl_status = "running"
        db.commit()

        logger.info(f"Starting crawl: website={website_id} url={website.url}")

        result = _run_scrapy(website_id, website.url, crawl_log.id)
        combined_output = (result.stdout or "") + (result.stderr or "")

        logger.info(
            f"Scrapy finished: website={website_id} returncode={result.returncode} "
            f"output_len={len(combined_output)}"
        )
        # Log first 500 chars of output for debugging
        if combined_output:
            logger.debug(f"Scrapy output preview:\n{combined_output[:500]}")

        stats = _parse_scrapy_stats(combined_output)

        crawl_log.status = "success" if result.returncode == 0 else "error"
        crawl_log.machines_found = stats.get("item_scraped_count", 0)
        crawl_log.machines_new = stats.get("item_scraped_count", 0)
        crawl_log.errors_count = stats.get("spider_exceptions_count", 0)
        crawl_log.finished_at = datetime.now(timezone.utc)

        if result.returncode != 0:
            crawl_log.error_details = _get_error_summary(result)
        elif crawl_log.machines_found == 0:
            # Success but 0 machines — store output snippet for debugging
            crawl_log.error_details = f"[0 machines found]\n{combined_output[-1000:]}"

        db.commit()

        # Update website machine_count
        from sqlalchemy import func
        from app.models.machine import Machine
        count = db.query(func.count(Machine.id)).filter(Machine.website_id == website_id).scalar()
        website.machine_count = count or 0
        website.crawl_status = crawl_log.status
        website.last_crawled_at = crawl_log.finished_at
        db.commit()

        logger.info(
            f"Crawl complete: website={website_id} status={crawl_log.status} "
            f"machines_found={crawl_log.machines_found} total_in_db={website.machine_count}"
        )

    except subprocess.TimeoutExpired:
        logger.error(f"Crawl timed out: website={website_id}")
        if crawl_log:
            crawl_log.status = "error"
            crawl_log.error_details = "Crawl timed out after 1 hour"
            crawl_log.finished_at = datetime.now(timezone.utc)
            db.commit()
    except Exception as exc:
        logger.exception(f"Crawl error: website={website_id} error={exc}")
        if crawl_log:
            try:
                crawl_log.status = "error"
                crawl_log.error_details = str(exc)[:1000]
                crawl_log.finished_at = datetime.now(timezone.utc)
                db.commit()
            except Exception:
                pass
    finally:
        db.close()


# ─────────────────────────────────────────────────────────────────────────────
# Celery tasks
# ─────────────────────────────────────────────────────────────────────────────

@celery_app.task(bind=True, name="tasks.crawl_tasks.crawl_website_task")
def crawl_website_task(self, website_id: int):
    """Crawl a single website by ID (via Celery worker)."""
    from app.models.website import Website
    from app.models.crawl_log import CrawlLog

    db = get_sync_db()
    crawl_log = None
    try:
        website = db.query(Website).filter(Website.id == website_id).first()
        if not website:
            logger.error(f"Website {website_id} not found")
            return

        crawl_log = CrawlLog(
            website_id=website_id,
            task_id=self.request.id,
            status="running",
        )
        db.add(crawl_log)
        db.commit()

        website.crawl_status = "running"
        db.commit()

        result = _run_scrapy(website_id, website.url, crawl_log.id)
        combined_output = (result.stdout or "") + (result.stderr or "")
        stats = _parse_scrapy_stats(combined_output)

        crawl_log.status = "success" if result.returncode == 0 else "error"
        crawl_log.machines_found = stats.get("item_scraped_count", 0)
        crawl_log.machines_new = stats.get("item_scraped_count", 0)
        crawl_log.errors_count = stats.get("spider_exceptions_count", 0)
        crawl_log.finished_at = datetime.now(timezone.utc)

        if result.returncode != 0:
            crawl_log.error_details = _get_error_summary(result)
        elif crawl_log.machines_found == 0:
            crawl_log.error_details = f"[0 machines found]\n{combined_output[-1000:]}"

        db.commit()

        from sqlalchemy import func
        from app.models.machine import Machine
        count = db.query(func.count(Machine.id)).filter(Machine.website_id == website_id).scalar()
        website.machine_count = count or 0
        website.crawl_status = crawl_log.status
        website.last_crawled_at = crawl_log.finished_at
        db.commit()

        logger.info(
            f"Crawl finished: website={website_id} status={crawl_log.status} "
            f"machines={crawl_log.machines_found}"
        )

    except subprocess.TimeoutExpired:
        if crawl_log:
            crawl_log.status = "error"
            crawl_log.error_details = "Crawl timed out after 1 hour"
            crawl_log.finished_at = datetime.now(timezone.utc)
            db.commit()
    except Exception as exc:
        logger.exception(f"Crawl task error: {exc}")
        raise
    finally:
        db.close()


@celery_app.task(name="tasks.crawl_tasks.crawl_all_websites_task")
def crawl_all_websites_task():
    """Queue crawl tasks for all active websites (sequential via Celery)."""
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
            logger.info(f"Queued crawl for website: {site.id} ({site.url})")
        return {"queued": len(websites)}
    finally:
        db.close()
