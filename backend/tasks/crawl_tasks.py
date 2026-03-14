"""
Celery tasks that launch Scrapy crawls in a subprocess and record logs.
run_crawl_direct() is a Redis-free fallback for when Celery is unavailable.
"""
import subprocess
import sys
import os
import tempfile
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
# __file__ = backend/tasks/crawl_tasks.py  →  dirname/../ = backend/
_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_CRAWLER_DIR = os.path.join(_BACKEND_DIR, "crawler")


def _build_subprocess_env() -> dict:
    """Build env dict that puts backend/ on PYTHONPATH so 'app' is importable."""
    env = os.environ.copy()
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = _BACKEND_DIR + (os.pathsep + existing if existing else "")
    return env


def _run_scrapy(website_id: int, start_url: str, crawl_log_id: int) -> subprocess.CompletedProcess:
    log_file = os.path.join(tempfile.gettempdir(), f"scrapy_{website_id}.log")
    return subprocess.run(
        [
            sys.executable, "-m", "scrapy", "crawl", "generic",
            "-a", f"website_id={website_id}",
            "-a", f"start_url={start_url}",
            "-a", f"crawl_log_id={crawl_log_id}",
            "--logfile", log_file,
        ],
        cwd=_CRAWLER_DIR,
        capture_output=True,
        text=True,
        timeout=3600,
        env=_build_subprocess_env(),
    )


def run_crawl_direct(website_id: int):
    """
    Run a crawl without Celery/Redis (used when broker is unavailable).
    Called in a background thread from the admin endpoint.
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

        result = _run_scrapy(website_id, website.url, crawl_log.id)

        stats = _parse_scrapy_stats(result.stdout + result.stderr)
        crawl_log.status = "success" if result.returncode == 0 else "error"
        crawl_log.machines_found = stats.get("item_scraped_count", 0)
        crawl_log.errors_count = stats.get("spider_exceptions_count", 0)
        crawl_log.error_details = result.stderr[-2000:] if result.returncode != 0 else None
        crawl_log.finished_at = datetime.now(timezone.utc)
        db.commit()

        website.crawl_status = crawl_log.status
        website.last_crawled_at = crawl_log.finished_at
        db.commit()

        logger.info(
            f"Direct crawl finished: website={website_id} "
            f"status={crawl_log.status} machines={crawl_log.machines_found}"
        )

    except subprocess.TimeoutExpired:
        if crawl_log:
            crawl_log.status = "error"
            crawl_log.error_details = "Crawl timed out after 1 hour"
            crawl_log.finished_at = datetime.now(timezone.utc)
            db.commit()
        logger.error(f"Direct crawl timed out: website={website_id}")
    except Exception as exc:
        logger.exception(f"Direct crawl error: {exc}")
        if crawl_log:
            try:
                crawl_log.status = "error"
                crawl_log.error_details = str(exc)[:500]
                crawl_log.finished_at = datetime.now(timezone.utc)
                db.commit()
            except Exception:
                pass
    finally:
        db.close()


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

        stats = _parse_scrapy_stats(result.stdout + result.stderr)
        crawl_log.status = "success" if result.returncode == 0 else "error"
        crawl_log.machines_found = stats.get("item_scraped_count", 0)
        crawl_log.errors_count = stats.get("spider_exceptions_count", 0)
        crawl_log.error_details = result.stderr[-2000:] if result.stderr else None
        crawl_log.finished_at = datetime.now(timezone.utc)
        db.commit()

        website.crawl_status = crawl_log.status
        website.last_crawled_at = crawl_log.finished_at
        db.commit()

        logger.info(
            f"Crawl finished: website={website_id} "
            f"status={crawl_log.status} machines={crawl_log.machines_found}"
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
    """Queue crawl tasks for all active websites."""
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


def _parse_scrapy_stats(output: str) -> dict:
    stats = {}
    for line in output.split("\n"):
        if "item_scraped_count" in line:
            try:
                count = int(line.split(":")[1].strip().rstrip(","))
                stats["item_scraped_count"] = count
            except Exception:
                pass
        if "spider_exceptions_count" in line:
            try:
                count = int(line.split(":")[1].strip().rstrip(","))
                stats["spider_exceptions_count"] = count
            except Exception:
                pass
    return stats
