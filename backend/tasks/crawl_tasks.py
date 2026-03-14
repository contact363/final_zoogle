"""
Celery tasks that launch Scrapy crawls in a subprocess and record logs.
"""
import subprocess
import json
import sys
import os
from datetime import datetime, timezone

from celery import shared_task
from loguru import logger
from sqlalchemy import select, update

from tasks.celery_app import celery_app
from app.config import settings

# Sync DB access for Celery (not async)
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

_sync_engine = create_engine(settings.DATABASE_SYNC_URL, pool_pre_ping=True)
SyncSession = sessionmaker(bind=_sync_engine, expire_on_commit=False)


def get_sync_db() -> Session:
    return SyncSession()


@celery_app.task(bind=True, name="tasks.crawl_tasks.crawl_website_task")
def crawl_website_task(self, website_id: int):
    """Crawl a single website by ID."""
    from app.models.website import Website
    from app.models.crawl_log import CrawlLog

    db = get_sync_db()
    try:
        website = db.query(Website).filter(Website.id == website_id).first()
        if not website:
            logger.error(f"Website {website_id} not found")
            return

        # Create crawl log
        crawl_log = CrawlLog(
            website_id=website_id,
            task_id=self.request.id,
            status="running",
        )
        db.add(crawl_log)
        db.commit()

        # Update website status
        website.crawl_status = "running"
        db.commit()

        # Launch Scrapy in subprocess
        crawler_dir = os.path.join(os.path.dirname(__file__), "..", "crawler")
        result = subprocess.run(
            [
                sys.executable, "-m", "scrapy", "crawl", "generic",
                "-a", f"website_id={website_id}",
                "-a", f"start_url={website.url}",
                "-a", f"crawl_log_id={crawl_log.id}",
                "--logfile", f"/tmp/scrapy_{website_id}.log",
            ],
            cwd=crawler_dir,
            capture_output=True,
            text=True,
            timeout=3600,  # 1 hour max per site
        )

        # Read stats from output
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
            f"status={crawl_log.status} "
            f"machines={crawl_log.machines_found}"
        )

    except subprocess.TimeoutExpired:
        crawl_log.status = "error"
        crawl_log.error_details = "Crawl timed out after 1 hour"
        crawl_log.finished_at = datetime.now(timezone.utc)
        db.commit()
        website.crawl_status = "error"
        db.commit()
    except Exception as exc:
        logger.exception(f"Crawl task error: {exc}")
        raise
    finally:
        db.close()


@celery_app.task(name="tasks.crawl_tasks.crawl_all_websites_task")
def crawl_all_websites_task():
    """Queue crawl tasks for all active crawl-enabled websites."""
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
    """Extract Scrapy stats dict from log output."""
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
