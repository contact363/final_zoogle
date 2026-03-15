"""
Zoogle 24-Hour Crawl Scheduler
═══════════════════════════════

Distributes crawl jobs evenly across a 24-hour window so that:
  • No two crawls run simultaneously (avoids RAM spikes on small servers).
  • Every active website is crawled once per day automatically.
  • The admin can trigger a full reschedule at any time from the dashboard.

How it works
────────────
  1. Query all active+enabled websites, sorted by last_crawled_at ascending
     (sites not crawled recently go first).
  2. Compute the interval: 86 400 seconds ÷ number of websites.
  3. Assign each website an ETA = now + (index × interval).
  4. Send crawl_website_task.apply_async(eta=eta) for each website.
     Celery will execute them at the computed time automatically.

Direct (no-Celery) fallback
────────────────────────────
  When Redis/Celery is unavailable, run_scheduled_crawls_direct() is used
  instead.  It executes crawls sequentially with time.sleep(interval) between
  them in a single background thread (memory-safe for small servers).

Usage
─────
  From admin API:
      POST /api/admin/crawl/schedule        → start scheduled run
      GET  /api/admin/crawl/schedule/status → see schedule state

  From Celery beat (add to CELERYBEAT_SCHEDULE):
      "daily-crawl": {
          "task": "tasks.scheduler.distributed_crawl_task",
          "schedule": crontab(hour=0, minute=0),   # midnight daily
      }
"""
import time
import threading
from datetime import datetime, timedelta, timezone
from loguru import logger

from tasks.celery_app import celery_app
from app.config import settings

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

_sync_engine = create_engine(settings.DATABASE_SYNC_URL, pool_pre_ping=True)
SyncSession = sessionmaker(bind=_sync_engine, expire_on_commit=False)


def _get_db() -> Session:
    return SyncSession()


# ─────────────────────────────────────────────────────────────────────────────
# Schedule computation
# ─────────────────────────────────────────────────────────────────────────────

SCHEDULE_WINDOW_HOURS = 24


def compute_crawl_schedule(websites: list) -> list[tuple]:
    """
    Given a list of Website ORM objects, compute (website_id, eta) pairs that
    spread all crawls evenly over SCHEDULE_WINDOW_HOURS.

    Websites not crawled recently are placed first.
    Returns: [(website_id, datetime_utc), ...]
    """
    if not websites:
        return []

    n = len(websites)
    # Sort: never-crawled first, then by oldest last_crawled_at
    sorted_sites = sorted(
        websites,
        key=lambda s: s.last_crawled_at or datetime.min.replace(tzinfo=timezone.utc),
    )

    window_seconds = SCHEDULE_WINDOW_HOURS * 3600
    interval = window_seconds / n          # seconds between consecutive crawls
    now = datetime.now(timezone.utc)

    schedule = []
    for i, site in enumerate(sorted_sites):
        eta = now + timedelta(seconds=i * interval)
        schedule.append((site.id, eta))

    logger.info(
        f"Crawl schedule computed: {n} websites over {SCHEDULE_WINDOW_HOURS}h "
        f"(interval={interval:.0f}s ≈ {interval/60:.1f} min/site)"
    )
    return schedule


# ─────────────────────────────────────────────────────────────────────────────
# Celery-based distributed scheduler
# ─────────────────────────────────────────────────────────────────────────────

@celery_app.task(name="tasks.scheduler.distributed_crawl_task")
def distributed_crawl_task():
    """
    Celery task: compute 24-hour schedule and dispatch one crawl_website_task
    per website with the computed ETA.

    Designed to be triggered once per day via Celery beat or the admin API.
    """
    from app.models.website import Website
    from tasks.crawl_tasks import crawl_website_task

    db = _get_db()
    try:
        websites = (
            db.query(Website)
            .filter(Website.is_active == True, Website.crawl_enabled == True)
            .all()
        )
        if not websites:
            logger.info("No active websites to schedule")
            return {"scheduled": 0}

        schedule = compute_crawl_schedule(websites)
        queued = 0

        for website_id, eta in schedule:
            try:
                crawl_website_task.apply_async(args=[website_id], eta=eta)
                queued += 1
                logger.debug(f"Scheduled website {website_id} at {eta.isoformat()}")
            except Exception as exc:
                logger.error(f"Failed to queue website {website_id}: {exc}")

        logger.info(f"Distributed schedule: {queued}/{len(schedule)} websites queued")
        return {"scheduled": queued, "total": len(websites)}

    finally:
        db.close()


# ─────────────────────────────────────────────────────────────────────────────
# Direct (no-Celery) fallback
# ─────────────────────────────────────────────────────────────────────────────

def run_scheduled_crawls_direct() -> dict:
    """
    Fallback when Redis/Celery is unavailable.

    Runs every active+enabled website sequentially in a background thread,
    sleeping for `interval` seconds between each to simulate the 24-hour
    distribution without requiring a task queue.

    Returns immediately with a status dict; the actual crawls run in background.
    """
    from app.models.website import Website
    from tasks.crawl_tasks import run_crawl_direct

    db = _get_db()
    try:
        websites = (
            db.query(Website)
            .filter(Website.is_active == True, Website.crawl_enabled == True)
            .all()
        )
        if not websites:
            return {"scheduled": 0, "mode": "direct"}

        schedule = compute_crawl_schedule(websites)
        n = len(schedule)
        window_seconds = SCHEDULE_WINDOW_HOURS * 3600
        interval = window_seconds / n if n > 1 else 0

    finally:
        db.close()

    def _run_loop(schedule: list, sleep_seconds: float):
        now = datetime.now(timezone.utc)
        for website_id, eta in schedule:
            wait = max(0.0, (eta - now).total_seconds())
            if wait > 1:
                logger.info(f"[Scheduler] sleeping {wait:.0f}s before website {website_id}")
                time.sleep(wait)
            try:
                logger.info(f"[Scheduler] starting crawl for website {website_id}")
                run_crawl_direct(website_id)
            except Exception as exc:
                logger.error(f"[Scheduler] crawl error website={website_id}: {exc}")
            now = datetime.now(timezone.utc)

    t = threading.Thread(target=_run_loop, args=(schedule, interval), daemon=True)
    t.start()

    eta_first = schedule[0][1] if schedule else datetime.now(timezone.utc)
    eta_last  = schedule[-1][1] if schedule else datetime.now(timezone.utc)
    return {
        "scheduled": n,
        "mode": "direct",
        "interval_minutes": round(interval / 60, 1),
        "first_crawl_at": eta_first.isoformat(),
        "last_crawl_at": eta_last.isoformat(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Crawl report helper
# ─────────────────────────────────────────────────────────────────────────────

def generate_crawl_report(db: Session, website_id: int | None = None) -> list[dict]:
    """
    Return crawl report dicts for the most recent run of each website.

    Used by GET /api/admin/crawl/report.
    """
    from app.models.crawl_log import CrawlLog
    from app.models.website import Website
    from sqlalchemy import desc

    query = (
        db.query(CrawlLog, Website.name, Website.url)
        .outerjoin(Website, CrawlLog.website_id == Website.id)
        .order_by(desc(CrawlLog.started_at))
    )
    if website_id is not None:
        query = query.filter(CrawlLog.website_id == website_id)

    rows = query.limit(200).all()

    reports = []
    for log, site_name, site_url in rows:
        duration_s = None
        if log.started_at and log.finished_at:
            duration_s = int((log.finished_at - log.started_at).total_seconds())

        reports.append({
            "log_id":            log.id,
            "website_id":        log.website_id,
            "website_name":      site_name or f"Website #{log.website_id}",
            "website_url":       site_url,
            "status":            log.status,
            "started_at":        log.started_at.isoformat() if log.started_at else None,
            "finished_at":       log.finished_at.isoformat() if log.finished_at else None,
            "duration_seconds":  duration_s,
            "pages_crawled":     log.errors_count,          # proxy — real count in log_output
            "machines_found":    log.machines_found or 0,
            "machines_new":      log.machines_new or 0,
            "machines_updated":  log.machines_updated or 0,
            "machines_skipped":  log.machines_skipped or 0,
            "errors_count":      log.errors_count or 0,
            "error_summary":     (log.error_details or "")[:500] if log.error_details else None,
        })

    return reports
