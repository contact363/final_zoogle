"""
Crawl Tasks — clean 3-phase pipeline orchestration.

Each website crawl runs as a Celery task:

  crawl_website_task(website_id)
    Phase 1 — Discovery      (find how to crawl + entry URLs)
    Phase 2 — URL Collection (crawl listing pages → Redis queue)
    Phase 3 — Machine Crawl  (extract machine data → PostgreSQL)

  crawl_all_websites_task()
    Dispatches crawl_website_task for every active website.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras

from tasks.celery_app import celery_app
from app.config import settings

logger = logging.getLogger(__name__)

# Absolute path to backend/ directory (where scrapy.cfg lives)
BACKEND_DIR = str(Path(__file__).resolve().parent.parent)


# ── DB helpers (sync psycopg2) ────────────────────────────────────────────────

def _db() -> psycopg2.extensions.connection:
    return psycopg2.connect(settings.DATABASE_SYNC_URL)


def _get_website(website_id: int) -> dict:
    conn = _db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM websites WHERE id = %s", (website_id,))
            row = cur.fetchone()
            return dict(row) if row else {}
    finally:
        conn.close()


def _get_training_rules(website_id: int) -> dict:
    conn = _db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM website_training_rules WHERE website_id = %s",
                (website_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else {}
    finally:
        conn.close()


def _update_website(website_id: int, **fields) -> None:
    if not fields:
        return
    conn = _db()
    try:
        sets   = ", ".join(f"{k} = %s" for k in fields)
        values = list(fields.values()) + [website_id]
        with conn.cursor() as cur:
            cur.execute(f"UPDATE websites SET {sets} WHERE id = %s", values)
        conn.commit()
    finally:
        conn.close()


def _create_crawl_log(website_id: int, task_id: str, log_type: str = "crawl") -> int:
    conn = _db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO crawl_logs (website_id, task_id, log_type, status, started_at)
                VALUES (%s, %s, %s, 'running', %s) RETURNING id
                """,
                (website_id, task_id, log_type, datetime.now(timezone.utc)),
            )
            log_id = cur.fetchone()[0]
        conn.commit()
        return log_id
    finally:
        conn.close()


def _finish_crawl_log(
    log_id: int,
    status: str,
    machines_new: int = 0,
    machines_updated: int = 0,
    machines_skipped: int = 0,
    errors_count: int = 0,
    error_details: str = "",
    log_output: str = "",
) -> None:
    conn = _db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE crawl_logs SET
                    status           = %s,
                    machines_found   = %s,
                    machines_new     = %s,
                    machines_updated = %s,
                    machines_skipped = %s,
                    errors_count     = %s,
                    error_details    = %s,
                    log_output       = %s,
                    finished_at      = %s
                WHERE id = %s
                """,
                (
                    status,
                    machines_new + machines_updated,
                    machines_new,
                    machines_updated,
                    machines_skipped,
                    errors_count,
                    (error_details or "")[:5000],
                    (log_output or "")[:10000],
                    datetime.now(timezone.utc),
                    log_id,
                ),
            )
        conn.commit()
    finally:
        conn.close()


# ── Main crawl task ───────────────────────────────────────────────────────────

@celery_app.task(bind=True, max_retries=0, name="tasks.crawl_tasks.crawl_website_task")
def crawl_website_task(self, website_id: int) -> dict:
    """
    Full 3-phase crawl for a single website.
    Never raises — all errors are caught, logged, and written to crawl_logs.
    """
    task_id   = self.request.id or "manual"
    log_id    = _create_crawl_log(website_id, task_id)
    log_lines: list[str] = []

    def log(msg: str) -> None:
        logger.info(msg)
        log_lines.append(msg)

    # ── Load website ──────────────────────────────────────────────────────────
    website = _get_website(website_id)
    if not website:
        _finish_crawl_log(log_id, "error", error_details="Website not found")
        return {"status": "error", "error": "Website not found"}

    website_url = (website.get("url") or "").strip()
    log(f"[Crawl] Starting website_id={website_id} url={website_url}")

    _update_website(
        website_id,
        crawl_status="running",
        discovery_status="running",
    )

    training_rules = _get_training_rules(website_id)

    # ══════════════════════════════════════════════════════════════════════════
    # PHASE 1 — DISCOVERY
    # ══════════════════════════════════════════════════════════════════════════
    from crawler.pipeline.phase1_discovery import run_discovery

    try:
        discovery = run_discovery(
            website_id=website_id,
            website_url=website_url,
            training_rules=dict(training_rules) if training_rules else None,
        )
    except Exception as exc:
        error = f"Discovery crashed: {exc}"
        log(f"[Phase1] ERROR: {error}")
        _update_website(website_id, crawl_status="error", discovery_status="error")
        _finish_crawl_log(log_id, "error", error_details=error, log_output="\n".join(log_lines))
        return {"status": "error", "error": error}

    if not discovery.success:
        log(f"[Phase1] FAILED: {discovery.error}")
        _update_website(
            website_id,
            crawl_status="error",
            discovery_status="error",
            discovered_count=0,
        )
        _finish_crawl_log(
            log_id, "error",
            error_details=discovery.error or "Discovery failed",
            log_output="\n".join(log_lines),
        )
        return {"status": "error", "error": discovery.error}

    log(f"[Phase1] OK method={discovery.method} real_count={discovery.estimated_count}")
    # Do NOT set discovered_count here — Phase 2 will set it from actual URLs.
    # Only update discovery_status so the UI shows the method is known.
    _update_website(website_id, discovery_status="done")

    # ══════════════════════════════════════════════════════════════════════════
    # PLAYWRIGHT FAST PATH — JS-rendered site, no API found, use lightweight_crawler
    # Handles React/Vue/Angular/Next.js/Nuxt sites that Scrapy cannot parse.
    # ══════════════════════════════════════════════════════════════════════════
    if discovery.method in ("playwright-fallback", "playwright-xhr",
                            "playwright-category", "api-brute"):
        log(f"[Phase2/3] Playwright fallback — skipping Scrapy, using lightweight_crawler")
        _update_website(website_id, url_collection_status="running")

        from crawler.pipeline.phase3_machine_crawl import run_lightweight_crawl
        try:
            result = run_lightweight_crawl(
                website_id=website_id,
                website_url=website_url,
                db_sync_url=settings.DATABASE_SYNC_URL,
                max_requests=500,
                request_delay=float((training_rules or {}).get("request_delay") or 0.5),
            )
        except Exception as exc:
            error = f"Playwright crawl crashed: {exc}"
            log(f"[Phase3/PW] ERROR: {error}")
            _update_website(website_id, crawl_status="error")
            _finish_crawl_log(log_id, "error", error_details=error, log_output="\n".join(log_lines))
            return {"status": "error", "error": error}

        real_count = result.machines_new + result.machines_updated
        log(f"[Phase3/PW] new={result.machines_new} updated={result.machines_updated} "
            f"skipped={result.machines_skipped} — real_count={real_count}")
        _update_website(
            website_id,
            url_collection_status="done",
            urls_collected=real_count,
            discovered_count=real_count,
            crawl_status="success",
            last_crawled_at=datetime.now(timezone.utc),
        )
        _finish_crawl_log(
            log_id, "success",
            machines_new=result.machines_new,
            machines_updated=result.machines_updated,
            machines_skipped=result.machines_skipped,
            errors_count=result.errors,
            log_output="\n".join(log_lines),
        )
        return {
            "status": "success",
            "method": discovery.method,
            "machines_new": result.machines_new,
            "machines_updated": result.machines_updated,
        }

    # ══════════════════════════════════════════════════════════════════════════
    # API FAST PATH — direct API extraction, no Scrapy needed
    # ══════════════════════════════════════════════════════════════════════════
    if discovery.api_config:
        log(f"[Phase2/3] API path ({discovery.api_config.api_type}) — skipping Scrapy")
        _update_website(website_id, url_collection_status="running")

        from crawler.pipeline.phase3_machine_crawl import run_api_crawl
        try:
            result = run_api_crawl(
                website_id=website_id,
                api_config=discovery.api_config,
                db_sync_url=settings.DATABASE_SYNC_URL,
            )
        except Exception as exc:
            error = f"API crawl crashed: {exc}"
            log(f"[Phase3/API] ERROR: {error}")
            _update_website(website_id, crawl_status="error")
            _finish_crawl_log(log_id, "error", error_details=error, log_output="\n".join(log_lines))
            return {"status": "error", "error": error}

        # Real count = machines extracted from API
        real_count = result.machines_new + result.machines_updated
        log(
            f"[Phase3/API] new={result.machines_new} updated={result.machines_updated} "
            f"skipped={result.machines_skipped} — real_count={real_count}"
        )
        _update_website(
            website_id,
            url_collection_status="done",
            urls_collected=real_count,
            discovered_count=real_count,   # real, from actual API data
            crawl_status="success",
            last_crawled_at=datetime.now(timezone.utc),
        )
        _finish_crawl_log(
            log_id, "success",
            machines_new=result.machines_new,
            machines_updated=result.machines_updated,
            machines_skipped=result.machines_skipped,
            errors_count=result.errors,
            log_output="\n".join(log_lines),
        )
        return {
            "status": "success",
            "method": "api",
            "machines_new": result.machines_new,
            "machines_updated": result.machines_updated,
        }

    # ══════════════════════════════════════════════════════════════════════════
    # SITEMAP FAST PATH — product URLs known, skip Phase 2 spider
    # ══════════════════════════════════════════════════════════════════════════
    if discovery.product_urls:
        # Sitemap fast path — product URLs are already known (real count)
        log(f"[Phase2] Sitemap — loading {len(discovery.product_urls)} real URLs into queue")
        from crawler.queue.url_queue import URLQueue
        q = URLQueue(settings.REDIS_URL, website_id)
        q.clear()
        pushed = q.push_many(discovery.product_urls)
        log(f"[Phase2] Pushed {pushed} URLs to Redis")
        # discovered_count = real URL count from sitemap
        _update_website(
            website_id,
            url_collection_status="done",
            urls_collected=pushed,
            discovered_count=pushed,   # real
        )

    # ══════════════════════════════════════════════════════════════════════════
    # PHASE 2 — URL COLLECTION (category pages → product URLs)
    # ══════════════════════════════════════════════════════════════════════════
    else:
        log(f"[Phase2] Crawling {len(discovery.category_urls)} category pages")
        _update_website(website_id, url_collection_status="running")

        from crawler.pipeline.phase2_url_collection import run_url_collection

        delay   = float((training_rules or {}).get("request_delay") or 1.0)
        pattern = str((training_rules or {}).get("product_link_pattern") or "")

        try:
            urls_collected = run_url_collection(
                website_id=website_id,
                category_urls=discovery.category_urls,
                redis_url=settings.REDIS_URL,
                backend_dir=BACKEND_DIR,
                product_link_pattern=pattern,
                request_delay=delay,
            )
        except Exception as exc:
            error = f"URL collection crashed: {exc}"
            log(f"[Phase2] ERROR: {error}")
            _update_website(website_id, crawl_status="error", url_collection_status="error")
            _finish_crawl_log(log_id, "error", error_details=error, log_output="\n".join(log_lines))
            return {"status": "error", "error": error}

        log(f"[Phase2] Collected {urls_collected} real product URLs")

        # discovered_count = urls_collected (real — Phase 2 is the source of truth)
        _update_website(
            website_id,
            url_collection_status="done",
            urls_collected=urls_collected,
            discovered_count=urls_collected,   # real, not estimated
        )

        if urls_collected == 0:
            # Safety net: Scrapy found nothing — try Playwright-based crawler
            log("[Phase2] No URLs collected — trying Playwright fallback")
            from crawler.pipeline.phase3_machine_crawl import run_lightweight_crawl
            try:
                pw_result = run_lightweight_crawl(
                    website_id=website_id,
                    website_url=website_url,
                    db_sync_url=settings.DATABASE_SYNC_URL,
                    max_requests=500,
                    request_delay=delay,
                )
            except Exception as exc:
                log(f"[Phase2/PW] Playwright fallback error: {exc}")
                pw_result = None

            if pw_result and (pw_result.machines_new + pw_result.machines_updated) > 0:
                real_count = pw_result.machines_new + pw_result.machines_updated
                log(f"[Phase2/PW] Playwright rescued: new={pw_result.machines_new} "
                    f"updated={pw_result.machines_updated}")
                _update_website(
                    website_id,
                    url_collection_status="done",
                    urls_collected=real_count,
                    discovered_count=real_count,
                    crawl_status="success",
                    last_crawled_at=datetime.now(timezone.utc),
                )
                _finish_crawl_log(
                    log_id, "success",
                    machines_new=pw_result.machines_new,
                    machines_updated=pw_result.machines_updated,
                    machines_skipped=pw_result.machines_skipped,
                    errors_count=pw_result.errors,
                    log_output="\n".join(log_lines),
                )
                return {
                    "status": "success",
                    "method": "playwright-rescue",
                    "machines_new": pw_result.machines_new,
                    "machines_updated": pw_result.machines_updated,
                }
            else:
                log("[Phase2] Playwright fallback also found 0 — aborting")
                _update_website(website_id, crawl_status="error")
                _finish_crawl_log(
                    log_id, "error",
                    error_details="No product URLs found (Scrapy + Playwright both failed)",
                    log_output="\n".join(log_lines),
                )
                return {"status": "error", "error": "No product URLs found"}

    # ══════════════════════════════════════════════════════════════════════════
    # PHASE 3 — MACHINE CRAWLING
    # ══════════════════════════════════════════════════════════════════════════
    log("[Phase3] Starting machine extraction")

    from crawler.pipeline.phase3_machine_crawl import run_machine_crawl

    delay = float((training_rules or {}).get("request_delay") or 1.0)

    try:
        result = run_machine_crawl(
            website_id=website_id,
            redis_url=settings.REDIS_URL,
            db_sync_url=settings.DATABASE_SYNC_URL,
            backend_dir=BACKEND_DIR,
            request_delay=delay,
        )
    except Exception as exc:
        error = f"Machine crawl crashed: {exc}"
        log(f"[Phase3] ERROR: {error}")
        _update_website(website_id, crawl_status="error")
        _finish_crawl_log(log_id, "error", error_details=error, log_output="\n".join(log_lines))
        return {"status": "error", "error": error}

    log(
        f"[Phase3] new={result.machines_new} "
        f"updated={result.machines_updated} skipped={result.machines_skipped}"
    )
    _update_website(
        website_id,
        crawl_status="success",
        last_crawled_at=datetime.now(timezone.utc),
    )
    _finish_crawl_log(
        log_id, "success",
        machines_new=result.machines_new,
        machines_updated=result.machines_updated,
        machines_skipped=result.machines_skipped,
        errors_count=result.errors,
        log_output="\n".join(log_lines),
    )

    log(
        f"[Crawl] DONE website_id={website_id} "
        f"new={result.machines_new} updated={result.machines_updated}"
    )
    return {
        "status": "success",
        "method": discovery.method,
        "machines_new": result.machines_new,
        "machines_updated": result.machines_updated,
        "machines_skipped": result.machines_skipped,
    }


# ── Bulk dispatch task ────────────────────────────────────────────────────────

@celery_app.task(name="tasks.crawl_tasks.crawl_all_websites_task")
def crawl_all_websites_task() -> dict:
    """
    Dispatch crawl_website_task for every active website.
    Staggers all crawls evenly across 24 hours to avoid server overload.
    """
    conn = _db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id FROM websites
                WHERE is_active = TRUE AND crawl_enabled = TRUE
                ORDER BY last_crawled_at ASC NULLS FIRST
                """
            )
            website_ids = [row["id"] for row in cur.fetchall()]
    finally:
        conn.close()

    if not website_ids:
        logger.info("crawl_all_websites_task: no active websites to crawl")
        return {"dispatched": 0}

    # Stagger evenly across 24 hours
    interval_seconds = 86400.0 / len(website_ids)

    for idx, website_id in enumerate(website_ids):
        eta = datetime.now(timezone.utc) + timedelta(seconds=idx * interval_seconds)
        crawl_website_task.apply_async(args=[website_id], eta=eta)

    logger.info(
        "crawl_all_websites_task: dispatched %d crawls (interval=%.0fs)",
        len(website_ids),
        interval_seconds,
    )
    return {
        "dispatched": len(website_ids),
        "interval_seconds": round(interval_seconds),
    }


# ── Direct runners (no Celery — called from background threads in admin.py) ───
#
# These are the functions imported by the FastAPI router when Celery is not
# available or when the admin triggers discovery/collection/crawl directly.
# They MUST NOT raise — all exceptions are caught and written to the DB.

def run_discovery_direct(website_id: int) -> dict:
    """
    Run Phase 1 (detection) + Phase 2 (URL collection) in the current thread.
    Called by POST /api/admin/websites/{id}/discover.

    discovered_count = REAL URL count from Phase 2, not an estimate.
    Always creates a Crawl Log entry, never raises.
    """
    import uuid
    task_id = f"discovery-{website_id}-{uuid.uuid4().hex[:8]}"
    log_lines: list[str] = []

    def log(msg: str) -> None:
        logger.info(msg)
        log_lines.append(msg)

    log_id = None
    try:
        website = _get_website(website_id)
        if not website:
            return {"status": "error", "error": "Website not found"}

        log_id = _create_crawl_log(website_id, task_id, log_type="discovery")
        website_url = (website.get("url") or "").strip()
        log(f"[Discovery] Starting website_id={website_id} url={website_url}")

        training_rules = _get_training_rules(website_id)
        _update_website(website_id, discovery_status="running", url_collection_status="running")

        # ── Phase 1: detect how to crawl ──────────────────────────────────
        from crawler.pipeline.phase1_discovery import run_discovery
        detection = run_discovery(
            website_id=website_id,
            website_url=website_url,
            training_rules=dict(training_rules) if training_rules else None,
        )
        log(f"[Discovery] Detection: method={detection.method}")
        if detection.notes:
            log("  notes: " + "; ".join(detection.notes[-5:]))
        _update_website(website_id, discovery_status="done")

        urls_collected = 0

        # ── Playwright / brute-force — JS site, skip Scrapy entirely ─────────
        if detection.method in ("playwright-fallback", "playwright-xhr",
                                "playwright-category", "api-brute"):
            log("[Discovery] JS-rendered site with no API — Playwright full crawl")
            _update_website(website_id, url_collection_status="running")
            from crawler.pipeline.phase3_machine_crawl import run_lightweight_crawl
            try:
                pw_result = run_lightweight_crawl(
                    website_id=website_id,
                    website_url=website_url,
                    db_sync_url=settings.DATABASE_SYNC_URL,
                    max_requests=500,
                    request_delay=float((training_rules or {}).get("request_delay") or 0.5),
                )
                urls_collected = pw_result.machines_new + pw_result.machines_updated
                log(f"[Discovery] Playwright crawl: new={pw_result.machines_new} "
                    f"updated={pw_result.machines_updated}")
            except Exception as exc:
                log(f"[Discovery] Playwright crawl error: {exc}")
                urls_collected = 0

        # ── API fast path ──────────────────────────────────────────────────
        elif detection.api_config:
            log(f"[Discovery] API path ({detection.api_config.api_type})")
            try:
                from crawler.extractors.api_extractor import _fetch_page, _session
                sess = _session()
                sample = _fetch_page(detection.api_config, 0, sess)
                urls_collected = detection.api_config.page_size if len(sample) == detection.api_config.page_size else len(sample)
                log(f"[Discovery] API sample: {urls_collected} items on first page")
            except Exception as exc:
                log(f"[Discovery] API sample error: {exc}")
                urls_collected = detection.estimated_count

        # ── Sitemap fast path ──────────────────────────────────────────────
        elif detection.product_urls:
            from crawler.queue.url_queue import URLQueue
            q = URLQueue(settings.REDIS_URL, website_id)
            q.clear()
            urls_collected = q.push_many(detection.product_urls)
            log(f"[Discovery] Sitemap: {urls_collected} real URLs loaded")

        # ── HTML/category path — run Phase 2 URL collection ───────────────
        else:
            delay   = float((training_rules or {}).get("request_delay") or 1.0)
            pattern = str((training_rules or {}).get("product_link_pattern") or "")
            log(f"[Discovery] Phase 2 URL collection on {len(detection.category_urls)} entry points")
            from crawler.pipeline.phase2_url_collection import run_url_collection
            try:
                urls_collected = run_url_collection(
                    website_id=website_id,
                    category_urls=detection.category_urls,
                    redis_url=settings.REDIS_URL,
                    backend_dir=BACKEND_DIR,
                    product_link_pattern=pattern,
                    request_delay=delay,
                )
            except Exception as exc:
                log(f"[Discovery] URL collection error: {exc}")
                urls_collected = 0

            # Safety net: Scrapy found nothing — try Playwright
            if urls_collected == 0:
                log("[Discovery] Scrapy found 0 URLs — trying Playwright fallback")
                from crawler.pipeline.phase3_machine_crawl import run_lightweight_crawl
                try:
                    pw_result = run_lightweight_crawl(
                        website_id=website_id,
                        website_url=website_url,
                        db_sync_url=settings.DATABASE_SYNC_URL,
                        max_requests=500,
                        request_delay=delay,
                    )
                    urls_collected = pw_result.machines_new + pw_result.machines_updated
                    log(f"[Discovery] Playwright rescue: new={pw_result.machines_new} "
                        f"updated={pw_result.machines_updated}")
                except Exception as exc:
                    log(f"[Discovery] Playwright rescue error: {exc}")
                    urls_collected = 0

        log(f"[Discovery] Real URL count: {urls_collected}")

        # Mismatch warning
        if detection.estimated_count > 0 and abs(urls_collected - detection.estimated_count) > detection.estimated_count * 0.5:
            log(
                f"[Discovery] WARNING: detection estimated {detection.estimated_count} "
                f"but Phase 2 found {urls_collected} — using real count"
            )

        # discovered_count = real URL count (Phase 2 is source of truth)
        _update_website(
            website_id,
            url_collection_status="done",
            urls_collected=urls_collected,
            discovered_count=urls_collected,
        )
        _finish_crawl_log(
            log_id, "success",
            machines_new=urls_collected,
            log_output="\n".join(log_lines),
        )
        return {
            "status": "success",
            "method": detection.method,
            "urls_collected": urls_collected,
            "discovered_count": urls_collected,
        }

    except Exception as exc:
        logger.exception("[run_discovery_direct] error for website_id=%d: %s", website_id, exc)
        try:
            _update_website(website_id, discovery_status="error", url_collection_status="error")
            if log_id:
                _finish_crawl_log(log_id, "error", error_details=str(exc), log_output="\n".join(log_lines))
        except Exception:
            pass
        return {"status": "error", "error": str(exc)}


def run_url_collection_direct(website_id: int) -> dict:
    """
    Run Phase 1 + Phase 2 in the current thread (no Celery).
    Called by POST /api/admin/websites/{id}/collect-urls.

    Always creates a Crawl Log entry, never raises.
    """
    import uuid
    task_id = f"url-collect-{website_id}-{uuid.uuid4().hex[:8]}"
    log_lines: list[str] = []

    def log(msg: str) -> None:
        logger.info(msg)
        log_lines.append(msg)

    log_id = None
    try:
        website = _get_website(website_id)
        if not website:
            return {"status": "error", "error": "Website not found"}

        # Create log entry immediately
        log_id = _create_crawl_log(website_id, task_id, log_type="url_collection")
        log(f"[URLCollection] Starting website_id={website_id} url={website.get('url')}")

        website_url = (website.get("url") or "").strip()
        training_rules = _get_training_rules(website_id)

        _update_website(
            website_id,
            discovery_status="running",
            url_collection_status="running",
        )

        from crawler.pipeline.phase1_discovery import run_discovery
        discovery = run_discovery(
            website_id=website_id,
            website_url=website_url,
            training_rules=dict(training_rules) if training_rules else None,
        )

        log(f"[URLCollection] Detection: method={discovery.method}")
        # Do NOT set discovered_count here — Phase 2 will set the real count
        _update_website(website_id, discovery_status="done")

        urls_collected = 0

        urls_collected = 0

        # Sitemap fast path — real URLs already known
        if discovery.product_urls:
            from crawler.queue.url_queue import URLQueue
            q = URLQueue(settings.REDIS_URL, website_id)
            q.clear()
            urls_collected = q.push_many(discovery.product_urls)
            log(f"[URLCollection] Sitemap: {urls_collected} real URLs loaded")

        # API fast path — URL count from API sample
        elif discovery.api_config:
            log(f"[URLCollection] API path ({discovery.api_config.api_type})")
            try:
                from crawler.extractors.api_extractor import _fetch_page, _session
                sample = _fetch_page(discovery.api_config, 0, _session())
                urls_collected = discovery.api_config.page_size if len(sample) == discovery.api_config.page_size else len(sample)
            except Exception as exc:
                log(f"[URLCollection] API sample error: {exc}")
                urls_collected = 0

        # HTML/category — Scrapy url_collector
        else:
            delay   = float((training_rules or {}).get("request_delay") or 1.0)
            pattern = str((training_rules or {}).get("product_link_pattern") or "")
            log(f"[URLCollection] Crawling {len(discovery.category_urls)} category pages")
            from crawler.pipeline.phase2_url_collection import run_url_collection
            urls_collected = run_url_collection(
                website_id=website_id,
                category_urls=discovery.category_urls,
                redis_url=settings.REDIS_URL,
                backend_dir=BACKEND_DIR,
                product_link_pattern=pattern,
                request_delay=delay,
            )

        log(f"[URLCollection] Real URLs: {urls_collected}")
        # discovered_count = urls_collected (real, authoritative)
        _update_website(
            website_id,
            url_collection_status="done",
            urls_collected=urls_collected,
            discovered_count=urls_collected,   # real count
        )
        _finish_crawl_log(
            log_id, "success",
            machines_new=urls_collected,
            log_output="\n".join(log_lines),
        )
        return {
            "status": "success",
            "method": discovery.method,
            "urls_collected": urls_collected,
            "discovered_count": urls_collected,
        }

    except Exception as exc:
        logger.exception("[run_url_collection_direct] error for website_id=%d: %s", website_id, exc)
        try:
            _update_website(website_id, url_collection_status="error")
            if log_id:
                _finish_crawl_log(log_id, "error", error_details=str(exc), log_output="\n".join(log_lines))
        except Exception:
            pass
        return {"status": "error", "error": str(exc)}


def run_crawl_direct(website_id: int) -> dict:
    """
    Run the full 3-phase crawl in the current thread (no Celery).
    Called by POST /api/admin/crawl/start/{id} as fallback when Celery is down.

    Wraps crawl_website_task logic directly — never raises.
    """
    try:
        # Reuse the Celery task body directly (it already handles all exceptions)
        return crawl_website_task(website_id)
    except Exception as exc:
        logger.exception("[run_crawl_direct] unhandled error for website_id=%d: %s", website_id, exc)
        try:
            _update_website(website_id, crawl_status="error")
        except Exception:
            pass
        return {"status": "error", "error": str(exc)}
