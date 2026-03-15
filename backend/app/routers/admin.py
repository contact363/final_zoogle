"""
Admin panel endpoints – all require is_admin=True.

Covers:
  - Website CRUD + crawl control
  - Machine table view + export
  - Crawl log viewer
  - Dashboard stats
  - Stuck crawl cleanup
"""
import io
import csv
from typing import List, Optional
from datetime import datetime, timezone
from urllib.parse import urlparse

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import select, func, desc, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.database import get_db
from app.models.machine import Machine
from app.models.website import Website
from app.models.crawl_log import CrawlLog
from app.models.user import User
from app.models.search_log import SearchLog
from app.schemas.website import (
    WebsiteCreate, WebsiteRead, WebsiteUpdate,
    TrainingRulesCreate, TrainingRulesRead,
)
from app.schemas.machine import MachineRead, MachineUpdate
from app.utils.security import require_admin

router = APIRouter(prefix="/api/admin", tags=["admin"])


# ── Dashboard ─────────────────────────────────────────────────────────────────

@router.get("/stats")
async def dashboard_stats(
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    total_machines = (await db.execute(select(func.count()).select_from(Machine))).scalar()
    total_websites = (await db.execute(select(func.count()).select_from(Website))).scalar()
    total_users = (await db.execute(select(func.count()).select_from(User))).scalar()
    total_searches = (await db.execute(select(func.count()).select_from(SearchLog))).scalar()

    recent_crawls = (
        await db.execute(
            select(CrawlLog, Website.name)
            .outerjoin(Website, CrawlLog.website_id == Website.id)
            .order_by(desc(CrawlLog.started_at))
            .limit(5)
        )
    ).all()

    return {
        "total_machines": total_machines,
        "total_websites": total_websites,
        "total_users": total_users,
        "total_searches": total_searches,
        "recent_crawls": [
            {
                "id": c.id,
                "website_id": c.website_id,
                "website_name": name or f"Website #{c.website_id}",
                "status": c.status,
                "machines_found": c.machines_found or 0,
                "machines_new": c.machines_new or 0,
                "started_at": c.started_at,
                "finished_at": c.finished_at,
            }
            for c, name in recent_crawls
        ],
    }


# ── Website Management ────────────────────────────────────────────────────────

@router.get("/websites", response_model=List[WebsiteRead])
async def list_websites(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    result = await db.execute(
        select(Website).order_by(Website.created_at.desc()).offset(skip).limit(limit)
    )
    return result.scalars().all()


@router.post("/websites", response_model=WebsiteRead, status_code=201)
async def add_website(
    payload: WebsiteCreate,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    result = await db.execute(select(Website).where(Website.url == payload.url))
    if result.scalar_one_or_none():
        raise HTTPException(status_code=409, detail="Website URL already exists")

    website = Website(**payload.model_dump())
    db.add(website)
    await db.flush()
    return website


@router.patch("/websites/{website_id}", response_model=WebsiteRead)
async def update_website(
    website_id: int,
    payload: WebsiteUpdate,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    result = await db.execute(select(Website).where(Website.id == website_id))
    website = result.scalar_one_or_none()
    if not website:
        raise HTTPException(status_code=404, detail="Website not found")

    for field, value in payload.model_dump(exclude_unset=True).items():
        setattr(website, field, value)
    await db.flush()
    return website


@router.delete("/websites/{website_id}", status_code=204)
async def delete_website(
    website_id: int,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    result = await db.execute(select(Website).where(Website.id == website_id))
    website = result.scalar_one_or_none()
    if not website:
        raise HTTPException(status_code=404, detail="Website not found")

    # Use raw DELETE so DB-level CASCADE handles child rows (machines, crawl_logs)
    # without SQLAlchemy trying to lazy-load them first
    from sqlalchemy import delete as sql_delete
    await db.execute(sql_delete(Machine).where(Machine.website_id == website_id))
    await db.execute(sql_delete(CrawlLog).where(CrawlLog.website_id == website_id))
    await db.delete(website)


# ── Training Rules ────────────────────────────────────────────────────────────

@router.get("/websites/{website_id}/training", response_model=Optional[TrainingRulesRead])
async def get_training_rules(
    website_id: int,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    """Return the training rules for a website, or null if none exist yet."""
    from app.models.training_rules import WebsiteTrainingRules
    result = await db.execute(
        select(WebsiteTrainingRules).where(WebsiteTrainingRules.website_id == website_id)
    )
    return result.scalar_one_or_none()


@router.post("/websites/{website_id}/training", response_model=TrainingRulesRead)
async def save_training_rules(
    website_id: int,
    payload: TrainingRulesCreate,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    """Create or update (upsert) training rules for a website."""
    from app.models.training_rules import WebsiteTrainingRules

    # Verify website exists
    website = (await db.execute(select(Website).where(Website.id == website_id))).scalar_one_or_none()
    if not website:
        raise HTTPException(status_code=404, detail="Website not found")

    result = await db.execute(
        select(WebsiteTrainingRules).where(WebsiteTrainingRules.website_id == website_id)
    )
    rules = result.scalar_one_or_none()

    if rules:
        for field, value in payload.model_dump(exclude_unset=True).items():
            setattr(rules, field, value)
    else:
        rules = WebsiteTrainingRules(website_id=website_id, **payload.model_dump())
        db.add(rules)

    await db.flush()
    await db.refresh(rules)
    return rules


@router.delete("/websites/{website_id}/training", status_code=204)
async def delete_training_rules(
    website_id: int,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    """Remove all training rules for a website (revert to auto-discovery)."""
    from app.models.training_rules import WebsiteTrainingRules
    result = await db.execute(
        select(WebsiteTrainingRules).where(WebsiteTrainingRules.website_id == website_id)
    )
    rules = result.scalar_one_or_none()
    if rules:
        await db.delete(rules)


# ── Crawl Control ─────────────────────────────────────────────────────────────

@router.post("/crawl/start/{website_id}")
async def start_crawl(
    website_id: int,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    result = await db.execute(select(Website).where(Website.id == website_id))
    website = result.scalar_one_or_none()
    if not website:
        raise HTTPException(status_code=404, detail="Website not found")

    await db.execute(
        update(Website).where(Website.id == website_id).values(crawl_status="running")
    )

    try:
        from tasks.crawl_tasks import crawl_website_task
        task = crawl_website_task.delay(website_id)
        return {"task_id": task.id, "status": "started", "mode": "celery"}
    except Exception:
        import threading
        from tasks.crawl_tasks import run_crawl_direct
        t = threading.Thread(target=run_crawl_direct, args=(website_id,), daemon=True)
        t.start()
        return {"task_id": f"direct-{website_id}", "status": "started", "mode": "direct"}


@router.post("/crawl/start-all")
async def start_all_crawls(
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    try:
        from tasks.crawl_tasks import crawl_all_websites_task
        task = crawl_all_websites_task.delay()
        return {"task_id": task.id, "status": "started", "mode": "celery"}
    except Exception:
        import threading
        from tasks.crawl_tasks import run_crawl_direct
        result = await db.execute(
            select(Website).where(Website.is_active == True, Website.crawl_enabled == True)
        )
        sites = result.scalars().all()
        site_ids = [s.id for s in sites]

        # Run sequentially in ONE background thread to avoid OOM on limited RAM
        def run_all_sequential(ids):
            for sid in ids:
                try:
                    run_crawl_direct(sid)
                except Exception as e:
                    import logging
                    logging.error(f"Sequential crawl error site={sid}: {e}")

        t = threading.Thread(target=run_all_sequential, args=(site_ids,), daemon=True)
        t.start()
        return {"task_id": "direct-all", "status": "started", "mode": "direct", "count": len(sites)}


@router.post("/crawl/stop/{task_id}")
async def stop_crawl(
    task_id: str,
    _=Depends(require_admin),
):
    from tasks.celery_app import celery_app
    celery_app.control.revoke(task_id, terminate=True)
    return {"task_id": task_id, "status": "stopped"}


@router.post("/crawl/schedule")
async def start_scheduled_crawl(
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    """
    Start a 24-hour distributed crawl schedule.

    All active+enabled websites are crawled sequentially, spread evenly
    across 24 hours.  Sites not crawled recently run first.

    Uses Celery ETA tasks when available; falls back to a background thread.
    """
    try:
        from tasks.scheduler import distributed_crawl_task
        task = distributed_crawl_task.delay()
        return {
            "task_id": task.id,
            "status": "scheduled",
            "mode": "celery",
            "message": "24-hour distributed crawl schedule started via Celery",
        }
    except Exception:
        import asyncio
        from tasks.scheduler import run_scheduled_crawls_direct
        result = await asyncio.get_event_loop().run_in_executor(
            None, run_scheduled_crawls_direct
        )
        return {"status": "scheduled", **result}


@router.get("/crawl/report")
async def crawl_report(
    website_id: Optional[int] = None,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    """
    Return the crawl report for all (or one) website.

    Each entry shows: website name, start/end time, duration, machines found,
    new insertions, updates, skips, and error summary.
    """
    from tasks.scheduler import generate_crawl_report
    from sqlalchemy.orm import Session as SyncSession
    from sqlalchemy import create_engine
    from app.config import settings as _settings

    # generate_crawl_report is sync SQLAlchemy — run in thread executor
    import asyncio

    def _report():
        engine  = create_engine(_settings.DATABASE_SYNC_URL, pool_pre_ping=True)
        Sess    = __import__("sqlalchemy.orm", fromlist=["sessionmaker"]).sessionmaker(bind=engine)
        sync_db = Sess()
        try:
            return generate_crawl_report(sync_db, website_id)
        finally:
            sync_db.close()
            engine.dispose()

    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(None, _report)
    return {"count": len(data), "reports": data}


@router.get("/crawl/schedule/preview")
async def preview_crawl_schedule(
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    """
    Preview the 24-hour schedule without starting it.
    Returns a list of {website_id, website_name, scheduled_at} entries.
    """
    from tasks.scheduler import compute_crawl_schedule

    result = await db.execute(
        select(Website)
        .where(Website.is_active == True, Website.crawl_enabled == True)
        .order_by(Website.last_crawled_at.asc().nulls_first())
    )
    websites = result.scalars().all()
    schedule = compute_crawl_schedule(websites)

    n  = len(websites)
    window = 24 * 3600
    interval = window / n if n > 1 else 0

    return {
        "total_websites": n,
        "window_hours": 24,
        "interval_minutes": round(interval / 60, 1),
        "schedule": [
            {
                "website_id": wid,
                "scheduled_at": eta.isoformat(),
            }
            for wid, eta in schedule
        ],
    }


@router.get("/crawl/diagnose/{website_id}")
async def diagnose_crawl(
    website_id: int,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    """
    Run a quick 30-second test crawl and return the raw scrapy output.
    Use this to debug why a website is returning 0 machines or errors.
    """
    import subprocess, sys, os
    from tasks.crawl_tasks import _CRAWLER_DIR, _build_subprocess_env

    result = await db.execute(select(Website).where(Website.id == website_id))
    website = result.scalar_one_or_none()
    if not website:
        raise HTTPException(status_code=404, detail="Website not found")

    try:
        proc = subprocess.run(
            [
                sys.executable, "-m", "scrapy", "crawl", "generic",
                "-a", f"website_id={website_id}",
                "-a", f"start_url={website.url}",
                "-a", "crawl_log_id=0",
                "--set", "CLOSESPIDER_ITEMCOUNT=5",
                "--set", "DEPTH_LIMIT=2",
                "--set", "LOG_LEVEL=DEBUG",
            ],
            cwd=_CRAWLER_DIR,
            capture_output=True,
            text=True,
            timeout=60,
            env=_build_subprocess_env(),
        )
        combined = (proc.stdout or "") + (proc.stderr or "")
        return {
            "website_id": website_id,
            "url": website.url,
            "returncode": proc.returncode,
            "output": combined[-5000:],  # last 5000 chars
            "output_length": len(combined),
        }
    except subprocess.TimeoutExpired:
        return {"website_id": website_id, "url": website.url, "returncode": -1, "output": "Timed out after 60s"}
    except Exception as e:
        return {"website_id": website_id, "url": website.url, "returncode": -1, "output": str(e)}


@router.post("/websites/recalculate-counts")
async def recalculate_machine_counts(
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    """Recalculate machine_count for every website from actual DB rows."""
    from sqlalchemy import text
    await db.execute(text("""
        UPDATE websites w
        SET machine_count = (
            SELECT COUNT(*) FROM machines m WHERE m.website_id = w.id
        )
    """))
    return {"status": "ok", "message": "Machine counts recalculated"}


@router.post("/websites/fix-names")
async def fix_website_names(
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    """
    Auto-fix website names that were accidentally set to the full URL.
    Extracts a clean domain name (e.g. https://reble-machinery.de/ → Reble Machinery).
    """
    import re as _re
    result = await db.execute(select(Website))
    websites = result.scalars().all()
    fixed = 0
    for w in websites:
        # Fix if name looks like a URL
        if w.name.startswith("http") or w.name.startswith("www."):
            domain = urlparse(w.name if w.name.startswith("http") else f"https://{w.name}").netloc
            domain = domain.lstrip("www.")
            # "reble-machinery.de" → "Reble Machinery"
            clean = domain.split(".")[0]
            clean = _re.sub(r"[-_]", " ", clean).title()
            w.name = clean or domain
            fixed += 1
    return {"fixed": fixed}


@router.post("/machines/fill-types")
async def fill_machine_types(
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    """
    Auto-fill machine_type for all machines where it is NULL.
    Uses brand name → type hints mapping, then title keyword scan.
    Returns count of machines updated.
    """
    from app.services.normalization_service import infer_type_from_brand

    result = await db.execute(
        select(Machine).where(Machine.machine_type == None)
    )
    machines = result.scalars().all()

    updated = 0
    for m in machines:
        inferred = infer_type_from_brand(
            m.brand,
            f"{m.brand or ''} {m.model or ''} {m.machine_url or ''}"
        )
        if inferred:
            m.machine_type = inferred
            m.type_normalized = inferred
            updated += 1

    await db.commit()
    return {"updated": updated, "total_checked": len(machines)}


@router.post("/crawl/fix-stuck")
async def fix_stuck_crawls(
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    """
    Reset crawl logs and website statuses that are stuck in 'running'
    state (e.g. after a server restart or crash).
    """
    from sqlalchemy import update as sql_update

    # Fix stuck crawl logs
    result = await db.execute(
        sql_update(CrawlLog)
        .where(CrawlLog.status == "running", CrawlLog.finished_at == None)
        .values(
            status="error",
            error_details="Reset: server restarted while crawl was running",
            finished_at=datetime.now(timezone.utc),
        )
        .returning(CrawlLog.id)
    )
    fixed_logs = result.scalars().all()

    # Fix stuck website statuses
    await db.execute(
        sql_update(Website)
        .where(Website.crawl_status == "running")
        .values(crawl_status="error")
    )

    return {"fixed_crawl_logs": len(fixed_logs), "message": "Stuck crawls reset to error"}


# ── Machine Table ─────────────────────────────────────────────────────────────

@router.get("/machines")
async def list_machines(
    skip: int = 0,
    limit: int = 50,
    website_id: Optional[int] = None,
    machine_type: Optional[str] = None,
    brand: Optional[str] = None,
    q: Optional[str] = None,
    is_active: Optional[bool] = None,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    stmt = select(Machine).order_by(Machine.created_at.desc())
    if website_id:
        stmt = stmt.where(Machine.website_id == website_id)
    if machine_type:
        stmt = stmt.where(Machine.type_normalized.ilike(f"%{machine_type}%"))
    if brand:
        stmt = stmt.where(Machine.brand_normalized.ilike(f"%{brand}%"))
    if q:
        search = f"%{q}%"
        from sqlalchemy import or_
        stmt = stmt.where(
            or_(
                Machine.model.ilike(search),
                Machine.brand.ilike(search),
                Machine.machine_type.ilike(search),
            )
        )
    if is_active is not None:
        stmt = stmt.where(Machine.is_active == is_active)

    total = (await db.execute(select(func.count()).select_from(stmt.subquery()))).scalar()
    rows = (await db.execute(stmt.offset(skip).limit(limit))).scalars().all()

    # Return plain dicts — avoids async lazy-load on images/specs
    items = [
        {
            "id": m.id,
            "website_id": m.website_id,
            "machine_type": m.machine_type,
            "brand": m.brand,
            "model": m.model,
            "price": float(m.price) if m.price else None,
            "currency": m.currency,
            "location": m.location,
            "description": m.description,
            "machine_url": m.machine_url,
            "website_source": m.website_source,
            "thumbnail_url": m.thumbnail_url,
            "is_active": m.is_active,
            "created_at": m.created_at.isoformat() if m.created_at else None,
            "updated_at": m.updated_at.isoformat() if m.updated_at else None,
        }
        for m in rows
    ]

    return {"total": total, "items": items}


@router.post("/machines", status_code=201)
async def create_machine(
    payload: dict,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    from app.models.website import Website as WebsiteModel
    website_id = payload.get("website_id")
    if not website_id:
        raise HTTPException(status_code=400, detail="website_id is required")
    site = (await db.execute(select(WebsiteModel).where(WebsiteModel.id == website_id))).scalar_one_or_none()
    if not site:
        raise HTTPException(status_code=404, detail="Website not found")

    machine = Machine(
        website_id=website_id,
        machine_type=payload.get("machine_type"),
        brand=payload.get("brand"),
        model=payload.get("model"),
        price=payload.get("price"),
        currency=payload.get("currency", "USD"),
        location=payload.get("location"),
        description=payload.get("description"),
        machine_url=payload.get("machine_url", ""),
        website_source=site.name,
        is_active=payload.get("is_active", True),
    )
    db.add(machine)
    await db.flush()
    return {
        "id": machine.id,
        "machine_type": machine.machine_type,
        "brand": machine.brand,
        "model": machine.model,
        "price": float(machine.price) if machine.price else None,
        "currency": machine.currency,
        "location": machine.location,
        "description": machine.description,
        "machine_url": machine.machine_url,
        "website_source": machine.website_source,
        "is_active": machine.is_active,
    }


@router.patch("/machines/{machine_id}")
async def admin_update_machine(
    machine_id: int,
    payload: MachineUpdate,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    result = await db.execute(select(Machine).where(Machine.id == machine_id))
    machine = result.scalar_one_or_none()
    if not machine:
        raise HTTPException(status_code=404, detail="Machine not found")

    for field, value in payload.model_dump(exclude_unset=True).items():
        setattr(machine, field, value)
    await db.flush()

    return {
        "id": machine.id,
        "machine_type": machine.machine_type,
        "brand": machine.brand,
        "model": machine.model,
        "price": float(machine.price) if machine.price else None,
        "currency": machine.currency,
        "location": machine.location,
        "description": machine.description,
        "machine_url": machine.machine_url,
        "website_source": machine.website_source,
        "is_active": machine.is_active,
    }


@router.delete("/machines/{machine_id}", status_code=204)
async def admin_delete_machine(
    machine_id: int,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    result = await db.execute(select(Machine).where(Machine.id == machine_id))
    machine = result.scalar_one_or_none()
    if not machine:
        raise HTTPException(status_code=404, detail="Machine not found")
    await db.delete(machine)


@router.get("/machines/export/excel")
async def export_machines_excel(
    website_id: Optional[int] = None,
    machine_type: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    stmt = select(Machine)
    if website_id:
        stmt = stmt.where(Machine.website_id == website_id)
    if machine_type:
        stmt = stmt.where(Machine.type_normalized.ilike(f"%{machine_type}%"))

    rows = (await db.execute(stmt)).scalars().all()

    data = [
        {
            "ID": m.id,
            "Type": m.machine_type,
            "Brand": m.brand,
            "Model": m.model,
            "Price": float(m.price) if m.price else None,
            "Currency": m.currency,
            "Location": m.location,
            "Website": m.website_source,
            "URL": m.machine_url,
            "Created": m.created_at.isoformat() if m.created_at else None,
        }
        for m in rows
    ]

    df = pd.DataFrame(data)
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Machines")
    buffer.seek(0)

    filename = f"zoogle_machines_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    return StreamingResponse(
        buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


# ── Crawl Logs ────────────────────────────────────────────────────────────────

@router.get("/crawl-logs")
async def list_crawl_logs(
    skip: int = 0,
    limit: int = 50,
    website_id: Optional[int] = None,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    stmt = (
        select(CrawlLog, Website.name, Website.url)
        .outerjoin(Website, CrawlLog.website_id == Website.id)
        .order_by(CrawlLog.started_at.desc())
    )
    if website_id:
        stmt = stmt.where(CrawlLog.website_id == website_id)

    total = (await db.execute(
        select(func.count()).select_from(CrawlLog)
        .where(CrawlLog.website_id == website_id if website_id else True)
    )).scalar()

    rows = (await db.execute(stmt.offset(skip).limit(limit))).all()

    return {
        "total": total,
        "items": [
            {
                "id": c.id,
                "website_id": c.website_id,
                "website_name": name or f"Website #{c.website_id}",
                "website_url": url,
                "task_id": c.task_id,
                "status": c.status,
                "machines_found": c.machines_found,
                "machines_new": c.machines_new,
                "machines_updated": c.machines_updated,
                "errors_count": c.errors_count,
                "error_details": c.error_details,
                "log_output": c.log_output,
                "started_at": c.started_at,
                "finished_at": c.finished_at,
            }
            for c, name, url in rows
        ],
    }
