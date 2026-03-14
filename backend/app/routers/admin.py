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
from app.schemas.website import WebsiteCreate, WebsiteRead, WebsiteUpdate
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
            select(CrawlLog).order_by(desc(CrawlLog.started_at)).limit(5)
        )
    ).scalars().all()

    return {
        "total_machines": total_machines,
        "total_websites": total_websites,
        "total_users": total_users,
        "total_searches": total_searches,
        "recent_crawls": [
            {
                "id": c.id,
                "website_id": c.website_id,
                "status": c.status,
                "machines_new": c.machines_new,
                "started_at": c.started_at,
                "finished_at": c.finished_at,
            }
            for c in recent_crawls
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
        for site in sites:
            t = threading.Thread(target=run_crawl_direct, args=(site.id,), daemon=True)
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
    stmt = select(CrawlLog).order_by(CrawlLog.started_at.desc())
    if website_id:
        stmt = stmt.where(CrawlLog.website_id == website_id)

    total = (await db.execute(select(func.count()).select_from(stmt.subquery()))).scalar()
    rows = (await db.execute(stmt.offset(skip).limit(limit))).scalars().all()

    return {
        "total": total,
        "items": [
            {
                "id": c.id,
                "website_id": c.website_id,
                "task_id": c.task_id,
                "status": c.status,
                "machines_found": c.machines_found,
                "machines_new": c.machines_new,
                "machines_updated": c.machines_updated,
                "errors_count": c.errors_count,
                "error_details": c.error_details,
                "started_at": c.started_at,
                "finished_at": c.finished_at,
            }
            for c in rows
        ],
    }
