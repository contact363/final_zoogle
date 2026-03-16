"""
Zoogle FastAPI application entry point.
"""
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse
from loguru import logger
import traceback

from app.config import settings
from app.database import engine, Base
from app.routers import auth, search, machines, users, admin


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Create any missing tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Add any columns that exist in models but not yet in the DB
    await auto_migrate()

    # Ensure media dir exists
    os.makedirs(settings.MEDIA_DIR, exist_ok=True)

    # Seed admin user
    await seed_admin()

    # Reset any crawl logs stuck in 'running' from a previous crash
    await reset_stuck_crawls()

    logger.info(f"Zoogle API started — {settings.APP_NAME} v{settings.APP_VERSION}")
    yield
    await engine.dispose()


async def auto_migrate():
    """
    Safely add any columns that exist in the SQLAlchemy models but are missing
    from the live database.  Uses ALTER TABLE … ADD COLUMN IF NOT EXISTS so it
    is idempotent and safe to run on every startup.

    This handles the case where Alembic migrations were not run after a deploy
    (e.g. Render free tier where manual migration is inconvenient).
    """
    from sqlalchemy import text

    migrations = [
        # 0002 — extended training rules
        "ALTER TABLE website_training_rules ADD COLUMN IF NOT EXISTS crawl_type           VARCHAR(20)  DEFAULT 'auto'",
        "ALTER TABLE website_training_rules ADD COLUMN IF NOT EXISTS use_playwright       BOOLEAN      DEFAULT FALSE",
        "ALTER TABLE website_training_rules ADD COLUMN IF NOT EXISTS api_url              TEXT",
        "ALTER TABLE website_training_rules ADD COLUMN IF NOT EXISTS api_key              TEXT",
        "ALTER TABLE website_training_rules ADD COLUMN IF NOT EXISTS api_headers_json     TEXT",
        "ALTER TABLE website_training_rules ADD COLUMN IF NOT EXISTS api_data_path        VARCHAR(255)",
        "ALTER TABLE website_training_rules ADD COLUMN IF NOT EXISTS api_pagination_param VARCHAR(50)",
        "ALTER TABLE website_training_rules ADD COLUMN IF NOT EXISTS api_page_size        INTEGER",
        "ALTER TABLE website_training_rules ADD COLUMN IF NOT EXISTS field_map_json       TEXT",
        "ALTER TABLE website_training_rules ADD COLUMN IF NOT EXISTS product_link_pattern TEXT",
        "ALTER TABLE website_training_rules ADD COLUMN IF NOT EXISTS skip_url_patterns    TEXT",
        "ALTER TABLE website_training_rules ADD COLUMN IF NOT EXISTS request_delay        NUMERIC(5,2)",
        "ALTER TABLE website_training_rules ADD COLUMN IF NOT EXISTS max_items            INTEGER",
        # 0002 — crawl log extra counters
        "ALTER TABLE crawl_logs ADD COLUMN IF NOT EXISTS machines_updated INTEGER DEFAULT 0",
        "ALTER TABLE crawl_logs ADD COLUMN IF NOT EXISTS machines_skipped INTEGER DEFAULT 0",
        # 0003 — stock number and cross-language dedup key
        "ALTER TABLE machines ADD COLUMN IF NOT EXISTS stock_number     VARCHAR(100)",
        "ALTER TABLE machines ADD COLUMN IF NOT EXISTS dedup_key        VARCHAR(64)",
        # 0004 — staleness tracking for inactive-machine cleanup
        "ALTER TABLE machines ADD COLUMN IF NOT EXISTS last_crawled_at  TIMESTAMPTZ",
        # 0005 — two-phase discovery
        "ALTER TABLE websites    ADD COLUMN IF NOT EXISTS discovered_count       INTEGER",
        "ALTER TABLE websites    ADD COLUMN IF NOT EXISTS discovery_status       VARCHAR(50) DEFAULT 'pending'",
        "ALTER TABLE crawl_logs  ADD COLUMN IF NOT EXISTS log_type               VARCHAR(20) DEFAULT 'crawl'",
        # 0006 — URL collection phase
        "ALTER TABLE websites    ADD COLUMN IF NOT EXISTS urls_collected         INTEGER",
        "ALTER TABLE websites    ADD COLUMN IF NOT EXISTS url_collection_status  VARCHAR(50) DEFAULT 'pending'",
    ]

    index_migrations = [
        "CREATE INDEX IF NOT EXISTS ix_machines_stock_website ON machines (stock_number, website_id) WHERE stock_number IS NOT NULL",
        "CREATE INDEX IF NOT EXISTS ix_machines_dedup_key     ON machines (dedup_key)                WHERE dedup_key    IS NOT NULL",
    ]

    async with engine.begin() as conn:
        for stmt in migrations:
            try:
                await conn.execute(text(stmt))
            except Exception as exc:
                logger.warning(f"auto_migrate skipped: {exc!s:.120}")

        for stmt in index_migrations:
            try:
                await conn.execute(text(stmt))
            except Exception as exc:
                logger.debug(f"auto_migrate index skipped: {exc!s:.80}")

    logger.info("auto_migrate: schema is up to date")


async def reset_stuck_crawls():
    """On startup, reset any crawls stuck in 'running' (server was restarted mid-crawl)."""
    from datetime import datetime, timezone
    from sqlalchemy import update as sql_update
    from app.database import AsyncSessionLocal
    from app.models.crawl_log import CrawlLog
    from app.models.website import Website

    async with AsyncSessionLocal() as db:
        await db.execute(
            sql_update(CrawlLog)
            .where(CrawlLog.status == "running", CrawlLog.finished_at == None)
            .values(
                status="error",
                error_details="Server restarted while crawl was in progress",
                finished_at=datetime.now(timezone.utc),
            )
        )
        await db.execute(
            sql_update(Website).where(Website.crawl_status == "running").values(crawl_status="error")
        )
        await db.commit()
    logger.info("Stuck crawls cleaned up on startup")


async def seed_admin():
    """Create default admin account on first run."""
    from sqlalchemy import select
    from app.database import AsyncSessionLocal
    from app.models.user import User
    from app.utils.security import hash_password

    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(User).where(User.email == settings.ADMIN_EMAIL)
        )
        if not result.scalar_one_or_none():
            admin = User(
                email=settings.ADMIN_EMAIL,
                hashed_password=hash_password(settings.ADMIN_PASSWORD),
                full_name="Zoogle Admin",
                is_admin=True,
            )
            db.add(admin)
            await db.commit()
            logger.info(f"Admin user seeded: {settings.ADMIN_EMAIL}")


app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="Global Industrial Machine Search Engine",
    lifespan=lifespan,
)


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    tb = traceback.format_exc()
    logger.error(f"Unhandled exception on {request.method} {request.url}\n{tb}")
    return JSONResponse(
        status_code=500,
        content={"detail": f"{type(exc).__name__}: {str(exc)}"},
    )

# ── CORS ──────────────────────────────────────────────────────────────────────
ALLOWED_ORIGINS = [
    "https://final-zoogle-frontend.onrender.com",
    "http://localhost:3000",
    "http://127.0.0.1:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Static media files ────────────────────────────────────────────────────────
os.makedirs("media", exist_ok=True)
app.mount("/media", StaticFiles(directory="media"), name="media")

# ── Routers ───────────────────────────────────────────────────────────────────
app.include_router(auth.router)
app.include_router(search.router)
app.include_router(machines.router)
app.include_router(users.router)
app.include_router(admin.router)


@app.get("/", tags=["health"])
async def root():
    return {"name": settings.APP_NAME, "version": settings.APP_VERSION, "status": "ok"}


@app.get("/health", tags=["health"])
async def health():
    return {"status": "ok"}
