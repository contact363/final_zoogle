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
    # Create tables on startup (use Alembic in production)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Ensure media dir exists
    os.makedirs(settings.MEDIA_DIR, exist_ok=True)

    # Seed admin user
    await seed_admin()

    logger.info(f"Zoogle API started — {settings.APP_NAME} v{settings.APP_VERSION}")
    yield
    await engine.dispose()


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
