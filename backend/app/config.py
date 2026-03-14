from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # Application
    APP_NAME: str = "Zoogle"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = False
    SECRET_KEY: str = "change-me-in-production-use-strong-random-key"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24  # 24 hours

    # Database
    # - Render production: uses internal hostname (dpg-xxx-a) via env var set in dashboard
    # - Local dev: uses external hostname (.oregon-postgres.render.com) from .env file
    DATABASE_URL: str = "postgresql+asyncpg://final_zoogle_db_user:A8D7GMXiIYmc20g6ZqhfGOfHW9ofXRAz@dpg-d6qjt7haae7s739hqia0-a.oregon-postgres.render.com/final_zoogle_db"
    DATABASE_SYNC_URL: str = "postgresql://final_zoogle_db_user:A8D7GMXiIYmc20g6ZqhfGOfHW9ofXRAz@dpg-d6qjt7haae7s739hqia0-a.oregon-postgres.render.com/final_zoogle_db"

    # Redis / Celery
    REDIS_URL: str = "redis://localhost:6379/0"
    CELERY_BROKER_URL: str = "redis://localhost:6379/0"
    CELERY_RESULT_BACKEND: str = "redis://localhost:6379/1"

    # Media storage
    MEDIA_DIR: str = "media/machines"
    BASE_URL: str = "http://localhost:8000"

    # Crawl settings
    CRAWL_CONCURRENT_REQUESTS: int = 16
    CRAWL_DOWNLOAD_DELAY: float = 1.0
    CRAWL_AUTOTHROTTLE: bool = True

    # Search
    SEARCH_DEFAULT_LIMIT: int = 20
    SEARCH_MAX_LIMIT: int = 100

    # Admin
    ADMIN_EMAIL: str = "admin@zoogle.com"
    ADMIN_PASSWORD: str = "admin123"

    class Config:
        env_file = ".env"
        case_sensitive = True


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
