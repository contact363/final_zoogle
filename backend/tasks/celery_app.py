from celery import Celery
from celery.schedules import crontab
from app.config import settings

celery_app = Celery(
    "zoogle",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
    include=["tasks.crawl_tasks"],
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    # Beat schedule – crawl all sites daily at 2 AM UTC
    beat_schedule={
        "daily-crawl-all": {
            "task": "tasks.crawl_tasks.crawl_all_websites_task",
            "schedule": crontab(hour=2, minute=0),
        },
    },
)
