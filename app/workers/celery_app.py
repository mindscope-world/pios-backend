from celery import Celery
from celery.schedules import crontab

from app.core.config import settings

celery_app = Celery(
    "pi_os",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
    include=["app.workers.backtest_worker"],
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    result_expires=86400,  # 24h
    # Canonical beat schedule. backtest_worker.py adds its own entries via
    # `.update()` on import — never reassign celery_app.conf.beat_schedule,
    # or whichever module imports last wins and silently drops these.
    beat_schedule={
        "snapshot-pnl-5min": {
            "task": "snapshot_pnl",
            "schedule": 300.0,  # every 5 minutes
        },
    },
)
