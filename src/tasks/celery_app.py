from celery import Celery, signals
from ..core.config import settings

# Initialize Celery app
celery_app = Celery(
    settings.PROJECT_NAME,
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
    include=['src.tasks.worker_tasks']
)

# Optional configuration
celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_acks_late=True,
    worker_concurrency=1,  # Playwright scrapers should run with concurrency=1 per worker
    task_time_limit=300,   # 5 minutes max
)

@signals.worker_ready.connect
def setup_tables(**kwargs):
    import asyncio
    from ..core.database import create_tables
    try:
        asyncio.run(create_tables())
        print("--- ✅ Database tables verified/created ---")
    except Exception as e:
        print(f"--- ❌ Table creation failed: {e} ---")

if __name__ == '__main__':
    celery_app.start()
