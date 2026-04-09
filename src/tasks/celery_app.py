from celery import Celery, signals
from kombu import Queue
from ..core.config import settings

# Initialize Celery app
celery_app = Celery(
    settings.PROJECT_NAME,
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
    include=['src.tasks.worker_tasks', 'src.tasks.llm_worker']
)

# Advanced Queue Configuration
celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_acks_late=True,
    worker_concurrency=1,
    task_time_limit=300,
    
    # 1. Define physical queues
    task_queues=(
        Queue('default', routing_key='default'),
        Queue('scrape', routing_key='scrape'),
        Queue('llm', routing_key='llm'),
    ),
    
    # 2. Define routing logic
    task_routes={
        'src.tasks.worker_tasks.scrape_subreddit_task': {'queue': 'scrape'},
        'src.tasks.llm_worker.process_raw_posts': {'queue': 'llm'},
    },
    
    # 3. Dedicated Beat schedule
    beat_schedule={
        'process-llm-tasks-every-30-seconds': {
            'task': 'src.tasks.llm_worker.process_raw_posts',
            'schedule': 30.0,
            'args': (10,)
        },
    }
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
