import asyncio
from ..core.config import settings
from ..core.database import AsyncSessionLocal, get_mongo_client, get_mongo_db
from ..core.models import Lead, RawData
from ..core.orm_models import LeadORM
from ..scrapers.reddit import RedditScraper
from ..scrapers.twitter import TwitterScraper
from .celery_app import celery_app

# Initialize scraper centrally
scraper = RedditScraper()
twitter_scraper = TwitterScraper()

@celery_app.task(name='src.tasks.worker_tasks.scrape_subreddit_task', bind=True, max_retries=3)
def scrape_subreddit_task(self, subreddit_name: str):
    """Celery task to scrape a single subreddit and store the results."""
    # Run the async scraping flow using asyncio.run
    return asyncio.run(run_scraping_flow(subreddit_name))

@celery_app.task(name='src.tasks.worker_tasks.scrape_twitter_task', bind=True, max_retries=3)
def scrape_twitter_task(self, query: str, max_results: int = 50):
    """Celery task to scrape Twitter for a health query."""
    return asyncio.run(run_twitter_scraping(query, max_results))

async def run_scraping_flow(subreddit_name: str):
    """Async flow to trigger Reddit scraping."""
    print(f"Executing scraping task for: {subreddit_name}")
    success = await scraper.scrape(subreddit_name)
    return {"status": "success" if success else "failed", "subreddit": subreddit_name}

async def run_twitter_scraping(query: str, max_results: int):
    """Async flow to trigger Twitter API scraping."""
    print(f"Executing Twitter API task for: {query}")
    success = await twitter_scraper.scrape(query, max_results)
    return {"status": "success" if success else "failed", "query": query}
