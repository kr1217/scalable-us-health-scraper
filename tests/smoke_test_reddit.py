import asyncio
import os
from src.scrapers.reddit import RedditScraper
from src.core.models import Lead

async def test_run():
    # Set dummy env vars for config if needed
    os.environ["DATABASE_URL"] = "postgresql+asyncpg://scraper:pass@localhost:5432/db"
    os.environ["MONGODB_URI"] = "mongodb://admin:pass@localhost:27017/"
    os.environ["CELERY_BROKER_URL"] = "redis://localhost:6379/0"
    os.environ["CELERY_RESULT_BACKEND"] = "redis://localhost:6379/1"
    
    scraper = RedditScraper()
    print("Starting smoke test for RedditScraper...")
    
    # Scrape 'health' subreddit (just a few posts)
    leads = await scraper.scrape("health")
    
    print(f"Extraction complete. Found {len(leads)} potential leads.")
    for lead in leads:
        if lead.first_name or lead.phone_number:
            print(f"FOUND LEAD: {lead.first_name} {lead.last_name} | Phone: {lead.phone_number} | Source: {lead.source_url}")
        else:
            print(f"Scraped post: {lead.source_url} (No lead data found)")

if __name__ == "__main__":
    asyncio.run(test_run())
