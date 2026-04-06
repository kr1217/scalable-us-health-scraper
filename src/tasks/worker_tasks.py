import asyncio
from ..core.config import settings
from ..core.database import AsyncSessionLocal, get_mongo_client, get_mongo_db
from ..core.models import Lead, RawData
from ..core.orm_models import LeadORM
from ..scrapers.reddit import RedditScraper
from .celery_app import celery_app

# Initialize scraper centrally
scraper = RedditScraper()

@celery_app.task(name='src.tasks.worker_tasks.scrape_subreddit_task', bind=True, max_retries=3)
def scrape_subreddit_task(self, subreddit_name: str):
    """Celery task to scrape a single subreddit and store the results."""
    # Run the async scraping flow using asyncio.run
    return asyncio.run(run_scraping_flow(subreddit_name))

async def run_scraping_flow(subreddit_name: str):
    """Async flow to scrape and store lead data."""
    print(f"Executing scraping task for: {subreddit_name}")
    
    # 1. Scrape via Playwright
    leads = await scraper.scrape(subreddit_name)
    
    # 2. Save to Databases
    client = get_mongo_client()
    mongo_db = await get_mongo_db(client)
    async with AsyncSessionLocal() as pg_session:
        for lead in leads:
            # Only save to Postgres if we found SOMETHING in the lead
            if (lead.first_name or lead.phone_number or lead.state) and lead.first_name != "unknown":
                # Create ORM model
                lead_orm = LeadORM(
                    first_name=lead.first_name,
                    last_name=lead.last_name,
                    date_of_birth=lead.date_of_birth,
                    phone_number=lead.phone_number,
                    address=lead.address,
                    city=lead.city,
                    state=lead.state,
                    disease_history=lead.disease_history,
                    source=lead.source,
                    source_url=lead.source_url,
                    source_id=lead.source_id,
                    extracted_at=lead.extracted_at
                )
                pg_session.add(lead_orm)
                print(f"QUEUED LEAD FOR DB: {lead.first_name} {lead.last_name}")
            
            # 3. Store raw data in MongoDB (regardless of extraction success)
            raw_entry = {
                "source": "reddit",
                "subreddit": subreddit_name,
                "url": lead.source_url,
                "timestamp": lead.extracted_at.isoformat(),
                "extracted_lead": lead.model_dump() if lead.first_name else None
            }
            # This is where the authentication was failing previously
            await mongo_db.raw_scrapes.insert_one(raw_entry)
            
        await pg_session.commit()

    return {"status": "success", "subreddit": subreddit_name, "leads_queued": len(leads)}
