import asyncio
import os
from sqlalchemy import select, func
from motor.motor_asyncio import AsyncIOMotorClient
from src.core.database import AsyncSessionLocal, get_mongo_client
from src.core.orm_models import LeadORM
from src.core.config import settings

async def inspect():
    print("--- 📊 Scraper Results Inspection ---")
    
    # 1. Inspect PostgreSQL (Leads)
    async with AsyncSessionLocal() as session:
        # ... (keep existing Postgres logic)
        result = await session.execute(select(func.count(LeadORM.id)))
        lead_count = result.scalar()
        
        print(f"\n[PostgreSQL] Total Leads Extracted: {lead_count}")
        
        if lead_count > 0:
            print("\nLatest 5 Leads:")
            stmt = select(LeadORM).order_by(LeadORM.extracted_at.desc()).limit(5)
            leads = (await session.execute(stmt)).scalars().all()
            for l in leads:
                dob_info = f" (Born: {l.date_of_birth})" if l.date_of_birth else ""
                info = f" - {l.first_name} {l.last_name}{dob_info}"
                print(f"{info} | {l.state} | {l.phone_number} | Disease: {l.disease_history} | Source: {l.source_url}")

    # 2. Inspect MongoDB (Raw Data)
    try:
        client = get_mongo_client()
        db = client[settings.MONGODB_DB_NAME]
        raw_count = await db.raw_scrapes.count_documents({})
        print(f"\n[MongoDB] Total Raw Posts Scraped: {raw_count}")
        
        if raw_count > 0:
            print("\nRecent Subreddits Scraped:")
            pipeline = [
                {"$group": {"_id": "$subreddit", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
            async for doc in db.raw_scrapes.aggregate(pipeline):
                print(f" - r/{doc['_id']}: {doc['count']} posts")
    except Exception as e:
        print(f"\n[MongoDB] Error connecting: {e}")

    print("\n--- End of Report ---")

if __name__ == "__main__":
    # Ensure .env is loaded if running manually
    from dotenv import load_dotenv
    load_dotenv()
    asyncio.run(inspect())
