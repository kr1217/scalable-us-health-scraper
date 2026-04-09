import asyncio
import os
from motor.motor_asyncio import AsyncIOMotorClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from src.core.config import settings
from src.core.database import AsyncSessionLocal
from src.core.orm_models import LeadORM

async def audit_twitter():
    print("--- Twitter Data & Lead Audit ---")
    
    # 1. Check MongoDB (Raw Tweets)
    mongo_client = AsyncIOMotorClient(settings.MONGODB_URI)
    db = mongo_client[settings.MONGODB_DB_NAME]
    raw_count = await db.raw_scrapes.count_documents({"source": "twitter_api"})
    print(f"Total RAW Tweets harvested: {raw_count}")
    
    # 2. Check Postgres (Extracted Leads)
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(LeadORM).where(LeadORM.source == "twitter_api")
        )
        leads = result.scalars().all()
        print(f"Total STRUCTURED Leads extracted: {len(leads)}")
        
        if leads:
            print("\n--- Latest Twitter Leads Preview ---")
            # Show top 5
            for i, lead in enumerate(leads[:5]):
                print(f"{i+1}. {lead.first_name} {lead.last_name or ''} | {lead.city or 'N/A'}, {lead.state or 'N/A'}")
                print(f"   Disease: {lead.disease_history[:100]}...")
                print(f"   URL: {lead.source_url}\n")
        else:
            print("\n[INFO] No Twitter leads found in Postgres. They may still be in the LLM processing queue.")

if __name__ == "__main__":
    asyncio.run(audit_twitter())
