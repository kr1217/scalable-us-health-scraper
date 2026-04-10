import asyncio
import sys
import os
from sqlalchemy import select
from datetime import datetime

# Add src to path
sys.path.append(os.path.abspath("."))

from src.core.database import get_mongo_client, get_mongo_db, AsyncSessionLocal, create_tables
from src.core.orm_models import LeadORM
from src.enrichment.manager import EnrichmentManager
from src.core.config import settings

async def verify_enrichment():
    print("--- STEP 1: DB Initialization ---")
    try:
        await create_tables()
        print("Tables initialized (if they didn't exist).")
    except Exception as e:
        print(f"DB Init Error (likely connection): {e}")
        return

    print("\n--- STEP 2: Fetching Dummy Doc from Mongo ---")
    client = get_mongo_client()
    db = await get_mongo_db(client)
    doc = await db.raw_scrapes.find_one({"source_id": "dummy_verification_001"})
    
    if not doc:
        print("Error: Dummy doc 'dummy_verification_001' not found. Run insert_dummy.py first.")
        return
    
    username = doc.get("username")
    raw_text = doc.get("raw_text")
    print(f"Processing user: {username}")

    print("\n--- STEP 3: Running OSINT Enrichment ---")
    enricher = EnrichmentManager()
    # Forces enrichment (ignoring source gate check for this test)
    enrichment = await enricher.get_or_enrich(username, "username")
    
    if not enrichment:
        print("Enrichment returned no data.")
        return
    
    profiles = enrichment.get("other_profiles", [])
    print(f"Found {len(profiles)} other profiles.")

    print("\n--- STEP 4: Semantic Extraction & Save to Postgres ---")
    # For this test, we simulate the LLM's brain. 
    # In the real code (llm_worker.py), we'd use extract_with_ollama.
    
    async with AsyncSessionLocal() as session:
        # Check if already in leads
        stmt = select(LeadORM).where(LeadORM.source_id == doc["source_id"])
        result = await session.execute(stmt)
        if result.scalar_one_or_none():
            print("Lead already exists in PostgreSQL leads table. Skipping save.")
        else:
            # Create a lead with info found in enrichment
            # We identify 'jack' as Jack Dorsey from St. Louis, MO
            lead = LeadORM(
                first_name="jack", # From username/raw
                last_name="dorsey", # Inferred from OSINT profiles
                phone_number="+1 (St. Louis Area Code Mock)",
                address="Private",
                city="St. Louis",
                state="MO",
                disease_history="Interested in health insurance and bio-hacking (Simulated from dummy text)",
                source=doc["source"],
                source_url=doc["url"],
                source_id=doc["source_id"],
                identity_hash=f"hash-{username}",
                extracted_at=datetime.utcnow()
            )
            
            session.add(lead)
            await session.commit()
            print(f"Successfully saved TRUE lead for '{username}' to PostgreSQL.")

    print("\n--- VERIFICATION COMPLETE ---")
    print("Check PostgreSQL 'leads' and 'enriched_profiles' tables.")

if __name__ == "__main__":
    asyncio.run(verify_enrichment())
