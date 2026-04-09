import asyncio
from ..core.database import get_mongo_client, get_mongo_db, AsyncSessionLocal
from ..core.orm_models import LeadORM
from ..pipelines.llm_extract import extract_with_ollama
from .celery_app import celery_app
from sqlalchemy import select

@celery_app.task(name='src.tasks.llm_worker.process_raw_posts')
def process_raw_posts(batch_size=10):
    """
    Periodic task to process raw scrapes using Ollama and migrate to Postgres.
    """
    return asyncio.run(run_llm_processing(batch_size))

async def run_llm_processing(batch_size: int):
    client = get_mongo_client()
    try:
        db = await get_mongo_db(client)
        
        # 1. Fetch unprocessed documents (Newest first)
        cursor = db.raw_scrapes.find({"llm_processed": False}).sort("_id", -1).limit(batch_size)
        docs = await cursor.to_list(length=batch_size)
        
        if not docs:
            return {"status": "idle", "processed": 0}

        print(f"[LLM Worker] Processing {len(docs)} raw documents...")
        
        processed_count = 0
        async with AsyncSessionLocal() as pg_session:
            for doc in docs:
                source_id = doc.get("source_id")
                source_url = doc.get("url", "")
                
                # 2. SELECITVE SKIP: Check if this post ID was already processed
                if source_id:
                    existing_post = await pg_session.execute(
                        select(LeadORM).where(LeadORM.source_id == source_id)
                    )
                    if existing_post.scalar_one_or_none():
                        print(f"[LLM Worker] Skipping duplicate post ID: {source_id}")
                        await db.raw_scrapes.update_one(
                            {"_id": doc["_id"]},
                            {"$set": {"llm_processed": True, "processed_at": asyncio.get_event_loop().time()}}
                        )
                        continue

                # 3. Extract via LLM
                raw_text = doc.get("raw_text", "")
                lead = await extract_with_ollama(raw_text)
                
                if lead and (lead.first_name or lead.disease_history):
                    # 4. IDENTITY HASH CHECK: Fingerprint the person
                    from ..utils.identity import generate_identity_hash
                    ident_hash = generate_identity_hash(
                        lead.first_name, lead.last_name, lead.phone_number, lead.state
                    )
                    
                    # Expert Patch: Skip identity check if no identifying info exists
                    if ident_hash:
                        existing_ident = await pg_session.execute(
                            select(LeadORM).where(LeadORM.identity_hash == ident_hash)
                        )
                        if existing_ident.scalar_one_or_none():
                            print(f"[LLM Worker] Skipping duplicate IDENTITY ({lead.first_name}): {ident_hash}")
                            # Mark as processed and continue to next doc
                            await db.raw_scrapes.update_one(
                                {"_id": doc["_id"]},
                                {"$set": {"llm_processed": True, "processed_at": asyncio.get_event_loop().time()}}
                            )
                            continue
                    
                    # 5. Save UNIQUE Lead (using Savepoints for transaction safety)
                    async with pg_session.begin_nested():
                        try:
                            lead_orm = LeadORM(
                                first_name=lead.first_name,
                                last_name=lead.last_name,
                                date_of_birth=lead.date_of_birth,
                                phone_number=lead.phone_number,
                                address=lead.address,
                                city=lead.city,
                                state=lead.state,
                                disease_history=lead.disease_history,
                                source=doc.get("source", "unknown"),
                                source_url=source_url,
                                source_id=source_id,
                                identity_hash=ident_hash,
                                extracted_at=lead.extracted_at
                            )
                            pg_session.add(lead_orm)
                            await pg_session.flush() 
                            print(f"[LLM Worker] Saved UNIQUE lead: {lead.first_name} {lead.last_name}")
                        except Exception as e:
                            # Savepoint automatic rollback happens on exit of 'begin_nested'
                            print(f"[LLM Worker] Skipping failed save for {source_url}: {e}")
                
                # Mark as processed in Mongo (even if identity check failed or skipped)
                await db.raw_scrapes.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"llm_processed": True, "processed_at": asyncio.get_event_loop().time()}}
                )
                processed_count += 1
                
            await pg_session.commit()
            
        return {"status": "success", "processed": processed_count}
        
    except Exception as e:
        print(f"[LLM Worker] Error: {e}")
        return {"status": "error", "message": str(e)}
    finally:
        client.close()
