import asyncio
from ..core.database import get_mongo_client, get_mongo_db, AsyncSessionLocal
from ..core.orm_models import LeadORM
from ..pipelines.llm_extract import extract_with_ollama
from .celery_app import celery_app
from sqlalchemy import select
from ..core.config import settings
from ..enrichment.manager import EnrichmentManager
from ..enrichment.bio_extractor import bio_extractor

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

                # 3. Multi-Author Discovery & Enrichment
                raw_text = doc.get("raw_text", "")
                main_author = doc.get("username")
                
                # Regex Rule #5: Stricter pattern to avoid false positives (URLs, local-parts)
                # Matches: AUTHOR: user123 or @user123 (excluding email local-parts and path fragments)
                import re
                author_pattern = r'(?<![\/\w])(?:AUTHOR:\s*|@)([a-zA-Z0-9_]{1,30})(?![\/\w])'
                found_authors = set(re.findall(author_pattern, raw_text))
                
                if main_author:
                    found_authors.add(main_author)
                
                # Relevance Limit: Process top 5 unique authors to avoid rate limits
                authors_to_enrich = list(found_authors)[:5]
                print(f"[LLM Worker] Found {len(found_authors)} authors. Enriching {len(authors_to_enrich)}...")
                
                # Recommendation #4: Limit fusion text to avoid token overflows
                fusion_parts = [raw_text[:6000], "\n\n[EXTENDED DIGITAL FOOTPRINTS]:"]
                enrichment_manager = EnrichmentManager()
                
                async def enrich_and_scrape(username):
                    try:
                        enrichment = await enrichment_manager.get_or_enrich(
                            identifier=username, 
                            identifier_type="username",
                            source=doc.get("source")
                        )
                        if enrichment:
                            profile_urls = [p['url'] for p in enrichment.get("other_profiles", [])]
                            if profile_urls:
                                # Recommendation #4: Truncate each bio to 500 chars
                                bio_data = await bio_extractor.get_combined_bios(profile_urls)
                                return f"\n[USER: {username}]\n{bio_data[:500]}"
                    except Exception as e:
                        print(f"[Enrichment Error] Critical failure for {username}: {e}")
                    return None

                # Recommendation #3: return_exceptions=True to prevent thread-wide failure
                enrich_tasks = [enrich_and_scrape(u) for u in authors_to_enrich]
                results = await asyncio.gather(*enrich_tasks, return_exceptions=True)
                
                # Join findings
                for res in results:
                    if isinstance(res, str) and res:
                        fusion_parts.append(res)
                
                # Recommendation #4: Final Fusion Cap at 8000 chars
                fusion_text = "\n".join(fusion_parts)
                if len(fusion_text) > 8000:
                    fusion_text = fusion_text[:8000] + "... [TRUNCATED]"
                
                print(f"[DEBUG] Optimized Fusion Text Size: {len(fusion_text)} characters")

                # 4. Extract via LLM
                leads = await extract_with_ollama(fusion_text)
                
                if leads:
                    for lead in leads:
                        # 5. IDENTITY HASH CHECK: Fingerprint the person
                        from ..utils.identity import generate_identity_hash
                        ident_hash = generate_identity_hash(
                            lead.first_name, lead.last_name, lead.phone_number, lead.state
                        )
                        
                        if ident_hash:
                            existing_ident = await pg_session.execute(
                                select(LeadORM).where(LeadORM.identity_hash == ident_hash)
                            )
                            if existing_ident.scalar_one_or_none():
                                print(f"[LLM Worker] Skipping duplicate IDENTITY ({lead.first_name}): {ident_hash}")
                                continue
                        
                        # 6. Save UNIQUE Lead
                        async with pg_session.begin_nested():
                            try:
                                lead_orm = LeadORM(
                                    first_name=lead.first_name,
                                    last_name=lead.last_name,
                                    date_of_birth=lead.date_of_birth,
                                    phone_number=lead.phone_number,
                                    phone_number_source=lead.phone_number_source, # Recommendation #8
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
                                print(f"[LLM Worker] Saved UNIQUE lead from thread: {lead.first_name} {lead.last_name}")
                            except Exception as e:
                                print(f"[LLM Worker] Skipping failed save for {lead.first_name}: {e}")
                
                # Mark as processed in Mongo
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
