import asyncio
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from sqlalchemy import select, update
from ..core.database import AsyncSessionLocal
from ..core.orm_models import EnrichedProfileORM
from ..core.config import settings
from .whatsmyname import WhatsMyNameEnricher
from .social_scanner import SocialScannerEnricher
from .blackbird import BlackbirdEnricher
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

class EnrichmentManager:
    def __init__(self):
        self.enrichers = [
            WhatsMyNameEnricher(),
            SocialScannerEnricher(),
            BlackbirdEnricher(),
        ]
        # Recommendation #10: Limit concurrency to avoid global rate limits
        self.semaphore = asyncio.Semaphore(3) 

    async def get_or_enrich(self, identifier: str, identifier_type: str, source: Optional[str] = None) -> Optional[dict]:
        """
        Orchestrates enrichment: check source gate, then cache, then APIs.
        """
        # 0. Source Gate
        enabled_sources = []
        if settings.ENRICHMENT_ENABLED_SOURCES:
            enabled_sources = [s.strip().lower() for s in settings.ENRICHMENT_ENABLED_SOURCES.split(",")]
            if source and source.lower() not in enabled_sources:
                return None

        # 1. Check cache
        async with AsyncSessionLocal() as session:
            stmt = select(EnrichedProfileORM).where(EnrichedProfileORM.identifier == identifier)
            result = await session.execute(stmt)
            cached = result.scalar_one_or_none()
            
            if cached:
                # Check TTL
                ttl_delta = timedelta(hours=settings.ENRICHMENT_CACHE_TTL_HOURS)
                if datetime.utcnow() < cached.last_enrich_at + ttl_delta:
                    logger.info(f"[Enrichment] Cache hit for {identifier}")
                    return cached.data
                else:
                    logger.info(f"[Enrichment] Cache expired for {identifier}, re-enriching...")

        # 2. Call all enrichers in parallel
        logger.info(f"[Enrichment] Starting enrichment for {identifier} ({identifier_type})")
        
        tasks = []
        for enricher in self.enrichers:
            tasks.append(self._wrapped_enrich(enricher, identifier, identifier_type))
            
        results = await asyncio.gather(*tasks)
        
        # 3. Merge results
        merged_profiles = []
        raw_responses = {}
        
        for res in results:
            if res:
                # Merge profile lists (avoid duplicates)
                found = res.get("other_profiles", [])
                for profile in found:
                    if profile not in merged_profiles:
                        merged_profiles.append(profile)
                
                # Merge raw responses for debugging
                source_name = res.get("raw_response", {}).get("source", "unknown")
                raw_responses[source_name] = res.get("raw_response")

        if not merged_profiles:
            logger.info(f"[Enrichment] No results found for {identifier}")
            return None

        merged_data = {
            "other_profiles": merged_profiles,
            "raw_responses": raw_responses,
            "merged_at": datetime.utcnow().isoformat()
        }

        # 4. Store in cache
        async with AsyncSessionLocal() as session:
            try:
                if cached:
                    # Update existing
                    await session.execute(
                        update(EnrichedProfileORM)
                        .where(EnrichedProfileORM.identifier == identifier)
                        .values(
                            data=merged_data,
                            other_profiles=merged_profiles,
                            last_enrich_at=datetime.utcnow()
                        )
                    )
                else:
                    # Insert new
                    profile = EnrichedProfileORM(
                        identifier=identifier,
                        identifier_type=identifier_type,
                        data=merged_data,
                        other_profiles=merged_profiles
                    )
                    session.add(profile)
                
                await session.commit()
                logger.info(f"[Enrichment] Cached result for {identifier}")
            except Exception as e:
                logger.error(f"[Enrichment] Failed to cache result for {identifier}: {e}")
                await session.rollback()
        
        return merged_data

    async def _wrapped_enrich(self, enricher, identifier, identifier_type) -> Optional[dict]:
        """Wrapper with semaphore and smart-skip logic."""
        name = enricher.__class__.__name__
        
        # Recommendation #9: Skip Blackbird unless explicitly enabled for bulk run
        # We check if 'blackbird' is in the allowed enrichment tools (if defined)
        if name == "BlackbirdEnricher":
            # If fast mode is preferred, or blackbird isn't explicitly in the tools list, skip
            allowed_tools = settings.ENRICHMENT_ENABLED_SOURCES.lower() if settings.ENRICHMENT_ENABLED_SOURCES else ""
            if "blackbird" not in allowed_tools:
                return None

        async with self.semaphore:
            try:
                res = await enricher.enrich(identifier, identifier_type)
                if res:
                    logger.info(f"[Enrichment] {name} HIT for {identifier}")
                else:
                    logger.info(f"[Enrichment] {name} MISS for {identifier}")
                return res
            except Exception as e:
                logger.warning(f"[Enrichment] {name} ERROR for {identifier}: {e}")
                return None
