import asyncio
import random
import httpx
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List
from sqlalchemy import select

from ..utils.proxy_manager import proxy_manager
from ..utils.stealth import get_stealth_headers
from ..core.config import settings
from ..core.database import AsyncSessionLocal
from ..core.orm_models import LeadORM

from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

class BaseScraper(ABC):
    def __init__(self):
        self.proxy_manager = proxy_manager
        self.current_proxy = None
        self.delay = settings.DELAY_BETWEEN_SCRAPES
        self.jitter = settings.DELAY_JITTER

    async def _get_next_proxy(self) -> Optional[str]:
        self.current_proxy = await self.proxy_manager.get_next_proxy()
        return self.current_proxy

    async def _rate_limit_sleep(self):
        """Apply delay + jitter between requests to mimic human behavior."""
        wait = self.delay + random.uniform(-self.jitter, self.jitter)
        await asyncio.sleep(max(0.1, wait))

    async def is_already_scraped(self, source_id: str) -> bool:
        """Centralized check to see if a post ID has already been persisted."""
        if not source_id:
            return False
            
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(LeadORM).where(LeadORM.source_id == source_id)
            )
            return result.scalar_one_or_none() is not None

    async def _request_with_retry(
        self, 
        url: str, 
        method: str = "GET", 
        max_retries: int = 5
    ) -> Optional[httpx.Response]:
        """
        Make an HTTP request with agile timeouts, proxy rotation, 
        and randomized exponential backoff.
        """
        # Agile Timeouts (5s Connection Probe, 15s Read window)
        timeout = httpx.Timeout(20.0, connect=5.0, read=15.0)
        
        for attempt in range(max_retries):
            if not self.current_proxy:
                await self._get_next_proxy()
            
            # Expert Patch: Direct Fallback if pool is empty
            if not self.current_proxy:
                logger.error("[SECURITY WARNING] No proxies available. Attempting DIRECT CONNECTION (Local IP visible).")
                
            try:
                headers = get_stealth_headers()
                async with httpx.AsyncClient(proxy=self.current_proxy, timeout=timeout) as client:
                    resp = await client.request(method, url, headers=headers)
                    
                    if resp.status_code == 200:
                        return resp
                        
                    # Handle blocks/rate-limits with instant rotation
                    if resp.status_code in [403, 407, 429]:
                        logger.warning(f"[RESILIENCE] {resp.status_code} detected for {url}. Rotating proxy...")
                        self.proxy_manager.report_failure(self.current_proxy, resp.status_code)
                        await self._get_next_proxy()
                        continue
                        
                    resp.raise_for_status()
                    
            except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError) as e:
                logger.warning(f"[RESILIENCE] Network error {type(e).__name__} at {url}. Rotating proxy...")
                self.proxy_manager.report_failure(self.current_proxy)
                await self._get_next_proxy()
                
                # Randomized Exponential Backoff: (2^attempt) * jitter(1-3s)
                backoff = (2 ** attempt) * random.uniform(1.0, 3.0)
                await asyncio.sleep(min(backoff, 30.0))
                
            except Exception as e:
                logger.warning(f"[RESILIENCE] Unexpected error for {url}: {e}")
                break
                
        return None

    @abstractmethod
    async def scrape(self, target: str) -> bool:
        """Scrape the target and store raw data in MongoDB."""
        pass
