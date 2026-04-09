import asyncio
import random
from typing import List, Optional, Dict, Any

from ..core.config import settings
from .base import BaseScraper
from ..utils.mongo_storage import save_raw_document

from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

class RedditScraper(BaseScraper):
    def __init__(self):
        super().__init__()
        self.base_url = settings.REDDIT_BASE_URL
        self.post_limit = settings.SCRAPE_POST_LIMIT

    async def _fetch_comments(self, post_id: str) -> str:
        """Fetch comments using centralized framework logic."""
        url = f"{self.base_url}/comments/{post_id}.json?limit=500&depth=1"
        
        logger.warning(f"  [HEARTBEAT] Attemping deep comment fetch for post {post_id}...")
        
        response = await self._request_with_retry(url)
        if response and response.status_code == 200:
            try:
                data = response.json()
                if len(data) > 1:
                    children = data[1].get("data", {}).get("children", [])
                    comments = [c.get("data", {}).get("body", "") for c in children if c.get("kind") == "t1"]
                    logger.warning(f"  [HEARTBEAT] Successfully retrieved {len(comments)} comments.")
                    return "\n--- COMMENT ---\n".join(comments[:500])
            except Exception as e:
                logger.warning(f"  [HEARTBEAT] ⚠️ Parser error for post {post_id}: {e}")
        return ""

    async def scrape(self, subreddit_name: str) -> bool:
        """Standardized scrape loop using the Scraper Framework."""
        after = None
        posts_collected = 0
        
        logger.warning(f"[PHASE: DATA AQUISITION] Started scraping r/{subreddit_name}")
        
        while posts_collected < self.post_limit:
            url = f"{self.base_url}/r/{subreddit_name}/new.json?limit=100"
            if after: 
                url += f"&after={after}"
            
            logger.warning(f"[HEARTBEAT] Requesting post list for r/{subreddit_name}...")
            response = await self._request_with_retry(url)
            
            if not response or response.status_code != 200:
                logger.warning(f"[HEARTBEAT] 🔄 Connection failed. Rotating proxy and waiting...")
                await asyncio.sleep(5)
                continue

            try:
                data = response.json().get("data", {})
                children = data.get("children", [])
                if not children: 
                    logger.warning(f"[HEARTBEAT] No more posts found in r/{subreddit_name}.")
                    break
                
                for post in children:
                    post_data = post.get("data", {})
                    source_id = post_data.get("name") # e.g. t3_1sg0gx0
                    
                    # 1. SYSTEMIC DEDUPLICATION: Skip before we even touch the text
                    if await self.is_already_scraped(source_id):
                        logger.warning(f"[DUPLICATE] Skipping already harvested post: {source_id}")
                        continue
                    
                    # 2. HARVEST: Fetch comments and combine
                    comments_text = await self._fetch_comments(post_data.get("id"))
                    raw_text = f"TITLE: {post_data.get('title')}\nBODY: {post_data.get('selftext')}\nCOMMENTS:\n{comments_text}"
                    
                    # 3. STORE: Move to MongoDB
                    await save_raw_document(
                        source="reddit",
                        raw_text=raw_text[:100000],
                        url=f"{self.base_url}{post_data.get('permalink')}",
                        subreddit=subreddit_name,
                        source_id=source_id
                    )
                    
                    posts_collected += 1
                    logger.warning(f"[PROGRESS] r/{subreddit_name}: {posts_collected}/{self.post_limit}")
                    
                    if posts_collected >= self.post_limit: 
                        break
                    
                    # 4. RATE LIMIT: Centralized pacing
                    await self._rate_limit_sleep()
                    
                after = data.get("after")
                if not after: 
                    break
                    
            except Exception as e:
                logger.warning(f"[HEARTBEAT] ❌ Critical Error: {e}")
                await asyncio.sleep(2)
                    
        logger.warning(f"[PHASE: COMPLETE] r/{subreddit_name} scraping finished.")
        return True
