import asyncio
import json
import os
import subprocess
import sys
import urllib.parse
from datetime import datetime
from typing import Optional, List, Dict, Any

from .base import BaseScraper
from ..utils.mongo_storage import save_raw_document
from ..core.config import settings

from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

class TwitterScraper(BaseScraper):
    """
    Twitter scraper using x-tweet-fetcher (Nitter-based) to bypass API limits.
    Inherits proxy rotation and deduplication logic from BaseScraper.
    """
    
    def __init__(self):
        super().__init__()
        # Use discovery script instead of direct nitter client (handles bot protection fallback)
        self.tool_path = settings.X_TWEET_FETCHER_PATH.replace("nitter_client.py", "x_discover.py")
        self.python_exe = "python" if sys.platform == "win32" else "python3"

    async def scrape(self, query: str, max_results: int = 50) -> bool:
        """
        Execute x_discover.py search via subprocess and store results.
        Returns True if at least one new discovery was processed.
        """
        logger.info(f"[Twitter] Starting discovery for keywords: {query} (limit: {max_results})")
        
        # 1. Prepare Environment (NO PROXY)
        # Search engines like Brave/DDG aggressively block free datacenter proxies.
        # It's much safer to use the direct local IP for discovery.
        env = os.environ.copy()
        # Ensure we don't accidentally pass system-wide proxies if they exist
        env.pop('HTTP_PROXY', None)
        env.pop('HTTPS_PROXY', None)

        # 2. Build the Command (x_discover uses --keywords and --json)
        cmd = [
            self.python_exe,
            self.tool_path,
            "--keywords", query,
            "--limit", str(max_results),
            "--json"
        ]
        
        try:
            # 3. Execute Subprocess (Non-blocking thread)
            logger.info(f"[Twitter] Executing: {' '.join(cmd)}")
            result = await asyncio.to_thread(
                subprocess.run, 
                cmd, 
                capture_output=True, 
                text=True, 
                env=env,
                encoding="utf-8", # Force UTF-8 for emoji support on Windows
                timeout=90 # Discovery with fallback can take longer
            )
            
            if result.returncode != 0:
                logger.error(f"[Twitter] Subprocess failed with return code {result.returncode}. Stderr: {result.stderr}")
                return False

            # 4. Parse JSON Results
            try:
                # x_discover returns { "finds": [...] }
                raw_result = json.loads(result.stdout)
                discoveries = raw_result.get("finds", [])
            except json.JSONDecodeError as jde:
                logger.error(f"[Twitter] Failed to parse Discovery JSON. Raw output start: {result.stdout[:200]}")
                return False

            if not discoveries:
                logger.info(f"[Twitter] No new discoveries for keywords: {query}")
                return False

            # 5. Process & Store Each Discovery
            processed_count = 0
            for item in discoveries:
                # Normalize Data
                url = item.get('url', '')
                if not url:
                    continue
                
                # Use URL as source_id for discovery-based scrapes
                source_id = url
                if await self.is_already_scraped(source_id):
                    continue
                
                title = item.get('title', 'Unknown Result')
                snippet = item.get('snippet', '')
                source_name = item.get('source', 'discovery')
                
                # Construct formatted raw text for extraction brain
                formatted_text = (
                    f"PLATFORM: Twitter (Discovery via {source_name})\n"
                    f"TITLE: {title}\n"
                    f"SNIPPET: {snippet}\n"
                    f"URL: {url}"
                )
                
                # Extract handle from URL for enrichment
                handle = None
                try:
                    parsed_url = urllib.parse.urlparse(url)
                    path_parts = parsed_url.path.strip("/").split("/")
                    if path_parts:
                        handle = path_parts[0]
                except Exception:
                    pass

                # Save to MongoDB
                await save_raw_document(
                    source="twitter_x_discover",
                    raw_text=formatted_text[:100000],
                    url=url,
                    username=handle,
                    source_id=source_id,
                    query=query,
                    discovery_source=source_name
                )
                
                processed_count += 1
                await self._rate_limit_sleep()
                
            logger.info(f"[Twitter] Successfully processed {processed_count} new leads for query: {query}")
            return processed_count > 0
            
        except subprocess.TimeoutExpired:
            logger.error(f"[Twitter] Subprocess timed out after 60s for query: {query}")
            return False
        except Exception as e:
            logger.error(f"[Twitter] Unexpected error during scraping: {e}")
            return False
