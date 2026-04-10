import httpx
import asyncio
import re
from datetime import datetime
from typing import List, Dict, Optional
from bs4 import BeautifulSoup
import logging
import random

from ..utils.proxy_manager import proxy_manager
from ..utils.stealth import get_stealth_headers
from ..core.config import settings

logger = logging.getLogger(__name__)

class BioExtractor:
    """
    Attempts to extract public metadata (Name, Bio, Location) from social profile URLs.
    """
    _profile_cache = {} # Global cache for the current session

    def __init__(self, timeout: int = 15):
        self.timeout = timeout
        self.proxy_manager = proxy_manager

    async def fetch_bio(self, url: str) -> str:
        """Fetches and extracts metadata with mirror-based stealth bypass."""
        # 1. URL Rewrite (Stealth Bypass - Recommendation #2)
        original_url = url
        if "twitter.com" in url or "x.com" in url:
            # Use a robust Nitter instance to bypass Login Wall
            url = url.replace("twitter.com", "nitter.net").replace("x.com", "nitter.net")
            logger.info(f"[BioExtractor] Rerouting to Mirror: {url}")
        elif "reddit.com" in url:
            # Use Libreddit mirror
            url = url.replace("reddit.com", "libreddit.spike.codes")
            logger.info(f"[BioExtractor] Rerouting to Mirror: {url}")

        # 2. Cache Check (TTL: 24h)
        now = datetime.utcnow()
        if original_url in self._profile_cache:
            cache_info = self._profile_cache[original_url]
            if (now - cache_info['ts']).total_seconds() < 86400:
                return cache_info['text']

        # 2. Proxy Rotation
        proxy = await self.proxy_manager.get_next_proxy()
        
        try:
            headers = get_stealth_headers()
            # Paced fetching: Sleep 1-3s before request to avoid concurrent bursts
            await asyncio.sleep(random.uniform(1.0, 3.0))

            async with httpx.AsyncClient(proxy=proxy, timeout=self.timeout, follow_redirects=True) as client:
                response = await client.get(url, headers=headers)
                if response.status_code != 200:
                    if response.status_code in [403, 429]:
                        self.proxy_manager.report_failure(proxy, response.status_code)
                    return ""
                
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Cleanup
                for s in soup(["script", "style"]):
                    s.decompose()
                
                # Metadata
                meta_desc = soup.find("meta", attrs={"name": "description"})
                og_desc = soup.find("meta", attrs={"property": "og:description"})
                og_title = soup.find("meta", attrs={"property": "og:title"})
                
                parts = []
                if soup.title and soup.title.string:
                    parts.append(f"Page Title: {soup.title.string.strip()}")
                if og_title:
                    parts.append(f"Profile Title: {og_title.get('content')}")
                
                visible_text = soup.get_text(separator=' ', strip=True)[:2000]
                
                # 3. Precision Phone Regex (Recommendation #7)
                phone_pattern = r'\b(?:\+?1[-.\s]?)?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}\b'
                phones = re.findall(phone_pattern, visible_text)
                if phones:
                    parts.append(f"Contact Numbers: {', '.join(list(set(phones)))}")
                
                # DOB detection
                dob_match = re.search(r'(?:Born|🎂|Birth)\s*:?\s*(\d{2}/\d{2}/\d{4}|\w+\s+\d{4}|\d{4})', visible_text, re.IGNORECASE)
                if dob_match:
                    parts.append(f"Birth Info: {dob_match.group(1)}")
                
                if og_desc:
                    parts.append(f"Bio (OG): {og_desc.get('content')}")
                elif meta_desc:
                    parts.append(f"Bio (Meta): {meta_desc.get('content')}")
                
                parts.append(f"Content Snippet: {visible_text[:500]}...")
                
                # Deep Link Discovery
                urls_in_text = re.findall(r'https?://(?:linktr\.ee|carrd\.co|bit\.ly|[\w-]+\.\w+)\S*', visible_text)
                if urls_in_text and url != urls_in_text[0]:
                    parts.append(f"[Deep Link Discovery] Found: {urls_in_text[0]}")
                
                extraction = "\n".join(parts)
                
                # 4. Cache Store
                self._profile_cache[original_url] = {'text': extraction, 'ts': now}
                return extraction
                
        except Exception as e:
            logger.warning(f"[BioExtractor] Failed to fetch {url}: {e}")
            if proxy: self.proxy_manager.report_failure(proxy)
            return ""

    async def get_combined_bios(self, urls: List[str]) -> str:
        """Parallel fetching with robust exception safety."""
        if not urls:
            return ""
            
        tasks = [self.fetch_bio(url) for url in urls]
        # Return results even if some fail
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        valid_results = [r for r in results if isinstance(r, str) and r]
        return "\n\n---\n".join(valid_results)

bio_extractor = BioExtractor()
