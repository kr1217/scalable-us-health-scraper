import httpx
import asyncio
from typing import Optional, Dict, Any, List
from .base import BaseEnricher
from ..utils.stealth import get_stealth_headers
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

class SocialScannerEnricher(BaseEnricher):
    """
    Self-contained enricher that probes high-value social platforms directly.
    Checks for username existence via HTTP status codes and profile URLs.
    """
    
    # Curated list of high-value sites with their URL pattern and expected existence status
    SITES = {
        "Instagram": "https://www.instagram.com/{}/",
        "LinkedIn": "https://www.linkedin.com/in/{}/",
        "TikTok": "https://www.tiktok.com/@{}/",
        "Facebook": "https://www.facebook.com/{}/",
        "YouTube": "https://www.youtube.com/@{}/",
        "GitHub": "https://github.com/{}/",
        "Pinterest": "https://www.pinterest.com/{}/",
        "Medium": "https://medium.com/@{}/"
    }

    async def enrich(self, identifier: str, identifier_type: str) -> Optional[Dict[str, Any]]:
        if identifier_type != "username":
            return None

        found_profiles = []
        headers = get_stealth_headers()

        async def check_platform(name, url_template):
            # Try different common placeholder formats
            url = url_template.replace("{account}", identifier).replace("{target}", identifier)
            if "{}" in url:
                url = url.format(identifier)
            try:
                async with httpx.AsyncClient(headers=headers, timeout=5.0, follow_redirects=True) as client:
                    resp = await client.get(url)
                    # Many platform return 200 for profile and 404/others for no profile
                    if resp.status_code == 200:
                        # Some platforms return 200 even for "Not found" pages, but we simplify for POC
                        return {"platform": name, "url": url}
            except Exception:
                pass
            return None

        tasks = [check_platform(name, template) for name, template in self.SITES.items()]
        results = await asyncio.gather(*tasks)
        found_profiles = [r for r in results if r]

        if found_profiles:
            return {
                "other_profiles": found_profiles,
                "raw_response": {"source": "SocialScanner", "platforms_checked": len(self.SITES)}
            }
        
        return None
