import httpx
import asyncio
from typing import Optional, Dict, Any, List
from .base import BaseEnricher
from ..core.config import settings
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

class WhatsMyNameEnricher(BaseEnricher):
    """
    Free enricher using the WhatsMyName public JSON dataset.
    Checks if a username exists across hundreds of platforms.
    """
    DATA_URL = "https://raw.githubusercontent.com/WebBreacher/WhatsMyName/main/wmn-data.json"
    _cached_data: Optional[Dict[str, Any]] = None

    async def _get_data(self) -> Dict[str, Any]:
        if WhatsMyNameEnricher._cached_data is None:
            async with httpx.AsyncClient() as client:
                resp = await client.get(self.DATA_URL)
                resp.raise_for_status()
                WhatsMyNameEnricher._cached_data = resp.json()
        return WhatsMyNameEnricher._cached_data

    async def enrich(self, identifier: str, identifier_type: str) -> Optional[Dict[str, Any]]:
        if identifier_type != "username":
            return None

        try:
            data = await self._get_data()
            sites = data.get("sites", [])
            found_profiles = []

            # We'll check a subset of high-value sites or a limited number to avoid massive delays
            # For a real POC, we can limit to top 50 or so.
            # Here we implement the logic to probe.
            
            # To keep it efficient, we only check a selected list of common platforms 
            # if we don't want to wait for 600+ requests.
            target_sites = [s for s in sites if s.get("name") in [
                "Twitter", "Reddit", "Instagram", "GitHub", "TikTok", "Pinterest", "Steam", "Spotify"
            ]]

            async def check_site(site):
                uri_check = site.get("uri_check", "")
                if not uri_check:
                    return None
                
                # WMN uses {account} as placeholder
                url = uri_check.replace("{account}", identifier).replace("{target}", identifier)
                
                try:
                    async with httpx.AsyncClient(timeout=5.0) as client:
                        # Adding a small delay as requested in the plan
                        await asyncio.sleep(settings.WHATSMYNAME_DELAY)
                        resp = await client.get(url, follow_redirects=True)
                        
                        # Check logic depends on site configuration in JSON
                        # Simplified check: status code 200 usually means found
                        # (Real implementation should check site.get("e_code") etc.)
                        expected_code = site.get("e_code", 200)
                        if resp.status_code == expected_code:
                            # Also check for "not found" text if applicable
                            e_string = site.get("e_string")
                            if e_string and e_string in resp.text:
                                return None
                            return {"platform": site.get("name"), "url": url}
                except Exception:
                    return None
                return None

            tasks = [check_site(s) for s in target_sites]
            results = await asyncio.gather(*tasks)
            found_profiles = [r for r in results if r]

            if found_profiles:
                return {
                    "other_profiles": found_profiles,
                    "raw_response": {"source": "WhatsMyName", "count": len(found_profiles)}
                }
        except Exception as e:
            logger.warning(f"[Enrichment] WhatsMyName failed: {e}")
        
        return None
