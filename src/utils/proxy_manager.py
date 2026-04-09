import httpx
import re
import random
import asyncio
from typing import List, Optional

from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

class ProxyManager:
    """
    Scrapes and rotates free public proxies.
    Automatically fetches new proxies when the pool is exhausted.
    """
    def __init__(self):
        self.proxies: List[str] = []
        self.bad_proxies: set = set()
        self.last_fetch_time = 0
        self.lock = asyncio.Lock()

    async def _fetch_source(self, client: httpx.AsyncClient, url: str) -> List[str]:
        """Fetch a single source using centralized browser impersonation."""
        # Architect's Note: Use the same global stealth headers for proxy site audits
        from .stealth import get_stealth_headers
        headers = get_stealth_headers()
        
        try:
            logger.warning(f"[RECOVERY] Auditing proxies via stealth: {url}...")
            response = await client.get(url, headers=headers)
            if response.status_code == 200:
                # Flex-Regex: Matches <tr> content and isolates <td> cells even with attributes
                rows = re.findall(r"<tr.*?>(.*?)</tr>", response.text, re.DOTALL)
                https_proxies = []
                
                for row in rows:
                    cells = re.findall(r"<td.*?>(.*?)</td>", row, re.DOTALL)
                    if len(cells) >= 7:
                        ip = re.sub(r"<.*?>", "", cells[0]).strip()
                        port = re.sub(r"<.*?>", "", cells[1]).strip()
                        is_https = re.sub(r"<.*?>", "", cells[6]).strip().lower()
                        
                        if is_https == "yes":
                            https_proxies.append(f"http://{ip}:{port}")
                            
                logger.warning(f"[RECOVERY] {url}: Audit found {len(https_proxies)} HTTPS proxies.")
                return https_proxies
        except Exception as e:
            logger.warning(f"[RECOVERY] Audit failed for {url}: {e}")
        return []

    async def _fetch_proxies(self) -> List[str]:
        """Scrapes multiple high-quality proxy sources in PARALLEL."""
        sources = [
            "https://free-proxy-list.net/",
            "https://www.sslproxies.org/",
            "https://www.us-proxy.org/"
        ]
        
        async with httpx.AsyncClient(timeout=10.0, follow_redirects=True, verify=False) as client:
            tasks = [self._fetch_source(client, url) for url in sources]
            results = await asyncio.gather(*tasks)
            all_matches = [proxy for sublist in results for proxy in sublist]
        
        unique_proxies = list(set(all_matches))
        
        if not unique_proxies:
            logger.error("[!!!] CRITICAL: Zero HTTPS proxies found in audit. System will attempt DIRECT CONNECTION with local IP exposure.")
            
        logger.warning(f"[PROXY POOL] Final Audit Complete: {len(unique_proxies)} HTTPS candidates verified.")
        return unique_proxies

    async def get_next_proxy(self) -> Optional[str]:
        """Rotates through the pool with fast-fail logic."""
        async with self.lock:
            if not self.proxies:
                self.proxies = await self._fetch_proxies()
            
            # Use Pop-based rotation for efficiency
            while self.proxies:
                proxy = self.proxies.pop(0)
                if proxy not in self.bad_proxies:
                    return proxy
            
            # Refresh if empty
            self.proxies = await self._fetch_proxies()
            return self.proxies.pop(0) if self.proxies else None

    def report_failure(self, proxy: str, status_code: int = None):
        """Immediately blacklists bad proxies."""
        if proxy and proxy not in self.bad_proxies:
            self.bad_proxies.add(proxy)
            logger.warning(f"[ProxyManager] Blacklisted: {proxy} (Status: {status_code or 'Timeout'}) | Total dead: {len(self.bad_proxies)}")

# Singleton instance
proxy_manager = ProxyManager()
