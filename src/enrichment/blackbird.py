import asyncio
import json
import subprocess
import os
from typing import Optional, Dict, Any, List
from .base import BaseEnricher
from ..core.config import settings
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

class BlackbirdEnricher(BaseEnricher):
    """
    Wrapper for the Blackbird OSINT CLI tool.
    Requires Blackbird to be installed and accessible.
    """

    async def enrich(self, identifier: str, identifier_type: str) -> Optional[Dict[str, Any]]:
        if identifier_type != "username":
            return None

        # Check if blackbird path is configured or likely available
        cmd = [settings.BLACKBIRD_PATH, "-u", identifier, "--json"]
        
        try:
            # We use asyncio.to_thread to run the blocking subprocess
            result = await asyncio.to_thread(
                subprocess.run,
                cmd,
                capture_output=True,
                text=True,
                timeout=120
            )

            if result.returncode == 0:
                try:
                    data = json.loads(result.stdout)
                    # Blackbird output usually contains found sites
                    results = data.get("results", [])
                    found_profiles = []
                    for res in results:
                        if res.get("status") == "FOUND":
                            found_profiles.append({
                                "platform": res.get("app"),
                                "url": res.get("url")
                            })
                    
                    if found_profiles:
                        return {
                            "other_profiles": found_profiles,
                            "raw_response": data
                        }
                except json.JSONDecodeError:
                    logger.warning(f"[Enrichment] Blackbird output is not valid JSON for {identifier}")
            else:
                # Silently skip if not installed or fails
                if "not found" in result.stderr.lower() or "not recognized" in result.stderr.lower():
                    logger.info(f"[Enrichment] Blackbird CLI not found at '{settings.BLACKBIRD_PATH}'. Skipping.")
                else:
                    logger.warning(f"[Enrichment] Blackbird failed with code {result.returncode}: {result.stderr}")
                    
        except Exception as e:
            logger.info(f"[Enrichment] Blackbird skipped: {e}")

        return None
