from abc import ABC, abstractmethod
from typing import Optional, Dict, Any

class BaseEnricher(ABC):
    """Abstract base for all enrichment APIs."""
    
    @abstractmethod
    async def enrich(self, identifier: str, identifier_type: str) -> Optional[Dict[str, Any]]:
        """
        Takes a username or email, returns enrichment data:
        {
            "other_profiles": [{"platform": "twitter", "url": "...", "found": True}],
            "raw_response": {...}
        }
        """
        pass
