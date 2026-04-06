from abc import ABC, abstractmethod
from typing import List, Optional
from ..core.models import Lead

class BaseScraper(ABC):
    """Abstract base class for all scrapers."""
    
    @abstractmethod
    async def scrape(self, target: str) -> List[Lead]:
        """Scrape the given target (e.g., subreddit, URL) and return a list of Leads."""
        pass

    @abstractmethod
    async def save_lead(self, lead: Lead):
        """Save the extracted lead to the database."""
        pass

    @abstractmethod
    async def save_raw(self, raw_data: str, url: str):
        """Save the raw data to the secondary storage (MongoDB)."""
        pass
