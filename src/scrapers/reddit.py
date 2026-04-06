import asyncio
import httpx
from typing import List, Optional, Dict, Any
from datetime import datetime
from ..core.config import settings
from ..core.models import Lead
from ..pipelines.extract import extractor
from .base import BaseScraper

class RedditScraper(BaseScraper):
    def __init__(self):
        self.base_url = settings.REDDIT_BASE_URL
        self.post_limit = settings.SCRAPE_POST_LIMIT
        self.delay = settings.DELAY_BETWEEN_SCRAPES
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        }

    async def _fetch_comments(self, client: httpx.AsyncClient, post_id: str) -> str:
        """Fetch top-level comments for a given post ID."""
        url = f"{self.base_url}/comments/{post_id}.json"
        try:
            response = await client.get(url)
            if response.status_code == 200:
                data = response.json()
                # Reddit comments JSON is a list: [post_info, comment_tree]
                if len(data) > 1:
                    children = data[1].get("data", {}).get("children", [])
                    comments = []
                    for child in children[:10]:  # Limit to top 10 comments
                        body = child.get("data", {}).get("body", "")
                        if body:
                            comments.append(body)
                    return "\n--- COMMENT ---\n".join(comments)
        except Exception as e:
            print(f"Error fetching comments for {post_id}: {e}")
        return ""

    async def scrape(self, subreddit_name: str) -> List[Lead]:
        """Scrape the given subreddit using pagination and comment extraction."""
        leads = []
        after = None
        posts_collected = 0
        
        async with httpx.AsyncClient(headers=self.headers, timeout=30.0) as client:
            # Loop for pagination
            while posts_collected < self.post_limit:
                url = f"{self.base_url}/r/{subreddit_name}/new.json?limit=100"
                if after:
                    url += f"&after={after}"
                
                print(f"Scraping {subreddit_name} (Total so far: {posts_collected}) via {url}")
                
                try:
                    response = await client.get(url)
                    response.raise_for_status()
                    data = response.json().get("data", {})
                    
                    children = data.get("children", [])
                    if not children:
                        break
                    
                    for post in children:
                        post_data = post.get("data", {})
                        
                        # Extract attributes
                        title = post_data.get("title", "")
                        body = post_data.get("selftext", "")
                        post_id_full = post_data.get("name", "") # e.g. t3_12345
                        post_id_short = post_data.get("id", "")   # e.g. 12345
                        
                        # Fetch comments for deeper context
                        comments_text = await self._fetch_comments(client, post_id_short)
                        
                        # Combine text for extraction
                        combined_text = f"TITLE: {title}\nBODY: {body}\nCOMMENTS:\n{comments_text}"
                        
                        permalink = post_data.get("permalink", "")
                        post_url = f"{self.base_url}{permalink}"
                        
                        # Extract lead
                        lead = extractor.extract_lead(combined_text, "reddit", post_url, post_id_full)
                        leads.append(lead)
                        
                        posts_collected += 1
                        if posts_collected >= self.post_limit:
                            break
                    
                    after = data.get("after")
                    if not after:
                        break
                        
                    # Politeness delay between pages
                    await asyncio.sleep(self.delay)
                    
                except Exception as e:
                    print(f"Error on page {after}: {e}")
                    break
        
        return leads

    async def save_lead(self, lead: Lead): pass
    async def save_raw(self, raw_data: str, url: str): pass
