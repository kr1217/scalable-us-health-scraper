import asyncio
import os
import sys

# Ensure project root is in path
sys.path.append(os.getcwd())

from src.scrapers.twitter import TwitterScraper
from src.utils.export_leads import export_to_formats

async def run_twitter_smoke_test():
    """
    Test the full Twitter pipeline: 
    API -> Deduplication -> Mongo Storage -> Export Trigger
    """
    print("--- Starting Twitter Integration Test ---")
    
    # 1. Initialize Scraper
    # Assumes TWITTER_BEARER_TOKEN is in .env or environment
    scraper = TwitterScraper()
    
    # 2. Trigger a Small Scrape (Max 5 results for speed)
    test_query = "diabetes symptoms place_country:US"
    print(f"Testing query: {test_query}")
    
    success = await scraper.scrape(test_query, max_results=5)
    
    if success:
        print("[SUCCESS] Twitter Scraper retrieved and stored results.")
    else:
        print("[FAILURE] Twitter Scraper failed. Check Bearer Token and logs.")
        return

    # 3. Inform user about next steps
    print("\n--- Next Steps ---")
    print("1. Your raw tweets are now in MongoDB (raw_scrapes).")
    print("2. Run the LLM Worker to process them into Postgres leads:")
    print("   $env:PYTHONPATH='.'; python -m celery -A src.tasks.celery_app worker --pool=solo --loglevel=info --queues=llm")
    print("3. Once processed, run the export utility to update your Excel file:")
    print("   $env:PYTHONPATH='.'; python -m src.utils.export_leads")

if __name__ == "__main__":
    asyncio.run(run_twitter_smoke_test())
