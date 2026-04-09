import asyncio
import os
import sys
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
from src.scrapers.twitter import TwitterScraper
from src.tasks.llm_worker import run_llm_processing
from src.utils.export_leads import export_to_formats

async def run_full_twitter_pipeline():
    print("--- Starting Full Twitter Pipeline ---")
    
    # 1. HARVEST
    print("\nPhase 1: Harvesting Twitter via Discovery Fallback...")
    scraper = TwitterScraper()
    queries_file = "twitter_queries.txt"
    if os.path.exists(queries_file):
        with open(queries_file, 'r') as f:
            queries = [l.strip() for l in f if l.strip() and not l.startswith('#')]
        
        print(f"Preparing {len(queries)} parallel searches in chunks of 5...")
        for i in range(0, len(queries), 5):
            chunk = queries[i:i+5]
            print(f"   > Dispatching chunk: {chunk}")
            tasks = [scraper.scrape(q, max_results=10) for q in chunk]
            await asyncio.gather(*tasks)
            await asyncio.sleep(2) # Safety delay between chunks
    else:
        print("ERROR: twitter_queries.txt not found!")
        return

    # 2. EXTRACT (LLM BRAIN)
    print("\nPhase 2: Extracting Leads via LLM Brain...")
    await run_llm_processing(batch_size=50)

    # 3. SYNC (EXCEL)
    print("\nPhase 3: Synchronizing with health_leads_formatted.xlsx...")
    await export_to_formats()

    print("\nPIPELINE COMPLETE! New leads are ready in your Excel file.")

if __name__ == "__main__":
    # Ensure PYTHONPATH includes the current directory
    asyncio.run(run_full_twitter_pipeline())
