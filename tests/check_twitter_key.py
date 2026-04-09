import os
import asyncio
from tweepy.asynchronous import AsyncClient
from dotenv import load_dotenv

async def check_api():
    load_dotenv()
    token = os.getenv("TWITTER_BEARER_TOKEN")
    
    if not token:
        print("ERROR: TWITTER_BEARER_TOKEN is empty in .env")
        return

    print(f"Checking Token (Length: {len(token)})...")
    
    client = AsyncClient(bearer_token=token)
    try:
        # Simple test query
        response = await client.search_recent_tweets(query="health", max_results=10)
        print("SUCCESS: Connection established. Token is valid.")
        if response.data:
            print(f"Verified: Retrieved {len(response.data)} sample tweets.")
        else:
            print("Verified: Connection OK, but no tweets found for 'health'.")
            
    except Exception as e:
        print(f"API ERROR: {e}")
        print("\nPossible solutions:")
        print("1. Ensure your token is a 'Bearer Token' (not API Key/Secret).")
        print("2. Confirm 'Recent Search' is enabled in your Twitter App settings.")
        print("3. Check if your Developer Account is in the 'Free' tier (limited to 100 tweets/month).")

if __name__ == "__main__":
    asyncio.run(check_api())
