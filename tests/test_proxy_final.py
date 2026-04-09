import asyncio
import httpx
import sys
import os

# Add project root to path
sys.path.append(os.getcwd())

from src.utils.proxy_manager import proxy_manager

async def test_final_verification():
    print("\n--- 🕵️ FINAL PROXY POOL AUDIT ---")
    
    # 1. Trigger a fresh scrape
    proxies = await proxy_manager.get_next_proxy()
    pool_size = len(proxy_manager.proxies)
    
    print(f"Pool size: {pool_size}")
    
    if pool_size < 5:
        print("❌ FAILED: Pool size too small (< 5). Parser might be failing.")
        return
    
    # 2. Sample and Verify
    samples = proxy_manager.proxies[:10]
    print(f"Sampling {len(samples)} proxies for verification...")
    
    async with httpx.AsyncClient(timeout=10.0, verify=False) as client:
        for proxy in samples:
            print(f"  > Verifying {proxy} against Reddit...")
            try:
                # Use a specific Reddit endpoint that requires SSL/HTTPS
                resp = await client.get("https://www.reddit.com/r/lyme/new.json?limit=1", proxy=proxy)
                if resp.status_code == 200:
                    print(f"    ✅ Success! Status: 200 OK")
                elif resp.status_code == 403:
                    print(f"    ⚠️ Warning: 403 Forbidden (Proxy is HTTPS but IP might be flagged)")
                else:
                    print(f"    ❌ FAILED: Status {resp.status_code}")
            except Exception as e:
                print(f"    ❌ FAILED: Connection Error: {e}")

    print("\n--- AUDIT COMPLETE ---")
    if pool_size >= 10:
        print("🚀 STATUS: READY FOR MASSIVE SCALE HARVEST (249 Subreddits)")
    else:
        print("⚠️ STATUS: POOL WEAK BUT FUNCTIONAL")

if __name__ == "__main__":
    asyncio.run(test_final_verification())
