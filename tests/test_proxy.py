import asyncio
import sys
import os
import httpx

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.utils.proxy_manager import proxy_manager

async def verify_proxy_rotation():
    print("--- 🔬 Verifying Proxy Rotation System ---")
    
    # 1. Test Scraping
    print("[1] Fetching fresh proxies...")
    p1 = await proxy_manager.get_next_proxy()
    print(f"Candidate Proxy 1: {p1}")
    
    # 2. Test rotation on failure
    print("[2] Reporting failure to trigger rotation...")
    proxy_manager.report_failure(p1)
    p2 = await proxy_manager.get_next_proxy()
    print(f"Candidate Proxy 2: {p2}")
    
    if p1 != p2:
        print("✅ SUCCESS: Proxy rotated successfully.")
    else:
        print("❌ FAILURE: Proxy did not rotate.")

    # 3. Test a real request (Optional - might fail if free proxies are down)
    print(f"[3] Attempting request through {p2}...")
    try:
        async with httpx.AsyncClient(proxy=p2, timeout=10.0) as client:
            resp = await client.get("https://httpbin.org/ip")
            print(f"Response (IP Check): {resp.json()}")
            print("✅ Proxy is LIVE and working.")
    except Exception as e:
        print(f"⚠️ Proxy request failed (Expected for free proxies): {e}")

if __name__ == "__main__":
    asyncio.run(verify_proxy_rotation())
