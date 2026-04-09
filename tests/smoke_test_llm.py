import asyncio
import sys
import os

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.pipelines.llm_extract import extract_with_ollama
from src.core.config import settings

async def test_extraction():
    print(f"--- 🧪 LLM Extraction Smoke Test ---")
    print(f"Using Model: {settings.OLLAMA_MODEL}")
    print(f"Ollama URL: {settings.OLLAMA_BASE_URL}")
    
    sample_text = """
    TITLE: Chronic Back Pain help
    BODY: Hi, I'm John Doe. I've been living in Brooklyn, NY for 10 years. 
    I've been struggling with severe scoliosis and chronic back pain since 2015. 
    My doctor in New York said I might need surgery. Any advice? 
    You can reach me at 212-555-0199 if you have specialist recommendations.
    COMMENTS:
    --- COMMENT ---
    I have the same issue, it's tough!
    """
    
    print("\n[Input Text]:")
    print(sample_text)
    
    print("\n[Processing via Ollama]...")
    lead = await extract_with_ollama(sample_text)
    
    if lead:
        print("\n✅ Extraction Successful!")
        print(f"Name: {lead.first_name} {lead.last_name}")
        print(f"Location: {lead.city}, {lead.state}")
        print(f"Phone: {lead.phone_number}")
        print(f"Diseases: {lead.disease_history}")
    else:
        print("\n❌ Extraction Failed or Geolocation Filter Blocked the result.")

async def test_geolocation_filter():
    print(f"\n--- 🧪 Geolocation Filter Test (Non-US) ---")
    
    non_us_text = """
    I live in London and I have a broken arm.
    """
    
    print(f"[Input Text]: {non_us_text}")
    lead = await extract_with_ollama(non_us_text)
    
    if lead and (lead.state or lead.city):
        print("❌ FAILED: Found US location for London text.")
    else:
        print("✅ PASSED: Correctily ignored non-US text.")

if __name__ == "__main__":
    try:
        asyncio.run(test_extraction())
        asyncio.run(test_geolocation_filter())
    except Exception as e:
        print(f"Test crashed: {e}")
        print("\nTIP: Make sure Ollama is running and you have run 'ollama pull llama3.2:3b'")
        sys.exit(1)
