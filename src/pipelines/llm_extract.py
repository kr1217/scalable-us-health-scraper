import json
from typing import Optional, Dict, Any
import ollama
from ..core.config import settings
from ..core.models import Lead

SYSTEM_PROMPT = """
You are a highly accurate medical data extraction specialist. Your task is to extract structured person-centric health data from raw social media text.

STRICT RULES:
1. ONLY output valid JSON. No conversational filler or explanations.
2. EXTRACT the following fields:
   - first_name: (string/null)
   - last_name: (string/null)
   - phone_number: (string/null) - Must be a US-style phone number.
   - address: (string/null)
   - city: (string/null)
   - state: (string/null) - Two-letter US state code (e.g., NY, CA) or full state name.
   - disease_history: (string/null) - Comma-separated list of medical conditions or symptoms mentioned.
3. GEOLOCATION FILTER:
   - If the text does NOT explicitly or strongly imply a location within the United States, set 'state' and 'city' to null.
   - Look for US cities, states, or zip codes.
4. SELF-REFERENCE RULE:
   - Only return a lead if the text refers to the person's OWN health issues (e.g., "I have...", "My doctor told me..."). 
   - If they are talking about a friend, child, or someone else, set 'is_self' to false (if we had that field) or simply return null values for PII. In this case, strictly set all personal fields to null if it's not a self-reference.
5. JSON SCHEMA:
{
  "first_name": "...",
  "last_name": "...",
  "phone_number": "...",
  "address": "...",
  "city": "...",
  "state": "...",
  "disease_history": "..."
}
"""

async def extract_with_ollama(raw_text: str) -> Optional[Lead]:
    """
    Sends raw text to the local Ollama instance for structured extraction.
    """
    try:
        # Truncate to avoid context window overflows for very long comment threads
        truncated_text = raw_text[:4000]
        
        response = ollama.chat(
            model=settings.OLLAMA_MODEL,
            messages=[
                {'role': 'system', 'content': SYSTEM_PROMPT},
                {'role': 'user', 'content': f"Extract data from this text:\n\n{truncated_text}"}
            ],
            options={
                'temperature': 0.1,  # Keep it deterministic
            }
        )
        
        content = response['message']['content'].strip()
        
        # Simple cleanup if the model adds markdown code blocks
        if "```json" in content:
            content = content.split("```json")[-1].split("```")[0].strip()
        elif "```" in content:
            content = content.split("```")[-1].split("```")[0].strip()

        data = json.loads(content)
        
        # Geolocation Hard-Check REMOVED (Loosened for Reddit)
        # We now allow leads without city/state to pass through to the worker.
            
        # Create the Lead object
        # We merge with default metadata fields in the worker later
        return Lead(
            first_name=data.get("first_name"),
            last_name=data.get("last_name"),
            phone_number=data.get("phone_number"),
            address=data.get("address"),
            city=data.get("city"),
            state=data.get("state"),
            disease_history=data.get("disease_history"),
            source="llm_extracted",
            source_url="pending", # Filled by worker
        )
        
    except Exception as e:
        print(f"[Ollama] Extraction error: {e}")
        return None
