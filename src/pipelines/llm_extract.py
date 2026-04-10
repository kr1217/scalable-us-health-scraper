import json
from typing import Optional, Dict, Any, List
import ollama
from ..core.config import settings
from ..core.models import Lead

SYSTEM_PROMPT = """
You are a highly accurate lead generation and PII extraction specialist. Your task is to extract structured person-centric health data from raw text, which MAY include extended social profile metadata (Digital Footprint).

STRICT Extraction Rules:
- Identify the person's name (First, Last), location (City, State), Date of Birth, and any phone numbers.
- Prioritize "Digital Footprint" metadata (OpenGraph/Bios) and "Deep Link Discovery" results for PII.
- PHONE NUMBERS: Look for strings like (xxx) xxx-xxxx, +1-xxxxxx, or xxx-xxx-xxxx in both the post and the Bios.
- DATE OF BIRTH: 
  - Look for years (19XX, 20XX) or specific dates.
  - AGE DEDUCTION: If the text says "I am X years old" or similar, calculate the birth year using the CURRENT YEAR: 2026.
- ONLY include leads from the USA.

STRICT RULES:
1. ONLY output valid JSON. No conversational filler.
2. EXTRACTION PRIORITY:
   - If First Name and Last Name are not found in the original post, check the '[EXTENDED DIGITAL FOOTPRINT INFO]' block. Titles like "John Doe (@jdoe)" or descriptions like "Chief Editor: Jane Smith" are strong indicators.
   - Look for US-style locations (e.g., "Chicago, IL", "Austin", "SF, California") in both the post and the social bios.
3. FIELDS TO EXTRACT:
   - first_name: (string/null)
   - last_name: (string/null)
   - date_of_birth: (string/null) - Format as YYYY-MM-DD if possible, else year or string.
   - phone_number: (string/null) - Extract US-style numbers (e.g., 555-0199, (555) 012-3456).
   - address: (string/null) - Usually null, but grab if mentioned.
   - city: (string/null)
   - state: (string/null) - Two-letter code or full name.
   - disease_history: (string/null) - The MEDICAL context.
   - raw_username: (string/null) - The username found in text (to help map it)
4. GEOLOCATION FILTER:
   - Strictly filter for the UNITED STATES. If you find a location like "London", "Toronto", or "Sydney" without a clear US context, set city/state to null.
5. JSON SCHEMA (MUST BE A LIST):
[
  {
    "first_name": "...",
    "last_name": "...",
    "date_of_birth": "...",
    "phone_number": "...",
    "address": "...",
    "city": "...",
    "state": "...",
    "disease_history": "...",
    "raw_username": "..."
  }
]
"""

async def extract_with_ollama(raw_text: str) -> Optional[List[Lead]]:
    """
    Sends raw text to the local Ollama instance for structured extraction of MULTIPLE leads.
    """
    try:
        # Truncate to avoid context window overflows
        truncated_text = raw_text[:6000] # Slightly larger for threads
        
        response = ollama.chat(
            model=settings.OLLAMA_MODEL,
            messages=[
                {'role': 'system', 'content': SYSTEM_PROMPT},
                {'role': 'user', 'content': f"Extract ALL health leads from this thread:\n\n{truncated_text}"}
            ],
            format='json', 
            options={'temperature': 0.1}
        )
        
        content = response['message']['content'].strip()
        data_list = json.loads(content)
        
        if not isinstance(data_list, list):
            data_list = [data_list] # Fallback
            
        leads = []
        for data in data_list:
            if data.get("first_name") or data.get("disease_history"):
                # Helper to safely flatten lists/dicts into clean strings (Rec #1)
                def safe_str(val):
                    if val is None: return None
                    if isinstance(val, list):
                        # Flatten list: ["A", "B"] -> "A, B"
                        items = []
                        for item in val:
                            if isinstance(item, dict):
                                items.append(" ".join(str(v) for v in item.values() if v))
                            else:
                                items.append(str(item))
                        return ", ".join(items)
                    if isinstance(val, dict):
                        # Flatten dict: {"type": "Lyme"} -> "Lyme"
                        return ", ".join(str(v) for v in val.values() if v)
                    return str(val)

                leads.append(Lead(
                    first_name=safe_str(data.get("first_name")),
                    last_name=safe_str(data.get("last_name")),
                    date_of_birth=safe_str(data.get("date_of_birth")),
                    phone_number=safe_str(data.get("phone_number")),
                    phone_number_source=safe_str(data.get("phone_number_source") or "Thread Context"),
                    address=safe_str(data.get("address")),
                    city=safe_str(data.get("city")),
                    state=safe_str(data.get("state")),
                    disease_history=safe_str(data.get("disease_history")),
                    source="llm_extracted",
                    source_url="pending"
                ))
        return leads
        
    except Exception as e:
        print(f"[Ollama] Extraction error: {e}")
        return None
