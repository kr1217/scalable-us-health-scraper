from datetime import datetime
from typing import Dict, Any
from ..core.database import get_mongo_client, get_mongo_db
from ..core.config import settings

async def save_raw_document(source: str, raw_text: str, url: str, **extra_metadata) -> bool:
    """
    Centralized utility to save raw text data into MongoDB for later LLM processing.
    """
    client = get_mongo_client()
    try:
        db = await get_mongo_db(client)
        
        document = {
            "source": source,
            "url": url,
            "raw_text": raw_text,
            "timestamp": datetime.utcnow().isoformat(),
            "llm_processed": False,
            **extra_metadata
        }
        
        # Insert into raw_scrapes collection
        result = await db.raw_scrapes.insert_one(document)
        return result.acknowledged
        
    except Exception as e:
        print(f"[MongoStorage] Error saving raw document from {source}: {e}")
        return False
    finally:
        client.close()
