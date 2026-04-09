import asyncio
from src.utils.mongo_storage import save_raw_document

async def scrape_youtube_comments(video_id: str):
    """
    Example using YouTube Data API v3 (Mocked logic).
    Patterns for future scrapers: Fetch -> Clean -> Centralized Storage.
    """
    print(f"[YouTube] Fetching comments for video: {video_id}")
    
    # In a real implementation, you would use google-api-python-client
    # and fetch comments for a specific video ID related to health.
    mock_comments = [
        "I've been dealing with chronic fatigue in Seattle for years.",
        "Anyone in Texas know a good specialist for this?",
        "My doctor in Miami updated my prescription today."
    ]
    
    video_url = f"https://www.youtube.com/watch?v={video_id}"
    combined_text = "\n".join(mock_comments)
    
    # The BEAUTY of the new architecture: We just call this one function.
    # No extraction logic needed here!
    await save_raw_document(
        source="youtube",
        raw_text=combined_text,
        url=video_url,
        video_id=video_id
    )
    print(f"✅ Saved raw YouTube data to MongoDB.")

if __name__ == "__main__":
    asyncio.run(scrape_youtube_comments("EXAMPLE_ID_123"))
