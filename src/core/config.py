from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional

class Settings(BaseSettings):
    # Project Settings
    PROJECT_NAME: str = "scalable-us-health-scraper"
    DEBUG: bool = True

    # Database Settings
    DATABASE_URL: str
    MONGODB_URI: str
    MONGODB_DB_NAME: str = "health_leads_raw"

    # Celery Settings
    CELERY_BROKER_URL: str
    CELERY_RESULT_BACKEND: str

    # Scraper Settings
    REDDIT_BASE_URL: str = "https://old.reddit.com"
    SUBREDDITS_FILE_PATH: str = "subreddits.txt"
    SCRAPE_POST_LIMIT: int = 100
    DELAY_BETWEEN_SCRAPES: float = 1.5
    DELAY_JITTER: float = 0.5
    TWITTER_BEARER_TOKEN: Optional[str] = None
    X_TWEET_FETCHER_PATH: str = "./x-tweet-fetcher/scripts/nitter_client.py"
    PROXY_LIST: Optional[str] = None
    
    # Ollama Settings
    OLLAMA_BASE_URL: str = "http://localhost:11434"
    OLLAMA_MODEL: str = "llama3.2:3b"

    # Pydantic v2 Config
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

# Global settings instance
settings = Settings()
