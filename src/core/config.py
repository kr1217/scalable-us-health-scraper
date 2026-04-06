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
    SCRAPE_POST_LIMIT: int = 10
    DELAY_BETWEEN_SCRAPES: float = 1.5

    # Pydantic v2 Config
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

# Global settings instance
settings = Settings()
