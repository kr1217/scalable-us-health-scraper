from sqlalchemy import Column, Integer, String, Text, DateTime, JSON
from datetime import datetime
from .database import Base

class LeadORM(Base):
    """SQLAlchemy model for heart leads data."""
    __tablename__ = "leads"

    id = Column(Integer, primary_key=True, index=True)
    first_name = Column(String(100), nullable=True)
    last_name = Column(String(100), nullable=True)
    date_of_birth = Column(String(50), nullable=True)
    phone_number = Column(String(50), nullable=True)
    phone_number_source = Column(String(50), nullable=True)
    address = Column(String(255), nullable=True)
    city = Column(String(100), nullable=True)
    state = Column(String(50), nullable=True)
    disease_history = Column(Text, nullable=True)
    source = Column(String(100), nullable=True)
    source_url = Column(String(255), nullable=True)
    source_id = Column(String(100), nullable=True, unique=True, index=True)
    identity_hash = Column(String(128), nullable=True, unique=True, index=True)
    extracted_at = Column(DateTime, default=datetime.utcnow)

class RawScrapeORM(Base):
    """SQLAlchemy model for raw scrape metadata (optional, primary raw data is in Mongo)."""
    __tablename__ = "raw_metadata"

    id = Column(Integer, primary_key=True, index=True)
    source = Column(String(100))
    url = Column(String(255))
    timestamp = Column(DateTime, default=datetime.utcnow)

class EnrichedProfileORM(Base):
    """SQLAlchemy model for cached enrichment data."""
    __tablename__ = "enriched_profiles"

    id = Column(Integer, primary_key=True)
    identifier = Column(String(255), unique=True, index=True)  # username or email
    identifier_type = Column(String(20))  # 'username' or 'email'
    data = Column(JSON)  # stores full merged response
    phone_number = Column(String(50), nullable=True)
    email = Column(String(255), nullable=True)
    other_profiles = Column(JSON, nullable=True)  # list of {platform, url}
    last_enrich_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
