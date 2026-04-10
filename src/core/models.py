from pydantic import BaseModel, ConfigDict, Field, EmailStr
from typing import Optional, List
from datetime import datetime

class Lead(BaseModel):
    # Core Lead Information (Scraped Fields)
    first_name: Optional[str] = Field(None, description="First name of the potential lead")
    last_name: Optional[str] = Field(None, description="Last name of the potential lead")
    date_of_birth: Optional[str] = Field(None, description="Date of birth (extracted as string for now)")
    phone_number: Optional[str] = Field(None, description="US Phone number pattern match")
    phone_number_source: Optional[str] = Field(None, description="Platform that provided the phone number")
    address: Optional[str] = Field(None, description="Full address if available")
    city: Optional[str] = Field(None, description="City mention")
    state: Optional[str] = Field(None, description="State abbreviation or name")
    disease_history: Optional[str] = Field(None, description="Mentioned diseases or medical conditions")

    # Metadata
    source: str = Field(..., description="Source of the lead (e.g., reddit, hospital_dir)")
    source_url: str = Field(..., description="URL where the lead was found")
    source_id: Optional[str] = Field(None, description="Unique ID from the source system (e.g., Reddit post ID)")
    extracted_at: datetime = Field(default_factory=datetime.utcnow, description="When the lead was extracted")

    # Pydantic v2 Config
    model_config = ConfigDict(
        populate_by_name=True,
        str_strip_whitespace=True,
        extra="forbid"
    )

class RawData(BaseModel):
    # Raw HTML/JSON storage for MongoDB
    source: str
    url: str
    content: str  # Raw body or JSON blob
    timestamp: datetime = Field(default_factory=datetime.utcnow)

    model_config = ConfigDict(extra="allow")
