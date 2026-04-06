import pytest
from src.pipelines.extract import extractor
from src.core.models import Lead

def test_extract_lead_with_full_info():
    """Test extracting a lead from text containing names, phone, and disease."""
    # Input text with common patterns
    text = "Hello, my name is John Doe. I am from Texas and have been struggling with diabetes for 5 years. You can reach me at 555-123-4567."
    source = "reddit"
    url = "https://reddit.com/r/health/123"
    
    lead = extractor.extract_lead(text, source, url, "123")
    
    assert isinstance(lead, Lead)
    assert lead.first_name == "John"
    assert lead.last_name == "Doe"
    assert lead.phone_number == "555-123-4567"
    assert lead.state.lower() == "texas"
    assert "diabetes" in lead.disease_history.lower()
    assert lead.source == source

def test_extract_lead_no_info():
    """Test extracting a lead from text with no matching patterns."""
    text = "This is a random post about nothing."
    source = "reddit"
    url = "https://reddit.com/r/health/456"
    
    lead = extractor.extract_lead(text, source, url, "456")
    
    assert lead.first_name is None
    assert lead.phone_number is None
    assert lead.disease_history is None
    assert lead.source == source

def test_extract_multiple_diseases():
    """Test extracting multiple diseases from the same text."""
    text = "I have hypertension and also deal with anxiety."
    lead = extractor.extract_lead(text, "reddit", "url")
    
    assert "hypertension" in lead.disease_history.lower()
    assert "anxiety" in lead.disease_history.lower()
    assert "," in lead.disease_history
