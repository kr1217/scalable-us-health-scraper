import re
from datetime import datetime
from typing import Optional, Dict, Any
from ..core.models import Lead

class ExtractionPipeline:
    def __init__(self):
        # Regex patterns for core health lead extraction
        self.phone_pattern = re.compile(r'\+?1?\s*\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}')
        self.state_pattern = re.compile(
            r'\b(?:AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY|Alabama|Alaska|Arizona|Arkansas|California|Colorado|Connecticut|Delaware|Florida|Georgia|Hawaii|Idaho|Illinois|Indiana|Iowa|Kansas|Kentucky|Louisiana|Maine|Maryland|Massachusetts|Michigan|Minnesota|Mississippi|Missouri|Montana|Nebraska|Nevada|New Hampshire|New Jersey|New Mexico|New York|North Carolina|North Dakota|Ohio|Oklahoma|Oregon|Pennsylvania|Rhode Island|South Carolina|South Dakota|Tennessee|Texas|Utah|Vermont|Virginia|Washington|West Virginia|Wisconsin|Wyoming)\b',
            re.IGNORECASE
        )
        # Internal age regex to help estimate Date of Birth
        self.age_pattern = re.compile(r'\b(\d{1,2})\s*(?:yo|y.o|years old|age|y\/o)\b', re.IGNORECASE)
        self.age_gender_shorthand = re.compile(r'\b(\d{1,2})\s*([MFmf])\b')

        self.disease_keywords = [
            "diabetes", "cancer", "heart disease", "hypertension", "arthritis",
            "alzheimer", "parkinson", "crohn", "lupus", "ms", "fibromyalgia",
            "autism", "adhd", "anxiety", "depression", "bipolar", "schizophrenia"
        ]

    def extract_lead(self, text: str, source: str, url: str, source_id: Optional[str] = None) -> Lead:
        """Extract lead information (Name, DOB, Phone, Location, Disease) from raw text."""
        
        # 1. Phone Number
        phone_match = self.phone_pattern.search(text)
        phone = phone_match.group(0) if phone_match else None

        # 2. Date of Birth (Estimate from age if direct DOB is missing)
        dob = None
        current_year = datetime.now().year
        
        # Try to find age to calculate approximate birth year
        age_found = None
        sh_match = self.age_gender_shorthand.search(text)
        if sh_match:
            age_found = int(sh_match.group(1))
        else:
            age_match = self.age_pattern.search(text)
            if age_match:
                age_found = int(age_match.group(1))
        
        if age_found:
            dob = str(current_year - age_found)
        
        # Also check for direct birth years (e.g. "born in 1990")
        year_match = re.search(r'\b(?:born in|birth year|born)\b\s*(\d{4})', text, re.IGNORECASE)
        if year_match:
            dob = year_match.group(1)

        # 3. Names
        name_match = re.search(r'(?i)(?:I am|my name is)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)', text)
        first_name, last_name = None, None
        if name_match:
            full_name = name_match.group(1).split()
            first_name = full_name[0] if full_name else None
            last_name = full_name[1] if len(full_name) > 1 else None

        # 4. Location (State)
        state_match = self.state_pattern.search(text)
        state = state_match.group(0) if state_match else None

        # 5. Disease History
        diseases = []
        for keyword in self.disease_keywords:
            if re.search(rf'\b{keyword}\b', text, re.IGNORECASE):
                diseases.append(keyword)
        disease_history = ", ".join(diseases) if diseases else None

        return Lead(
            first_name=first_name,
            last_name=last_name,
            date_of_birth=dob,
            phone_number=phone,
            state=state,
            disease_history=disease_history,
            source=source,
            source_url=url,
            source_id=source_id
        )

# Global extractor instance
extractor = ExtractionPipeline()
