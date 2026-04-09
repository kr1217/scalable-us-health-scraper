import hashlib
from typing import Optional

def generate_identity_hash(
    first_name: Optional[str], 
    last_name: Optional[str], 
    phone: Optional[str], 
    state: Optional[str]
) -> str:
    """
    Generates a unique SHA-256 fingerprint for a person based on their 
    core identity fields. Used for de-duplication across platforms.
    """
    # Expert Patch: Don't hash empty data (prevents "Global Anonymous" collisions)
    if not any([first_name, last_name, phone, state]):
        return None

    # Normalize strings to ensure consistent hashing
    def normalize(s):
        return str(s).strip().lower() if s else ""

    components = [
        normalize(first_name),
        normalize(last_name),
        normalize(phone),
        normalize(state)
    ]
    
    # Create a joined string and hash it
    raw_id = "|".join(components)
    return hashlib.sha256(raw_id.encode()).hexdigest()
