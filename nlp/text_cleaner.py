import re
import hashlib

def clean_text(text: str) -> str:
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'[^\x00-\x7F]+', ' ', text)
    return text.strip()

def deduplicate(records: list, key="url") -> list:
    seen = set()
    unique = []
    for r in records:
        identifier = hashlib.md5(r.get(key, "").encode()).hexdigest()
        if identifier not in seen:
            seen.add(identifier)
            unique.append(r)
    return unique

def normalize_company_name(name: str) -> str:
    suffixes = [r'\bLtd\.?\b', r'\bLimited\b', r'\bPvt\.?\b',
                r'\bInc\.?\b', r'\bCorp\.?\b', r'\bLLP\b']
    for s in suffixes:
        name = re.sub(s, '', name, flags=re.IGNORECASE)
    return name.strip()
