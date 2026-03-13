import json

SIGNAL_KEYWORDS = [
    "office", "lease", "expand", "expansion", "hire", "hiring",
    "headquarter", "hq", "relocate", "new office", "workspace",
    "coworking", "seat", "sqft", "sq ft", "floor", "tower",
    "campus", "facility", "warehouse", "logistics", "park",
    "funding", "raises", "series", "investment", "backed",
    "unicorn", "startup", "company", "employees", "staff",
    "headcount", "team", "opens", "launch", "setup"
]

LOCATION_KEYWORDS = [
    "mumbai", "navi mumbai", "bkc", "bandra kurla", "lower parel",
    "andheri", "powai", "thane", "worli", "nariman point", "goregaon",
    "malad", "vikhroli", "india", "indian"
]

SPACE_KEYWORDS = ["office", "warehouse", "coworking", "campus", "facility"]

def classify_signal(article: dict) -> dict:
    title = (article.get('title') or '').lower()
    summary = (article.get('summary') or '').lower()
    text = title + ' ' + summary

    location_hit = any(loc in text for loc in LOCATION_KEYWORDS)
    signal_hits = [kw for kw in SIGNAL_KEYWORDS if kw in text]

    if not signal_hits:
        return None

    score = len(signal_hits) * 10
    if location_hit:
        score += 30
    if any(loc in text for loc in ["mumbai", "bkc", "lower parel", "andheri", "powai", "thane"]):
        score += 20

    if score < 10:
        return None

    return {
        "signal_type": signal_hits[0].upper(),
        "confidence": min(score, 100),
        "space_type": next((kw for kw in signal_hits if kw in SPACE_KEYWORDS), None),
        "matched_phrases": signal_hits[:5],
        "location_hit": location_hit
    }

def extract_summary(text: str, matched_phrases: list) -> str:
    return text[:500]
