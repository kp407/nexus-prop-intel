import json
import os

def load_keywords():
    try:
        with open('config/keywords.json') as f:
            return json.load(f)
    except:
        return {}

LOCATION_KEYWORDS = [
    "mumbai", "navi mumbai", "bkc", "bandra kurla", "lower parel",
    "andheri", "powai", "thane", "worli", "nariman point", "goregaon",
    "malad", "vikhroli", "pune", "delhi", "bengaluru", "hyderabad",
    "chennai", "india", "indian"
]

SIGNAL_KEYWORDS = [
    "office", "lease", "expand", "expansion", "hire", "hiring",
    "headquarter", "hq", "relocate", "new office", "workspace",
    "coworking", "seat", "sqft", "sq ft", "floor", "tower",
    "campus", "facility", "warehouse", "logistics", "park",
    "funding", "raises", "series", "investment", "backed",
    "unicorn", "startup", "company", "employees", "staff",
    "headcount", "team", "opens", "launch", "setup"
]

def classify_signal(article: dict) -> dict:
    title = (article.get('title') or '').lower()
    summary = (article.get('summary') or '').lower()
    text = title + ' ' + summary

    # Check for any location keyword
    location_hit = any(loc in text for loc in LOCATION_KEYWORDS)
    
    # Check for any signal keyword
    signal_hits = [kw for kw in SIGNAL_KEYWORDS if kw in text]
    
    if not signal_hits:
        return None

    # Score based on hits
    score = len(signal_hits) * 10
    if location_hit:
        score += 30
    if any(loc in text for loc in ["mumbai", "bkc", "lower parel", "andheri", "powai", "thane"]):
        score += 20

    if score < 10:
        return None

    return {
        "signal_type": signal_hits[0] if signal_hits else "general",
        "score": min(score, 100),
        "priority": "HIGH" if score >= 60 else "MEDIUM" if score >= 30 else "LOW",
        "keywords_matched": signal_hits[:5],
        "location_hit": location_hit


    }


def extract_summary(article: dict) -> str:
    title = article.get('title') or ''
    summary = article.get('summary') or ''
    text = summary if summary else title
    return text[:500]
