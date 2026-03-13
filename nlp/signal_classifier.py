SIGNAL_KEYWORDS = [
    "office", "lease", "expand", "expansion", "hire", "hiring",
    "headquarter", "hq", "relocate", "new office", "workspace",
    "coworking", "seat", "sqft", "sq ft", "floor", "tower",
    "campus", "facility", "warehouse", "logistics", "park",
    "funding", "raises", "series a", "series b", "series c",
    "investment", "backed", "unicorn", "valuation", "ipo",
    "employees", "staff", "headcount", "team size", "workforce",
    "opens", "launch", "setup", "inaugurate", "new branch",
    "data center", "r&d center", "innovation hub", "tech park"
]

SPACE_KEYWORDS = ["office", "warehouse", "coworking", "campus", "facility", 
                  "data center", "tech park", "r&d center"]

INDIA_LOCATIONS = [
    "mumbai", "navi mumbai", "thane", "pune", "delhi", "ncr", "gurugram",
    "gurgaon", "noida", "bengaluru", "bangalore", "hyderabad", "chennai",
    "kolkata", "ahmedabad", "surat", "jaipur", "lucknow", "india", "indian",
    "bkc", "lower parel", "andheri", "powai", "whitefield", "hsr layout",
    "koramangala", "cyberabad", "hitec city", "gachibowli"
]

def classify_signal(article: dict) -> dict:
    title = (article.get('title') or '').lower()
    summary = (article.get('summary') or '').lower()
    text = title + ' ' + summary

    signal_hits = [kw for kw in SIGNAL_KEYWORDS if kw in text]

    if not signal_hits:
        return None

    location_hit = any(loc in text for loc in INDIA_LOCATIONS)

    score = len(signal_hits) * 10
    if location_hit:
        score += 20

    return {
        "signal_type": signal_hits[0].upper(),
        "confidence": min(score, 100),
        "space_type": next((kw for kw in signal_hits if kw in SPACE_KEYWORDS), None),
        "matched_phrases": signal_hits[:5],
        "location_hit": location_hit
    }

def extract_summary(text: str, matched_phrases: list) -> str:
    return text[:500]
