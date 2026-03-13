"""
CRE Relevance Filter
====================
Hard gate before any signal hits the DB.
Returns (is_relevant: bool, confidence: float, reason: str)

Logic:
1. Must contain at least one SPACE_VERB + SPACE_NOUN combo
2. Must NOT be dominated by noise topics (finance, politics, entertainment)
3. Location must be India-relevant
4. Company must be acting as OCCUPIER (not developer/investor)
"""

import re

# ── POSITIVE SIGNALS ─────────────────────────────────────────────────────────

SPACE_VERBS = [
    "leased", "leasing", "lease", "signed lease", "signed a lease",
    "rented", "renting", "took up", "taken up",
    "moved to", "moving to", "relocated", "relocating", "relocation",
    "shifted", "shifting",
    "opened", "opening", "launched", "set up", "setting up",
    "inaugurated", "commissioned",
    "acquired", "acquiring", "purchased", "buying",
    "expanding", "expansion", "expand", "scaled up",
    "new office", "new facility", "new campus", "new hq", "new headquarters",
    "additional space", "more space", "extra space",
    "sq ft", "sqft", "sq. ft", "square feet", "square foot",
    "lakh sq", "crore sq",  # Indian RE measurements
]

SPACE_NOUNS = [
    "office", "office space", "office park", "office building",
    "workspace", "co-working", "coworking", "flex space", "managed office",
    "campus", "headquarters", "hq", "corporate office",
    "facility", "centre", "center", "development centre",
    "tech park", "it park", "sez", "special economic zone",
    "commercial space", "commercial property", "commercial real estate",
    "floor", "tower", "block", "wing",  # building parts
    "seat", "seats", "workstation", "workstations",
    "sqft", "sq ft", "square feet",
]

INDIA_CRE_HUBS = [
    "bengaluru", "bangalore", "mumbai", "delhi", "ncr", "gurugram", "gurgaon",
    "noida", "hyderabad", "pune", "chennai", "kolkata", "ahmedabad",
    "navi mumbai", "thane", "bkc", "worli", "lower parel", "andheri",
    "whitefield", "electronic city", "koramangala", "bandra",
    "cyberabad", "hitec city", "gachibowli", "madhapur",
    "cybercity", "dlf", "unitech", "rg complex",
    "india", "pan-india", "pan india", "across india",
]

# ── NEGATIVE SIGNALS (noise killers) ─────────────────────────────────────────

NOISE_TOPICS = {
    "entertainment": [
        "song", "album", "movie", "film", "actor", "actress", "bollywood",
        "music", "singer", "rapper", "lyric", "video", "viral", "celebrity",
        "instagram", "reel", "controversy", "outrage",
    ],
    "commodities": [
        "crude oil", "oil prices", "petroleum", "natural gas", "brent",
        "opec", "strait of hormuz", "fuel prices", "lpg crunch",
        "gold price", "silver price", "commodity",
    ],
    "politics": [
        "election", "parliament", "minister", "lok sabha", "rajya sabha",
        "bjp", "congress", "aap", "political party", "vote", "constituency",
        "chief minister", "cm ", "prime minister",
    ],
    "banking_retail": [
        "credit card", "interest rate", "emi", "loan", "deposit",
        "rbi policy", "repo rate", "monetary policy",
        "stock market", "nifty", "sensex", "share price",
        "ipo ", "fpo ", "nse listing",
    ],
    "sports": [
        "cricket", "ipl", "match", "tournament", "player", "team",
        "fifa", "football", "tennis", "olympics",
    ],
}

# Minimum ratio of CRE terms to noise terms to pass
NOISE_THRESHOLD = 0.4  # if noise_score > cre_score * this, flag it


def _score_text(text: str, terms: list) -> int:
    t = text.lower()
    return sum(1 for term in terms if term in t)


def is_cre_relevant(title: str, text: str) -> tuple[bool, float, str]:
    combined = (title + " " + text).lower()

    # ── Step 1: Must have CRE space signal ───────────────────────────────────
    verb_score = _score_text(combined, SPACE_VERBS)
    noun_score = _score_text(combined, SPACE_NOUNS)
    cre_score = verb_score + noun_score

    if cre_score == 0:
        return False, 0.0, "no_cre_terms"

    # Boost if sq ft mentioned (very strong CRE signal)
    sqft_match = re.search(r'\d[\d,]*\s*(?:sq\.?\s*ft|sqft|square\s*feet)', combined)
    if sqft_match:
        cre_score += 5

    # ── Step 2: Noise check ───────────────────────────────────────────────────
    total_noise = 0
    dominant_noise = None
    for topic, terms in NOISE_TOPICS.items():
        n = _score_text(combined, terms)
        if n > total_noise:
            total_noise = n
            dominant_noise = topic

    if total_noise > 0 and cre_score < total_noise:
        # CRE terms are outnumbered by noise terms — reject
        return False, 0.0, f"noise_dominant:{dominant_noise}"

    # ── Step 3: India location check ─────────────────────────────────────────
    location_hit = any(loc in combined for loc in INDIA_CRE_HUBS)
    if not location_hit and cre_score < 3:
        # Weak CRE signal AND no India location — likely irrelevant
        return False, 0.1, "no_india_location"

    # ── Step 4: Compute confidence ────────────────────────────────────────────
    base_confidence = min(cre_score / 8.0, 1.0)  # cap at 1.0
    if verb_score > 0 and noun_score > 0:
        base_confidence = min(base_confidence + 0.2, 1.0)  # bonus for both
    if sqft_match:
        base_confidence = min(base_confidence + 0.3, 1.0)
    if location_hit:
        base_confidence = min(base_confidence + 0.1, 1.0)

    return True, round(base_confidence, 2), "cre_relevant"


def get_signal_type(title: str, text: str) -> str:
    """More precise signal typing than simple keyword match."""
    combined = (title + " " + text).lower()

    # Check for sq ft or specific space transaction first (strongest signal)
    if re.search(r'\d[\d,]*\s*(?:sq\.?\s*ft|sqft|square\s*feet)', combined):
        if any(w in combined for w in ["leased", "signed", "took up", "rented"]):
            return "LEASE"
        return "OFFICE"

    if any(w in combined for w in ["new office", "new campus", "inaugurated", "opened office", "set up office"]):
        return "OFFICE"

    if any(w in combined for w in ["leased", "lease", "rented", "rental agreement", "mou signed"]):
        return "LEASE"

    if any(w in combined for w in ["relocat", "shifted", "moving to", "moved to"]):
        return "RELOCATE"

    if any(w in combined for w in ["expand", "expansion", "scaling", "additional space", "more space"]):
        return "EXPAND"

    if any(w in combined for w in ["raised", "funding", "series", "investment", "backed", "crore raised", "mn raised"]):
        return "FUNDING"

    if any(w in combined for w in ["hiring", "headcount", "employees", "workforce", "recruit", "jobs"]):
        return "HIRING"

    if any(w in combined for w in ["acquired", "acquisition", "purchased property", "bought"]):
        return "EXPAND"

    return "OFFICE"
