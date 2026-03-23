"""
CRE Intent Intelligence Layer
==============================
Converts indirect signals into CRE leads using business logic:

1. FUNDING → "Company raised capital, will expand headcount → needs space"
2. FOREIGN ENTRY → "Global co entering India → needs first office"
3. HIRING SURGE → "X00+ jobs in one city → facility expansion imminent"
4. UNICORN/GROWTH → "Valuation milestone → prestige HQ upgrade likely"
5. NEW SUBSIDIARY → "MCA incorporation → new entity needs registered office"
6. LEASE EXPIRY PROXY → "Old office news + age → renewal opportunity"

Each intent maps to a CRE_LEAD with:
- signal_type
- confidence_score
- why_cre (human-readable reason for the lead)
- urgency (HIGH/MEDIUM/LOW)
- suggested_action (what the broker should do)
"""

import re

# ── FUNDING INTELLIGENCE ──────────────────────────────────────────────────────

FUNDING_ROUNDS = {
    "seed": {"urgency": "LOW", "confidence": 35,
             "reason": "Early-stage funding — will likely need first proper office in 6-12 months"},
    "pre-series a": {"urgency": "MEDIUM", "confidence": 45,
                     "reason": "Pre-Series A — team scaling, co-working likely insufficient soon"},
    "series a": {"urgency": "MEDIUM", "confidence": 55,
                 "reason": "Series A — typically 20-50 person team, need dedicated office now"},
    "series b": {"urgency": "HIGH", "confidence": 70,
                 "reason": "Series B — aggressive hiring planned, space requirement imminent"},
    "series c": {"urgency": "HIGH", "confidence": 75,
                 "reason": "Series C — multi-city expansion, multiple office requirements"},
    "series d": {"urgency": "HIGH", "confidence": 80,
                 "reason": "Series D+ — large campus or HQ upgrade likely in pipeline"},
    "pre-ipo": {"urgency": "HIGH", "confidence": 85,
                "reason": "Pre-IPO — company needs prestigious address before listing"},
    "ipo": {"urgency": "HIGH", "confidence": 85,
            "reason": "IPO/listing — HQ upgrade and compliance space required"},
}

# Amount thresholds (in crores) → urgency boost
FUNDING_THRESHOLDS = [
    (500, "HIGH", 80),    # ₹500Cr+ = large space need
    (100, "HIGH", 70),    # ₹100Cr+ = significant expansion
    (50,  "MEDIUM", 60),  # ₹50Cr+ = moderate expansion
    (10,  "MEDIUM", 45),  # ₹10Cr+ = some growth
]


def parse_funding_amount_cr(text: str) -> float:
    """Extract funding amount and normalize to crores."""
    text = text.lower()

    # Match patterns like "₹50 crore", "$5 million", "Rs 200 cr"
    patterns = [
        (r'(?:rs\.?|inr|₹)\s*([\d,]+(?:\.\d+)?)\s*crore', 1.0),
        (r'(?:rs\.?|inr|₹)\s*([\d,]+(?:\.\d+)?)\s*cr\b', 1.0),
        (r'(?:rs\.?|inr|₹)\s*([\d,]+(?:\.\d+)?)\s*lakh', 0.01),
        (r'\$\s*([\d,]+(?:\.\d+)?)\s*million', 8.3),   # ~₹8.3Cr per $1M
        (r'\$\s*([\d,]+(?:\.\d+)?)\s*mn', 8.3),
        (r'([\d,]+(?:\.\d+)?)\s*million\s*(?:dollar|usd)', 8.3),
        (r'\$\s*([\d,]+(?:\.\d+)?)\s*billion', 8300.0),
        (r'([\d,]+(?:\.\d+)?)\s*crore', 1.0),
    ]

    for pattern, multiplier in patterns:
        m = re.search(pattern, text)
        if m:
            try:
                amount = float(m.group(1).replace(",", ""))
                return amount * multiplier
            except ValueError:
                pass
    return 0.0


def analyze_funding_intent(title: str, text: str, location: str) -> dict | None:
    """If article is about funding, generate CRE lead."""
    combined = (title + " " + text).lower()

    # Must be a funding article
    funding_triggers = ["raised", "raises", "funding", "series", "investment",
                        "backed", "seed round", "pre-series", "ipo", "listing"]
    if not any(t in combined for t in funding_triggers):
        return None

    # Must be India company (not overseas funding news)
    india_signals = ["india", "bengaluru", "bangalore", "mumbai", "hyderabad", "pune",
                     "delhi", "ncr", "gurugram", "gurgaon", "noida", "greater noida",
                     "chennai", "kolkata", "ahmedabad", "surat", "jaipur", "lucknow",
                     "chandigarh", "mohali", "kochi", "coimbatore", "nagpur", "indore",
                     "bkc", "lower parel", "andheri", "whitefield", "hitec city",
                     "gachibowli", "hinjewadi", "kharadi", "salt lake", "rajarhat",
                     "gift city", "aerocity", "cyber city", "electronic city",
                     "pan-india", "pan india", "across india", "indian"]
    if not any(loc in combined for loc in india_signals):
        return None

    # Determine round type
    round_info = {"urgency": "MEDIUM", "confidence": 40,
                  "reason": "Company received funding — likely to expand team and require office space"}
    for round_name, info in FUNDING_ROUNDS.items():
        if round_name in combined:
            round_info = info
            break

    # Check funding amount
    amount_cr = parse_funding_amount_cr(combined)
    if amount_cr > 0:
        for threshold, urgency, conf in FUNDING_THRESHOLDS:
            if amount_cr >= threshold:
                round_info["urgency"] = urgency
                round_info["confidence"] = max(round_info["confidence"], conf)
                round_info["reason"] += f" (₹{amount_cr:.0f}Cr raised)"
                break

    return {
        "signal_type": "FUNDING",
        "confidence_score": round_info["confidence"],
        "why_cre": round_info["reason"],
        "urgency": round_info["urgency"],
        "suggested_action": "Contact within 2 weeks — present co-working/managed office options for immediate need, long-term lease for 12-18 months out",
        "amount_cr": amount_cr,
    }


# ── FOREIGN ENTRY INTELLIGENCE ────────────────────────────────────────────────

FOREIGN_ENTRY_SIGNALS = [
    "enters india", "entry into india", "entering india", "india entry",
    "launches in india", "launch in india", "india launch",
    "expands to india", "expansion to india", "india expansion",
    "sets up in india", "set up in india", "india operations",
    "india subsidiary", "india office", "india headquarters",
    "india presence", "forays into india", "foray into india",
    "india debut", "debuts in india",
    "global capability centre", "gcc", "global delivery centre",
    "captive centre", "india gdc", "india gsc",
    "india unit", "india arm", "india entity",
    "registered in india", "incorporated in india",
]

HIGH_VALUE_FOREIGN = [
    # Fortune 500 / well-known globals
    "microsoft", "google", "amazon", "apple", "meta", "salesforce",
    "oracle", "sap", "adobe", "servicenow", "workday",
    "jpmorgan", "goldman", "morgan stanley", "blackrock", "kkr",
    "mckinsey", "bcg", "bain", "deloitte", "pwc", "ey", "kpmg",
    "airbus", "boeing", "siemens", "bosch", "schneider",
    "walmart", "target", "ikea", "zara", "h&m",
]


def analyze_foreign_entry_intent(title: str, text: str) -> dict | None:
    """If article is about a foreign company entering India, generate CRE lead."""
    combined = (title + " " + text).lower()

    if not any(sig in combined for sig in FOREIGN_ENTRY_SIGNALS):
        return None

    # Determine if high-value company
    is_high_value = any(co in combined for co in HIGH_VALUE_FOREIGN)
    confidence = 80 if is_high_value else 65

    # GCC specifically = guaranteed large office need
    is_gcc = any(gcc in combined for gcc in ["gcc", "global capability centre",
                                              "global delivery centre", "captive centre"])
    if is_gcc:
        confidence = 90
        reason = "Global Capability Centre (GCC) setup — typically 500-5000 seats, Grade A office in metro required immediately"
        urgency = "HIGH"
        action = "Reach out immediately — GCC setups need 50,000-500,000 sq ft in Bengaluru/Hyderabad/Pune. Present pre-committed space options."
    elif is_high_value:
        reason = "Major global brand entering India — will need flagship India HQ in Grade A building"
        urgency = "HIGH"
        action = "Priority outreach — present premium Grade A options in BKC/Bandra/Whitefield/HITEC City"
    else:
        reason = "Foreign company entering India market — first office requirement guaranteed"
        urgency = "MEDIUM"
        action = "Contact within 1 week — present managed office / co-working as first step, then long-term lease"

    return {
        "signal_type": "EXPAND",
        "confidence_score": confidence,
        "why_cre": reason,
        "urgency": urgency,
        "suggested_action": action,
        "is_gcc": is_gcc,
    }


# ── GROWTH MILESTONE INTELLIGENCE ─────────────────────────────────────────────

GROWTH_SIGNALS = {
    "unicorn": {
        "confidence": 75,
        "reason": "Unicorn valuation milestone — HQ upgrade to premium address almost certain within 12 months",
        "urgency": "HIGH",
        "action": "Present flagship HQ options — unicorns want visible, prestigious addresses",
    },
    "ipo": {
        "confidence": 80,
        "reason": "IPO filing/listing — board room, compliance space, investor relations office needed",
        "urgency": "HIGH",
        "action": "Approach CFO/COO directly — IPO prep creates immediate need for upgraded premises",
    },
    "acqui": {  # acquisition/merger
        "confidence": 65,
        "reason": "M&A activity — office consolidation or new combined HQ likely in 6-18 months",
        "urgency": "MEDIUM",
        "action": "Flag for 6-month follow-up — post-merger office strategy will need CRE partner",
    },
    "merger": {
        "confidence": 65,
        "reason": "Merger announced — combined entity will rationalize or upgrade office portfolio",
        "urgency": "MEDIUM",
        "action": "Flag for 6-month follow-up — post-merger office strategy will need CRE partner",
    },
    "headcount": {
        "confidence": 55,
        "reason": "Significant headcount growth announced — current space likely insufficient",
        "urgency": "MEDIUM",
        "action": "Contact HR/Admin head — headcount growth of 20%+ typically triggers space review",
    },
    "data center": {
        "confidence": 70,
        "reason": "Data centre / tech infrastructure expansion — support office space also required",
        "urgency": "MEDIUM",
        "action": "Present campus options near major data centre corridors (Chennai, Pune, Hyderabad)",
    },
}


def analyze_growth_intent(title: str, text: str) -> dict | None:
    """Detect growth milestones that imply CRE need."""
    combined = (title + " " + text).lower()

    india_signals = ["india", "bengaluru", "bangalore", "mumbai", "hyderabad", "pune",
                     "delhi", "ncr", "gurugram", "gurgaon", "noida", "greater noida",
                     "chennai", "kolkata", "ahmedabad", "surat", "jaipur", "lucknow",
                     "chandigarh", "mohali", "kochi", "coimbatore", "nagpur", "indore",
                     "bkc", "lower parel", "andheri", "whitefield", "hitec city",
                     "gachibowli", "hinjewadi", "kharadi", "salt lake", "rajarhat",
                     "gift city", "aerocity", "cyber city", "electronic city",
                     "pan-india", "pan india", "across india", "indian"]
    if not any(loc in combined for loc in india_signals):
        return None

    for keyword, info in GROWTH_SIGNALS.items():
        if keyword in combined:
            return {
                "signal_type": "EXPAND",
                "confidence_score": info["confidence"],
                "why_cre": info["reason"],
                "urgency": info["urgency"],
                "suggested_action": info["action"],
            }

    return None


# ── MASTER INTENT ANALYZER ────────────────────────────────────────────────────

def analyze_cre_intent(title: str, text: str, location: str = "India") -> dict | None:
    """
    Master function — tries all intent analyzers in priority order.
    Returns the highest-confidence CRE lead found, or None.
    """
    candidates = []

    intent = analyze_foreign_entry_intent(title, text)
    if intent:
        candidates.append(intent)

    intent = analyze_funding_intent(title, text, location)
    if intent:
        candidates.append(intent)

    intent = analyze_growth_intent(title, text)
    if intent:
        candidates.append(intent)

    if not candidates:
        return None

    # Return highest confidence
    return max(candidates, key=lambda x: x["confidence_score"])
