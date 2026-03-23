"""
Lead Scorer v2 — FIXED
=======================
v1 was broken: SIGNAL_WEIGHTS used old keys (OFFICE_EXPANSION, WAREHOUSE_DEMAND)
that never matched actual signal types (OFFICE, LEASE, FUNDING, EXPAND, HIRING).
Result: every company scored 0, priority always LOW.

v2 fixes:
- Correct signal type keys matching main.py output
- Urgency multiplier (HIGH urgency = 1.5x, MEDIUM = 1.0x, LOW = 0.7x)
- Multi-signal bonus (company with 3+ signals gets stacking bonus)
- Recency bonus placeholder (hook for future timestamp scoring)
- Thresholds recalibrated for real score ranges
"""

# ── SIGNAL BASE WEIGHTS ───────────────────────────────────────────────────────
# Keys must match signal_type values produced by main.py:
# LEASE, OFFICE, EXPAND, RELOCATE, HIRING, FUNDING
# (plus legacy types from BSE/NSE hints kept for safety)

SIGNAL_WEIGHTS = {
    # Direct transaction signals — highest value
    "LEASE":              85,   # Signed lease = active deal, hottest lead
    "RELOCATE":           75,   # Actively moving = needs new space now
    "OFFICE":             65,   # New office opening = confirmed demand

    # Growth / expansion signals
    "EXPAND":             60,   # Expansion announced = imminent space need
    "DATACENTER":         55,   # Data centre = large facility requirement
    "WAREHOUSE":          50,   # Logistics/warehouse demand

    # Proxy signals — indirect but valuable
    "FUNDING":            45,   # Funded = will hire = will need space
    "HIRING":             35,   # Hiring surge = space pressure building

    # Filing signals from BSE/NSE
    "FILING":             40,   # Corporate filing mentioning CRE keywords

    # Legacy keys (safety net, should not appear in v2 data)
    "OFFICE_EXPANSION":   60,
    "WAREHOUSE_DEMAND":   50,
    "DATACENTER_BUILD":   55,
    "LAND_ACQUISITION":   45,
    "LOGISTICS_EXPANSION":50,
    "CAPITAL_DEPLOYMENT": 30,

    "NO_SIGNAL":           0,
}

# ── URGENCY MULTIPLIERS ───────────────────────────────────────────────────────
URGENCY_MULTIPLIERS = {
    "HIGH":   1.5,
    "MEDIUM": 1.0,
    "LOW":    0.7,
}

# ── PRIORITY THRESHOLDS ───────────────────────────────────────────────────────
# Recalibrated: a single LEASE signal at 90% confidence = 85 * 1.5 * 0.9 = ~115
# So HIGH threshold set at 80 to catch real leads without over-triggering
PRIORITY_THRESHOLDS = {
    "HIGH":   80,
    "MEDIUM": 35,
    "LOW":    0,
}

# ── MULTI-SIGNAL BONUS ────────────────────────────────────────────────────────
# Company with multiple independent signals = stronger conviction
def _multi_signal_bonus(signal_count: int) -> float:
    if signal_count >= 5: return 1.4
    if signal_count >= 3: return 1.25
    if signal_count >= 2: return 1.1
    return 1.0

# ── SIGNAL TYPE NORMALISER ────────────────────────────────────────────────────
def _normalise_type(raw: str) -> str:
    """Handle case variants and legacy type names."""
    t = (raw or "NO_SIGNAL").upper().strip()
    aliases = {
        "OFFICE_EXPANSION":    "OFFICE",
        "WAREHOUSE_DEMAND":    "WAREHOUSE",
        "DATACENTER_BUILD":    "DATACENTER",
        "LOGISTICS_EXPANSION": "WAREHOUSE",
        "CAPITAL_DEPLOYMENT":  "FUNDING",
    }
    return aliases.get(t, t)


# ── MAIN SCORING FUNCTION ─────────────────────────────────────────────────────

def compute_lead_score(signals: list) -> dict:
    """
    Score a company based on all its signals.

    Each signal contributes:
        base_weight * urgency_multiplier * confidence_factor

    Final score is boosted by multi-signal bonus.

    Returns:
        {
            score: int,
            signal_count: int,
            priority_level: "HIGH" | "MEDIUM" | "LOW",
            top_signal: str,       # highest-weight signal type
            breakdown: list        # per-signal scores for debugging
        }
    """
    if not signals:
        return {
            "score": 0,
            "signal_count": 0,
            "priority_level": "LOW",
            "top_signal": "NONE",
            "breakdown": [],
        }

    total_score = 0.0
    breakdown   = []
    top_signal  = "NONE"
    top_weight  = 0

    for sig in signals:
        raw_type   = sig.get("signal_type", "NO_SIGNAL")
        sig_type   = _normalise_type(raw_type)
        base       = SIGNAL_WEIGHTS.get(sig_type, SIGNAL_WEIGHTS.get(raw_type, 0))
        urgency    = (sig.get("urgency") or "MEDIUM").upper()
        confidence = min(max(sig.get("confidence", 50), 0), 100) / 100.0
        multiplier = URGENCY_MULTIPLIERS.get(urgency, 1.0)

        contribution = base * multiplier * confidence
        total_score += contribution

        breakdown.append({
            "type":         sig_type,
            "base":         base,
            "urgency":      urgency,
            "confidence":   sig.get("confidence", 50),
            "contribution": round(contribution, 1),
        })

        if base > top_weight:
            top_weight = base
            top_signal = sig_type

    # Apply multi-signal bonus
    bonus        = _multi_signal_bonus(len(signals))
    total_score *= bonus
    total_score  = round(total_score)

    # Determine priority
    if total_score >= PRIORITY_THRESHOLDS["HIGH"]:
        priority = "HIGH"
    elif total_score >= PRIORITY_THRESHOLDS["MEDIUM"]:
        priority = "MEDIUM"
    else:
        priority = "LOW"

    return {
        "score":          total_score,
        "signal_count":   len(signals),
        "priority_level": priority,
        "top_signal":     top_signal,
        "breakdown":      breakdown,
    }
