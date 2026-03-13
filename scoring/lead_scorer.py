SIGNAL_WEIGHTS = {
    "OFFICE_EXPANSION": 40,
    "WAREHOUSE_DEMAND": 35,
    "DATACENTER_BUILD": 50,
    "LAND_ACQUISITION": 45,
    "LOGISTICS_EXPANSION": 35,
    "CAPITAL_DEPLOYMENT": 30,
    "NO_SIGNAL": 0
}

PRIORITY_THRESHOLDS = {"HIGH": 60, "MEDIUM": 30, "LOW": 0}

def compute_lead_score(signals: list) -> dict:
    total_score = 0
    for signal in signals:
        base = SIGNAL_WEIGHTS.get(signal.get("signal_type", "NO_SIGNAL"), 0)
        confidence_factor = signal.get("confidence", 50) / 100
        total_score += base * confidence_factor
    total_score = round(total_score)
    signal_count = len(signals)
    if total_score >= PRIORITY_THRESHOLDS["HIGH"]:
        priority = "HIGH"
    elif total_score >= PRIORITY_THRESHOLDS["MEDIUM"]:
        priority = "MEDIUM"
    else:
        priority = "LOW"
    return {"score": total_score, "signal_count": signal_count, "priority_level": priority}
