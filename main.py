"""
NEXUS PROP INTEL — Main Orchestrator FINAL
"""
import json
from crawler.rss_crawler import RSSCrawler
from crawler.news_crawler import NewsCrawler
from crawler.primary_sources import BSECrawler, FilingPDFCrawler, LinkedInSignalCrawler
from nlp.cre_filter import is_cre_relevant, get_signal_type
from nlp.cre_intent import analyze_cre_intent
from nlp.entity_extractor import extract_entities
from nlp.signal_classifier import classify_signal, extract_summary
from nlp.text_cleaner import clean_text, deduplicate
from scoring.lead_scorer import compute_lead_score
from database.db_client import get_client, upsert_company, insert_signal, upsert_lead_score

with open("config/sources.json") as f:
    SOURCES = json.load(f)

JUNK_NAMES = [
    "href=", "&#", "cin:", "dalal street", "5th floor", "limited cin",
    "stock exchange", "bse limited", "nse limited", "listing department",
    "p.j. tower", "g-block", "g block", "khasra", "plot ", "kisl/",
    "delphi", "assemblies limited", "compliance officer",
]

def refresh_database(client):
    print("[DB] Refreshing...")
    for table, col in [("signals","signal_id"),("lead_scores","company_id"),("companies","company_id")]:
        try:
            client.table(table).delete().neq(col,"00000000-0000-0000-0000-000000000000").execute()
            print(f"[DB] ✓ {table} cleared")
        except Exception as e:
            print(f"[DB] {table} clear skipped: {e}")

def build_signal(signal_type, summary, source_url, location, confidence, data_source, why_cre="", urgency="MEDIUM"):
    """All signals MUST go through here — guarantees correct keys for insert_signal."""
    return {
        "signal_type": str(signal_type or "OFFICE").upper(),
        "summary":     str(summary or "")[:500],
        "source_url":  str(source_url or ""),
        "location":    str(location or "India"),
        "confidence":  min(max(int(confidence or 0), 0), 100),
        "data_source": str(data_source or "RSS"),
        "why_cre":     str(why_cre or ""),
        "urgency":     str(urgency or "MEDIUM"),
        "space_type":  None,
    }

def save_signal(client, company_name, signal, company_signals):
    if not company_name or len(company_name.strip()) < 3:
        return False
    if any(j in company_name.lower() for j in JUNK_NAMES):
        return False
    try:
        cid = upsert_company(client, company_name.strip())
        insert_signal(client, cid, signal)
        print(f"[DB] ✓ {company_name[:35]:<35} | {signal['signal_type']:<8} | conf:{signal['confidence']:>3} | {signal['urgency']:<6} | {signal['location'][:20]}")
        company_signals.setdefault(cid, []).append(signal)
        return True
    except Exception as e:
        print(f"[DB] ERROR {company_name[:30]}: {e}")
        return False

def run_pipeline():
    client = get_client()
    try:
        client.table("companies").select("company_id").limit(1).execute()
        print("[Pipeline] Supabase OK")
    except Exception as e:
        print(f"[Pipeline] SUPABASE FAILED: {e}")
        return

    refresh_database(client)

    all_articles = []
    source_counts = {}

    for name, fn in [
        ("BSE",      lambda: BSECrawler().crawl(days_back=2)),
        ("NSE",      lambda: FilingPDFCrawler().crawl_nse_announcements(days_back=3)),
        ("IR_PAGES", lambda: FilingPDFCrawler().crawl_ir_pages()),
        ("LINKEDIN", lambda: LinkedInSignalCrawler().crawl(min_jobs=15)),
    ]:
        print(f"\n[Pipeline] === {name} ===")
        try:
            arts = fn()
            all_articles.extend(arts)
            source_counts[name] = len(arts)
        except Exception as e:
            print(f"[Pipeline] {name} error: {e}")
            source_counts[name] = 0

    print("\n[Pipeline] === RSS ===")
    try:
        rss_raw = RSSCrawler().crawl(SOURCES["rss_feeds"])
        for a in rss_raw:
            a["text"] = a.get("summary","") + " " + a.get("title","")
        all_articles.extend(rss_raw)
        source_counts["RSS"] = len(rss_raw)
    except Exception as e:
        print(f"[Pipeline] RSS error: {e}")
        source_counts["RSS"] = 0

    print("\n[Pipeline] === Google News RSS ===")
    try:
        gnews_feeds  = [f["url"]   for f in SOURCES.get("google_news_rss", [])]
        gnews_labels = {f["url"]: f["label"] for f in SOURCES.get("google_news_rss", [])}
        gnews_raw    = RSSCrawler().crawl(gnews_feeds)
        for a in gnews_raw:
            a["text"]   = a.get("summary","") + " " + a.get("title","")
            feed_url    = a.get("source","")
            a["source"] = f"GNEWS_{gnews_labels.get(feed_url,'GOOGLE').upper().replace(' ','_')}"
        all_articles.extend(gnews_raw)
        source_counts["GNEWS"] = len(gnews_raw)
        print(f"[Pipeline] Google News: {len(gnews_raw)} articles from {len(gnews_feeds)} queries")
    except Exception as e:
        print(f"[Pipeline] Google News RSS error: {e}")
        source_counts["GNEWS"] = 0

    print("\n[Pipeline] === News Portals ===")
    try:
        news = NewsCrawler().crawl(SOURCES.get("news_portals",[]))
        all_articles.extend(news)
        source_counts["NEWS"] = len(news)
    except Exception as e:
        print(f"[Pipeline] News error: {e}")
        source_counts["NEWS"] = 0

    all_articles = deduplicate(all_articles, key="url")
    print(f"\n[Pipeline] Unique articles: {len(all_articles)}")
    print(f"[Pipeline] By source: {source_counts}\n")

    stats = {"seen":0, "intent":0, "direct":0, "rejected":0, "saved":0}
    company_signals = {}

    for article in all_articles:
        stats["seen"] += 1
        title   = article.get("title","")
        text    = clean_text(article.get("text",""))
        combined = (title + " " + text).strip()
        if len(combined) < 30:
            stats["rejected"] += 1
            continue

        source     = article.get("source","RSS")
        is_primary = any(s in source for s in ["BSE","NSE","IR_","LINKEDIN"])
        entities   = extract_entities(combined)
        location   = entities["locations"][0] if entities["locations"] else article.get("location_hint","India")
        companies  = entities["companies"] or ([article["company_hint"]] if article.get("company_hint") else [])

        signal = None

        # ── STEP 1: Intent layer — ALWAYS runs first, no filter gate ─────────
        # Catches: funding rounds, GCC setups, foreign co India entry, unicorns
        intent = analyze_cre_intent(title, combined, location)
        if intent:
            why = intent.get("why_cre","")
            signal = build_signal(
                intent["signal_type"],
                why + " | " + extract_summary(combined,[]),
                article.get("url",""), location,
                intent["confidence_score"], source,
                why_cre=why, urgency=intent.get("urgency","MEDIUM")
            )
            stats["intent"] += 1
            print(f"[Intent] ★ {title[:70]}")
            print(f"         → {why[:90]}")

        # ── STEP 2: Primary source (BSE/NSE already CRE-vetted) ──────────────
        if signal is None and is_primary:
            sig_type = article.get("signal_type_hint") or get_signal_type(title, text)
            signal = build_signal(sig_type, extract_summary(combined,[]),
                                  article.get("url",""), location, 70, source)
            stats["direct"] += 1

        # ── STEP 3: CRE keyword filter + classifier ───────────────────────────
        if signal is None:
            relevant, confidence, reason = is_cre_relevant(title, text)
            if relevant:
                classified = classify_signal(article)
                if classified:
                    signal = build_signal(
                        classified.get("signal_type","OFFICE"),
                        extract_summary(combined, classified.get("matched_phrases",[])),
                        article.get("url",""), location,
                        classified.get("confidence_score", int(confidence*100)), source
                    )
                    stats["direct"] += 1
            if signal is None:
                stats["rejected"] += 1
                print(f"[Filter] REJECTED ({reason}): {title[:70]}")
                continue

        # ── SAVE ──────────────────────────────────────────────────────────────
        for co in (companies or ["Unknown Company"])[:2]:
            if save_signal(client, co, signal, company_signals):
                stats["saved"] += 1

    print(f"\n[Pipeline] Scoring {len(company_signals)} companies...")
    for cid, sigs in company_signals.items():
        try:
            score_data = compute_lead_score(sigs)
            score_data.pop("breakdown", None)
            score_data.pop("top_signal", None)
            upsert_lead_score(client, cid, score_data)
        except Exception as e:
            print(f"[Scoring] {cid}: {e}")

    total_captured = stats["intent"] + stats["direct"]
    print(f"""
[Pipeline] ════ COMPLETE ════
  Articles seen    : {stats['seen']}
  Intent leads     : {stats['intent']}   (funding/GCC/foreign entry)
  Direct CRE       : {stats['direct']}   (explicit space keywords)
  Rejected         : {stats['rejected']}
  Saved to DB      : {stats['saved']}
  Companies scored : {len(company_signals)}
  Capture rate     : {round(total_captured/max(stats['seen'],1)*100,1)}%
""")

if __name__ == "__main__":
    run_pipeline()
