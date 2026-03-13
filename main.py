"""
NEXUS PROP INTEL — Main Orchestrator v3
"""
import json
from crawler.rss_crawler import RSSCrawler
from crawler.news_crawler import NewsCrawler
from crawler.primary_sources import (
    BSECrawler, MCACrawler, RERAcrawler,
    FilingPDFCrawler, LinkedInSignalCrawler, MeetingMinutesCrawler,
)
from nlp.cre_filter import is_cre_relevant, get_signal_type
from nlp.cre_intent import analyze_cre_intent
from nlp.entity_extractor import extract_entities
from nlp.signal_classifier import classify_signal, extract_summary
from nlp.text_cleaner import clean_text, deduplicate
from scoring.lead_scorer import compute_lead_score
from database.db_client import get_client, upsert_company, insert_signal, upsert_lead_score

with open("config/sources.json") as f:
    SOURCES = json.load(f)


def refresh_database(client):
    print("[DB] Refreshing database...")
    for table in ["signals", "lead_scores", "companies"]:
        try:
            client.table(table).delete().neq(
                "signal_id" if table == "signals" else
                "company_id",
                "00000000-0000-0000-0000-000000000000"
            ).execute()
            print(f"[DB] ✓ {table} cleared")
        except Exception as e:
            print(f"[DB] Could not clear {table}: {e}")
    print("[DB] Refresh complete\n")


def build_signal(signal_type, summary, source_url, location,
                 confidence, data_source, why_cre="", urgency="MEDIUM"):
    return {
        "signal_type": signal_type,
        "summary": (summary or "")[:500],
        "source_url": source_url or "",
        "location": location or "India",
        "confidence": min(max(int(confidence), 0), 100),
        "data_source": data_source or "RSS",
        "why_cre": why_cre or "",
        "urgency": urgency or "MEDIUM",
    }


def save_signal(client, company_name, signal, company_signals):
    if not company_name or len(company_name.strip()) < 3:
        return False
    junk = ["unknown", "href=", "&#", "cin:", "dalal street",
            "5th floor", "assemblies limited", "limited cin"]
    if any(j in company_name.lower() for j in junk):
        return False
    try:
        company_id = upsert_company(client, company_name.strip())
        insert_signal(client, company_id, signal)
        print(f"[DB] ✓ {company_name[:35]} | {signal['signal_type']} | "
              f"conf:{signal['confidence']} | {signal.get('urgency','')}")
        company_signals.setdefault(company_id, []).append(signal)
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
        ("MCA",      lambda: MCACrawler().crawl_recent_office_changes() + MCACrawler().crawl_roc_filings()),
        ("RERA",     lambda: RERAcrawler().crawl_all()),
        ("MINUTES",  lambda: MeetingMinutesCrawler().crawl()),
        ("LINKEDIN", lambda: LinkedInSignalCrawler().crawl(min_jobs=15)),
    ]:
        print(f"\n[Pipeline] === {name} ===")
        try:
            articles = fn()
            all_articles.extend(articles)
            source_counts[name] = len(articles)
        except Exception as e:
            print(f"[Pipeline] {name} error: {e}")
            source_counts[name] = 0

    print("\n[Pipeline] === RSS ===")
    try:
        rss_raw = RSSCrawler().crawl(SOURCES["rss_feeds"])
        for a in rss_raw:
            a["text"] = a.get("summary", "") + " " + a.get("title", "")
        all_articles.extend(rss_raw)
        source_counts["RSS"] = len(rss_raw)
    except Exception as e:
        print(f"[Pipeline] RSS error: {e}")

    print("\n[Pipeline] === News Portals ===")
    try:
        news_articles = NewsCrawler().crawl(SOURCES.get("news_portals", []))
        all_articles.extend(news_articles)
        source_counts["NEWS"] = len(news_articles)
    except Exception as e:
        print(f"[Pipeline] News error: {e}")

    all_articles = deduplicate(all_articles, key="url")
    print(f"\n[Pipeline] Unique articles: {len(all_articles)}")
    print(f"[Pipeline] Sources: {source_counts}\n")

    stats = {"seen": 0, "cre_direct": 0, "cre_intent": 0, "rejected": 0, "saved": 0}
    company_signals = {}

    for article in all_articles:
        stats["seen"] += 1
        title = article.get("title", "")
        text = clean_text(article.get("text", ""))
        combined = title + " " + text

        if len(combined.strip()) < 30:
            stats["rejected"] += 1
            continue

        source = article.get("source", "RSS")
        is_primary = any(s in source for s in ["BSE","NSE","MCA","RERA","MINUTES","IR_"])
        entities = extract_entities(combined)
        location = (entities["locations"][0] if entities["locations"]
                    else article.get("location_hint", "India"))
        companies = entities["companies"] or ([article["company_hint"]]
                    if article.get("company_hint") else [])

        signal = None

        # PATH 1: Primary source (already pre-filtered)
        if is_primary:
            sig_type = article.get("signal_type_hint") or get_signal_type(title, text)
            signal = build_signal(sig_type, extract_summary(combined, []),
                                  article.get("url",""), location, 70, source)
            stats["cre_direct"] += 1

        else:
            # PATH 2: Direct CRE keyword match
            relevant, confidence, _ = is_cre_relevant(title, text)
            if relevant:
                classified = classify_signal(article)
                if classified:
                    signal = build_signal(
                        classified.get("signal_type", "OFFICE"),
                        extract_summary(combined, classified.get("matched_phrases", [])),
                        article.get("url",""), location,
                        classified.get("confidence_score", int(confidence * 100)),
                        source
                    )
                    stats["cre_direct"] += 1

            # PATH 3: Intent layer — funding, foreign entry, GCC, unicorn
            if signal is None:
                intent = analyze_cre_intent(title, combined, location)
                if intent:
                    why = intent.get("why_cre", "")
                    urgency = intent.get("urgency", "MEDIUM")
                    signal = build_signal(
                        intent["signal_type"],
                        f"{why} | {extract_summary(combined, [])}",
                        article.get("url",""), location,
                        intent["confidence_score"], source,
                        why_cre=why, urgency=urgency
                    )
                    stats["cre_intent"] += 1
                    print(f"[Intent] ★ {title[:65]}")
                    print(f"         → {why}")

        if signal is None:
            stats["rejected"] += 1
            continue

        if not companies:
            companies = ["Unknown Company"]

        for co in companies[:2]:
            if save_signal(client, co, signal, company_signals):
                stats["saved"] += 1

    print(f"\n[Pipeline] Scoring {len(company_signals)} companies...")
    for company_id, signals in company_signals.items():
        try:
            upsert_lead_score(client, company_id, compute_lead_score(signals))
        except Exception as e:
            print(f"[Scoring] {company_id}: {e}")

    print(f"""
[Pipeline] ════ COMPLETE ════
  Articles seen      : {stats['seen']}
  Direct CRE signals : {stats['cre_direct']}
  Intent CRE leads   : {stats['cre_intent']}
  Rejected           : {stats['rejected']}
  Saved to DB        : {stats['saved']}
  Companies scored   : {len(company_signals)}
  Capture rate       : {round((stats['cre_direct']+stats['cre_intent'])/max(stats['seen'],1)*100,1)}%
""")


if __name__ == "__main__":
    run_pipeline()
