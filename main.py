"""
NEXUS PROP INTEL — Main Orchestrator v2
========================================
Pipeline order (fastest signal first):
  1. BSE/NSE regulatory filings  ← earliest possible signal
  2. MCA office change filings   ← before any press release
  3. RERA new project regs       ← supply-side signal
  4. Municipal meeting minutes   ← planning-stage signal
  5. Company IR presentations    ← forward guidance
  6. RSS / news portals          ← mainstream (filtered hard)
  7. LinkedIn hiring surge       ← demand proxy signal

All articles pass through CRE relevance filter before DB write.
"""

import json
from crawler.rss_crawler import RSSCrawler
from crawler.news_crawler import NewsCrawler
from crawler.primary_sources import (
    BSECrawler,
    MCACrawler,
    RERAcrawler,
    FilingPDFCrawler,
    LinkedInSignalCrawler,
    MeetingMinutesCrawler,
)
from nlp.cre_filter import is_cre_relevant, get_signal_type
from nlp.entity_extractor import extract_entities
from nlp.signal_classifier import extract_summary
from nlp.text_cleaner import clean_text, deduplicate
from scoring.lead_scorer import compute_lead_score
from database.db_client import (
    get_client, upsert_company, insert_signal, upsert_lead_score
)

with open("config/sources.json") as f:
    SOURCES = json.load(f)


def run_pipeline():
    client = get_client()
    print("[Pipeline] Supabase client created OK")

    try:
        test = client.table("companies").select("company_id").limit(1).execute()
        print(f"[Pipeline] Supabase OK: {test.data}")
    except Exception as e:
        print(f"[Pipeline] SUPABASE FAILED: {e}")
        return

    all_articles = []
    source_counts = {}

    # ── 1. BSE filings ───────────────────────────────────────────────────────
    print("\n[Pipeline] === BSE Filings ===")
    try:
        bse = BSECrawler()
        bse_articles = bse.crawl(days_back=2)
        all_articles.extend(bse_articles)
        source_counts["BSE"] = len(bse_articles)
    except Exception as e:
        print(f"[Pipeline] BSE crawler error: {e}")

    # ── 2. NSE filings + IR presentations ────────────────────────────────────
    print("\n[Pipeline] === NSE Filings + IR Pages ===")
    try:
        filing_crawler = FilingPDFCrawler()
        nse_articles = filing_crawler.crawl_nse_announcements(days_back=3)
        ir_articles = filing_crawler.crawl_ir_pages()
        all_articles.extend(nse_articles + ir_articles)
        source_counts["NSE"] = len(nse_articles)
        source_counts["IR_PAGES"] = len(ir_articles)
    except Exception as e:
        print(f"[Pipeline] NSE/IR crawler error: {e}")

    # ── 3. MCA filings ───────────────────────────────────────────────────────
    print("\n[Pipeline] === MCA Filings ===")
    try:
        mca = MCACrawler()
        mca_articles = mca.crawl_recent_office_changes() + mca.crawl_roc_filings()
        all_articles.extend(mca_articles)
        source_counts["MCA"] = len(mca_articles)
    except Exception as e:
        print(f"[Pipeline] MCA crawler error: {e}")

    # ── 4. RERA registrations ─────────────────────────────────────────────────
    print("\n[Pipeline] === RERA Registrations ===")
    try:
        rera = RERAcrawler()
        rera_articles = rera.crawl_all()
        all_articles.extend(rera_articles)
        source_counts["RERA"] = len(rera_articles)
    except Exception as e:
        print(f"[Pipeline] RERA crawler error: {e}")

    # ── 5. Municipal meeting minutes ──────────────────────────────────────────
    print("\n[Pipeline] === Meeting Minutes ===")
    try:
        minutes = MeetingMinutesCrawler()
        minutes_articles = minutes.crawl()
        all_articles.extend(minutes_articles)
        source_counts["MINUTES"] = len(minutes_articles)
    except Exception as e:
        print(f"[Pipeline] Minutes crawler error: {e}")

    # ── 6. RSS feeds (with hard CRE filter) ───────────────────────────────────
    print("\n[Pipeline] === RSS Feeds ===")
    try:
        rss = RSSCrawler()
        rss_raw = rss.crawl(SOURCES["rss_feeds"])
        for a in rss_raw:
            a["text"] = a.get("summary", "") + " " + a.get("title", "")
        all_articles.extend(rss_raw)
        source_counts["RSS"] = len(rss_raw)
    except Exception as e:
        print(f"[Pipeline] RSS crawler error: {e}")

    # ── 7. News portals ───────────────────────────────────────────────────────
    print("\n[Pipeline] === News Portals ===")
    try:
        news = NewsCrawler()
        news_articles = news.crawl(SOURCES.get("news_portals", []))
        all_articles.extend(news_articles)
        source_counts["NEWS"] = len(news_articles)
    except Exception as e:
        print(f"[Pipeline] News crawler error: {e}")

    # ── 8. LinkedIn hiring surge ──────────────────────────────────────────────
    print("\n[Pipeline] === LinkedIn Job Surge ===")
    try:
        linkedin = LinkedInSignalCrawler()
        li_articles = linkedin.crawl(min_jobs=15)
        all_articles.extend(li_articles)
        source_counts["LINKEDIN"] = len(li_articles)
    except Exception as e:
        print(f"[Pipeline] LinkedIn crawler error: {e}")

    # ── Dedup ──────────────────────────────────────────────────────────────────
    all_articles = deduplicate(all_articles, key="url")
    print(f"\n[Pipeline] Total unique articles: {len(all_articles)}")
    print(f"[Pipeline] By source: {source_counts}")

    # ── CRE Filter + DB Write ─────────────────────────────────────────────────
    stats = {"seen": 0, "passed_filter": 0, "saved": 0, "rejected": 0}
    company_signals = {}

    for article in all_articles:
        stats["seen"] += 1
        title = article.get("title", "")
        raw_text = article.get("text", "")
        text = clean_text(raw_text)

        if len(text) < 30:
            stats["rejected"] += 1
            continue

        # ── CRE RELEVANCE GATE ────────────────────────────────────────────────
        # Skip filter for primary sources (BSE, MCA, RERA, Minutes)
        # they're already pre-filtered
        source = article.get("source", "")
        is_primary = any(s in source for s in ["BSE", "NSE", "MCA", "RERA", "MINUTES", "IR_"])

        if not is_primary:
            relevant, confidence, reason = is_cre_relevant(title, text)
            if not relevant:
                stats["rejected"] += 1
                print(f"[Filter] REJECTED ({reason}): {title[:60]}")
                continue
        else:
            confidence = 0.7  # Primary sources get baseline confidence

        stats["passed_filter"] += 1

        # ── Signal Type ────────────────────────────────────────────────────────
        signal_type = article.get("signal_type_hint") or get_signal_type(title, text)

        # ── Entities ───────────────────────────────────────────────────────────
        entities = extract_entities(text)

        # Use hint if NLP didn't find company
        companies = entities["companies"]
        if not companies and article.get("company_hint"):
            companies = [article["company_hint"]]
        if not companies:
            companies = ["Unknown Company"]

        location = entities["locations"][0] if entities["locations"] else \
                   article.get("location_hint", "India")

        # ── Build signal ───────────────────────────────────────────────────────
        signal = {
            "signal_type": signal_type,
            "summary": extract_summary(text, []) if hasattr(extract_summary, '__call__') else text[:300],
            "source_url": article.get("url", ""),
            "location": location,
            "confidence_score": int(confidence * 100),
            "data_source": source,
        }

        # ── Save to DB ─────────────────────────────────────────────────────────
        for company_name in companies[:2]:
            if len(company_name) < 3:
                continue
            try:
                company_id = upsert_company(client, company_name)
                signal_id = insert_signal(client, company_id, signal)
                print(f"[DB] ✓ {company_name} | {signal_type} | {location} | conf:{signal['confidence_score']}")
                stats["saved"] += 1

                if company_id not in company_signals:
                    company_signals[company_id] = []
                company_signals[company_id].append(signal)
            except Exception as e:
                print(f"[DB] ERROR {company_name}: {e}")

    # ── Lead Scoring ───────────────────────────────────────────────────────────
    print(f"\n[Pipeline] Scoring {len(company_signals)} companies...")
    for company_id, signals in company_signals.items():
        try:
            score_data = compute_lead_score(signals)
            upsert_lead_score(client, company_id, score_data)
        except Exception as e:
            print(f"[Pipeline] Score error {company_id}: {e}")

    # ── Summary ────────────────────────────────────────────────────────────────
    print(f"""
[Pipeline] ════ COMPLETE ════
  Articles seen    : {stats['seen']}
  Passed CRE filter: {stats['passed_filter']}
  Rejected (noise) : {stats['rejected']}
  Saved to DB      : {stats['saved']}
  Companies scored : {len(company_signals)}
  Filter efficiency: {round(stats['passed_filter']/max(stats['seen'],1)*100, 1)}%
""")


if __name__ == "__main__":
    run_pipeline()
