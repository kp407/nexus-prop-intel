"""
NEXUS ASIA PROP INTEL — Main Orchestrator
"""
import json
from crawler.rss_crawler import RSSCrawler
from crawler.news_crawler import NewsCrawler
from nlp.entity_extractor import extract_entities
from nlp.signal_classifier import classify_signal, extract_summary
from nlp.text_cleaner import clean_text, deduplicate
from scoring.lead_scorer import compute_lead_score
from database.db_client import get_client, upsert_company, insert_signal, upsert_lead_score

with open("config/sources.json") as f:
    SOURCES = json.load(f)

def run_pipeline():
    client = get_client()
    all_articles = []

    rss = RSSCrawler()
    rss_articles = rss.crawl(SOURCES["rss_feeds"])
    for a in rss_articles:
        a["text"] = a.get("summary", "") + " " + a.get("title", "")
    all_articles.extend(rss_articles)

    news = NewsCrawler()
    news_articles = news.crawl(SOURCES.get("news_portals", []))
    all_articles.extend(news_articles)

    all_articles = deduplicate(all_articles, key="url")
    print(f"[Pipeline] Processing {len(all_articles)} unique articles")

    company_signals = {}

    for article in all_articles:
        text = clean_text(article.get("text", ""))
        if len(text) < 50:
            continue

        signal = classify_signal(article)
        if not signal:
            continue

        entities = extract_entities(text)

        signal["summary"] = extract_summary(text, signal["matched_phrases"])
        signal["source_url"] = article.get("url", "")
        signal["location"] = (
            entities["locations"][0] if entities["locations"] else "India"
        )

        companies = entities["companies"] if entities["companies"] else ["Unknown Company"]

        for company_name in companies[:3]:
            if len(company_name) < 3:
                continue
            try:
                company_id = upsert_company(client, company_name)
                signal_id = insert_signal(client, company_id, signal)
                if company_id not in company_signals:
                    company_signals[company_id] = []
                company_signals[company_id].append(signal)
            except Exception as e:
                print(f"[Pipeline] Error saving {company_name}: {e}")
                continue

    for company_id, signals in company_signals.items():
        try:
            score_data = compute_lead_score(signals)
            upsert_lead_score(client, company_id, score_data)
        except Exception as e:
            print(f"[Pipeline] Error scoring {company_id}: {e}")

    print(f"[Pipeline] Done. Processed signals for {len(company_signals)} companies.")

if __name__ == "__main__":
    run_pipeline()
