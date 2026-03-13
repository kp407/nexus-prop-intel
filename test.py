from crawler.rss_crawler import RSSCrawler
import json

with open('config/sources.json') as f:
    sources = json.load(f)

rss = RSSCrawler()
articles = rss.crawl(sources['rss_feeds'][:2])

for a in articles[:5]:
    print("TITLE:", a.get('title', '')[:120])
    print("TEXT:", (a.get('summary', '') or '')[:150])
    print("---")
