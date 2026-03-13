import feedparser
from datetime import datetime
from crawler.base_crawler import BaseCrawler

class RSSCrawler(BaseCrawler):
    def __init__(self):
        super().__init__(delay=1)

    def crawl(self, feed_urls: list) -> list:
        articles = []
        for url in feed_urls:
            try:
                feed = feedparser.parse(url)
                for entry in feed.entries:
                    article = {
                        "title": entry.get("title", ""),
                        "summary": entry.get("summary", ""),
                        "url": entry.get("link", ""),
                        "published": entry.get("published", datetime.utcnow().isoformat()),
                        "source": url
                    }
                    articles.append(article)
                print(f"[RSSCrawler] {len(feed.entries)} entries from {url}")
            except Exception as e:
                print(f"[RSSCrawler] Failed {url}: {e}")
        return articles
