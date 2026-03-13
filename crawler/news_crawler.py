from bs4 import BeautifulSoup
from crawler.base_crawler import BaseCrawler

class NewsCrawler(BaseCrawler):
    def __init__(self):
        super().__init__(delay=2)

    def crawl(self, urls: list) -> list:
        results = []
        for url in urls:
            html = self.fetch(url)
            if not html:
                continue
            soup = BeautifulSoup(html, "lxml")
            paragraphs = soup.find_all("p")
            text = " ".join(p.get_text(strip=True) for p in paragraphs)
            links = [a["href"] for a in soup.find_all("a", href=True)
                     if "mumbai" in a["href"].lower() or "navi-mumbai" in a["href"].lower()]
            results.append({"url": url, "text": text, "links": links})
        return results
