from bs4 import BeautifulSoup
from crawler.base_crawler import BaseCrawler

class JobBoardCrawler(BaseCrawler):
    def __init__(self):
        super().__init__(delay=3)

    def crawl_naukri(self, keyword="mumbai office expansion") -> list:
        url = f"https://www.naukri.com/{keyword.replace(' ', '-')}-jobs"
        html = self.fetch(url)
        if not html:
            return []
        soup = BeautifulSoup(html, "lxml")
        jobs = []
        for card in soup.select(".jobTuple"):
            title = card.select_one(".title")
            company = card.select_one(".companyName")
            location = card.select_one(".location")
            if title and company:
                jobs.append({
                    "title": title.get_text(strip=True),
                    "company": company.get_text(strip=True),
                    "location": location.get_text(strip=True) if location else "Mumbai",
                    "url": url
                })
        return jobs
