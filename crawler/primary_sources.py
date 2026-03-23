"""
Primary Sources Crawler — FINAL v2 (no PDF downloads, no hangs)
================================================================
PDFs removed entirely from NSE/IR — they cause multi-minute hangs on
nsearchives.nseindia.com. Headlines + BSE API text is sufficient signal.
Max runtime: ~2 minutes total.
"""

import time
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from crawler.base_crawler import BaseCrawler

REQUEST_TIMEOUT = (8, 8)   # (connect_timeout, read_timeout) — both hard-capped


def safe_get(session, url, **kwargs):
    """Always times out, never raises."""
    try:
        resp = session.get(url, timeout=REQUEST_TIMEOUT, **kwargs)
        resp.raise_for_status()
        return resp
    except Exception as e:
        print(f"[BaseCrawler] Error fetching {url[:80]}: {e}")
        return None


# ── BSE CRAWLER ───────────────────────────────────────────────────────────────

class BSECrawler(BaseCrawler):
    API_URL      = "https://api.bseindia.com/BseIndiaAPI/api/AnnGetData/w"
    ANNOUNCE_URL = "https://www.bseindia.com/corporates/ann.html"

    CRE_KEYWORDS = [
        "new office","office space","office premises","office campus","sq ft","sqft",
        "square feet","lease","leased","leasing","new facility","new campus",
        "new headquarters","new hq","relocation","relocated","new premises",
        "commercial property","real estate","additional space","office expansion",
    ]
    NOISE_KEYWORDS = [
        "appointment","resignation","cfo","ceo","coo","esop","allotment","dividend",
        "agm","egm","auditor","book closure","record date","financial results",
        "quarterly results","investor meet","analyst meet","credit rating",
        "shareholding","compliance officer","intimation","outcome of board",
    ]

    def __init__(self):
        super().__init__(delay=1)
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer":    "https://www.bseindia.com/",
        })

    def crawl(self, days_back: int = 2) -> list:
        articles = []
        try:
            params = {
                "strCat":      "-1",
                "strPrevDate": (datetime.now() - timedelta(days=days_back)).strftime("%Y%m%d"),
                "strScrip":    "",
                "strSearch":   "P",
                "strToDate":   datetime.now().strftime("%Y%m%d"),
                "strType":     "C",
                "subcategory": "-1",
            }
            resp = safe_get(self.session, self.API_URL, params=params)
            if not resp:
                return []

            for item in resp.json().get("Table", []):
                headline = item.get("HEADLINE", "")
                company  = item.get("SLONGNAME", "")
                pdf_name = item.get("ATTACHMENTNAME", "")
                ann_date = item.get("NEWS_DT", "")
                hl_lower = headline.lower()

                if any(nk in hl_lower for nk in self.NOISE_KEYWORDS):
                    continue
                if not any(kw in hl_lower for kw in self.CRE_KEYWORDS):
                    continue

                # No PDF download — headline text is enough for signal classification
                pdf_url = f"https://www.bseindia.com/xml-data/corpfiling/AttachLive/{pdf_name}" if pdf_name else self.ANNOUNCE_URL
                articles.append({
                    "title":            f"{company}: {headline}",
                    "text":             headline,
                    "url":              pdf_url,
                    "source":           "BSE_FILING",
                    "company_hint":     company,
                    "published":        ann_date,
                    "signal_type_hint": "FILING",
                })
                print(f"[BSECrawler] ✓ {company[:40]} — {headline[:50]}")

        except Exception as e:
            print(f"[BSECrawler] Error: {e}")

        print(f"[BSECrawler] Found {len(articles)} CRE filings")
        return articles


# ── NSE CRAWLER (headlines only, no PDF) ──────────────────────────────────────

class FilingPDFCrawler(BaseCrawler):
    NSE_API = "https://www.nseindia.com/api/corporate-announcements"

    CRE_KEYWORDS = [
        "sq ft","sqft","office","campus","facility","lease",
        "expansion","relocation","headquarters","new premises","capex",
    ]

    def __init__(self):
        super().__init__(delay=2)
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept":     "application/json, text/plain, */*",
            "Referer":    "https://www.nseindia.com/",
        })

    def crawl_nse_announcements(self, days_back: int = 3) -> list:
        articles = []
        try:
            safe_get(self.session, "https://www.nseindia.com", timeout=(8, 8))
            time.sleep(2)
            params = {
                "index":     "equities",
                "from_date": (datetime.now() - timedelta(days=days_back)).strftime("%d-%m-%Y"),
                "to_date":   datetime.now().strftime("%d-%m-%Y"),
            }
            resp = safe_get(self.session, self.NSE_API, params=params)
            if not resp:
                return []

            data  = resp.json()
            items = data if isinstance(data, list) else data.get("data", [])
            for item in (items or []):
                if not isinstance(item, dict):
                    continue
                subject = item.get("subject", item.get("desc", ""))
                company = item.get("company", item.get("symbol", ""))
                if not any(kw in subject.lower() for kw in self.CRE_KEYWORDS):
                    continue
                # No PDF download — subject line only
                articles.append({
                    "title":            f"{company}: {subject}",
                    "text":             subject,
                    "url":              self.NSE_API,
                    "source":           "NSE_FILING",
                    "company_hint":     company,
                    "signal_type_hint": "FILING",
                    "published":        item.get("an_dt", datetime.now().isoformat()),
                })
                print(f"[FilingPDFCrawler] NSE: {company[:35]} — {subject[:50]}")

        except Exception as e:
            print(f"[FilingPDFCrawler] NSE error: {e}")
        return articles

    def crawl_ir_pages(self) -> list:
        # Removed — all major IT IR pages return 403/404, PDF downloads hang
        return []


# ── LINKEDIN JOB SURGE CRAWLER ────────────────────────────────────────────────

class LinkedInSignalCrawler(BaseCrawler):
    SEARCH_URL = "https://www.linkedin.com/jobs/search/"

    CITY_CODES = {
        "Bengaluru": "bengaluru-karnataka-india",
        "Mumbai":    "mumbai-maharashtra-india",
        "Hyderabad": "hyderabad-telangana-india",
        "Pune":      "pune-maharashtra-india",
    }

    def __init__(self):
        super().__init__(delay=2)
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
        })

    def crawl(self, min_jobs: int = 15) -> list:
        articles = []
        company_city_count = {}

        for city_name, city_slug in self.CITY_CODES.items():
            url  = f"{self.SEARCH_URL}?location={city_slug}&f_TPR=r86400&position=1&pageNum=0"
            resp = safe_get(self.session, url)
            if not resp:
                continue
            soup = BeautifulSoup(resp.text, "lxml")
            for card in soup.select("div.base-card"):
                el = card.select_one(".base-search-card__subtitle")
                if el:
                    key = (el.get_text(strip=True), city_name)
                    company_city_count[key] = company_city_count.get(key, 0) + 1

        for (company, city), count in company_city_count.items():
            if count >= min_jobs:
                articles.append({
                    "title":            f"{company} hiring surge in {city} ({count}+ roles)",
                    "text":             f"{company} is actively hiring {count}+ positions in {city}. Large-scale hiring signals upcoming office expansion.",
                    "url":              f"https://www.linkedin.com/jobs/search/?keywords={company}&location={city}",
                    "source":           "LINKEDIN_JOBS",
                    "company_hint":     company,
                    "location_hint":    city,
                    "signal_type_hint": "HIRING",
                    "published":        datetime.now().isoformat(),
                })
                print(f"[LinkedInCrawler] Surge: {company} in {city} — {count} jobs")

        return articles
