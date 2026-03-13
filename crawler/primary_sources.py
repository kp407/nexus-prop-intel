"""
Primary Sources Crawler — FINAL (timeout-hardened)
All network calls have explicit timeouts. Dead sources removed.
Max total runtime: ~3 minutes for all primary sources combined.
"""

import re, time
import pdfplumber
from io import BytesIO
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from crawler.base_crawler import BaseCrawler

REQUEST_TIMEOUT = 12
PDF_TIMEOUT = 15
MAX_PDF_PAGES = 5


def safe_get(session, url, timeout=REQUEST_TIMEOUT, **kwargs):
    """Always has timeout, never raises — returns None on any error."""
    try:
        resp = session.get(url, timeout=timeout, **kwargs)
        resp.raise_for_status()
        return resp
    except Exception as e:
        print(f"[BaseCrawler] Error fetching {url[:80]}: {e}")
        return None


def extract_pdf_text(session, pdf_url, max_pages=MAX_PDF_PAGES):
    try:
        resp = session.get(pdf_url, timeout=PDF_TIMEOUT, verify=False)
        resp.raise_for_status()
        with pdfplumber.open(BytesIO(resp.content)) as pdf:
            return "\n".join(page.extract_text() or "" for page in pdf.pages[:max_pages])
    except Exception as e:
        print(f"[PDF] Error {pdf_url[:60]}: {e}")
        return ""


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

                pdf_url  = f"https://www.bseindia.com/xml-data/corpfiling/AttachLive/{pdf_name}" if pdf_name else None
                pdf_text = extract_pdf_text(self.session, pdf_url) if pdf_url else ""

                articles.append({
                    "title":            f"{company}: {headline}",
                    "text":             pdf_text[:3000] if pdf_text else headline,
                    "url":              pdf_url or self.ANNOUNCE_URL,
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


# ── NSE + IR PAGE CRAWLER ─────────────────────────────────────────────────────

class FilingPDFCrawler(BaseCrawler):
    NSE_API = "https://www.nseindia.com/api/corporate-announcements"

    # Only IR pages confirmed to NOT return 403/404
    IR_PAGES = [
        {"company": "Tech Mahindra", "url": "https://www.techmahindra.com/en-in/investors/"},
        {"company": "LTIMindtree",   "url": "https://www.ltimindtree.com/investors/"},
        {"company": "Mphasis",       "url": "https://www.mphasis.com/investors.html"},
        {"company": "Persistent",    "url": "https://www.persistent.com/investors/"},
        {"company": "Coforge",       "url": "https://www.coforge.com/investors"},
    ]

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
            safe_get(self.session, "https://www.nseindia.com", timeout=10)
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
                subject    = item.get("subject", item.get("desc", ""))
                company    = item.get("company", item.get("symbol", ""))
                attachment = item.get("attchmntFile", item.get("filename", ""))

                if not any(kw in subject.lower() for kw in self.CRE_KEYWORDS):
                    continue

                pdf_url  = f"https://nsearchives.nseindia.com/corporate/{attachment}" if attachment else None
                pdf_text = extract_pdf_text(self.session, pdf_url) if pdf_url else ""

                articles.append({
                    "title":            f"{company}: {subject}",
                    "text":             pdf_text[:4000] if pdf_text else subject,
                    "url":              pdf_url or self.NSE_API,
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
        articles = []
        for co in self.IR_PAGES:
            resp = safe_get(self.session, co["url"])
            if not resp:
                continue
            soup = BeautifulSoup(resp.text, "lxml")
            for link in soup.find_all("a", href=True):
                href     = link["href"]
                link_txt = link.get_text(strip=True).lower()
                if not href.endswith(".pdf"):
                    continue
                if not any(kw in link_txt for kw in ["investor","presentation","result","annual","quarterly","agm"]):
                    continue

                full_url = href if href.startswith("http") else co["url"].rstrip("/") + "/" + href.lstrip("/")
                pdf_text = extract_pdf_text(self.session, full_url)
                if not pdf_text:
                    continue
                if sum(1 for kw in self.CRE_KEYWORDS if kw in pdf_text.lower()) < 2:
                    continue

                articles.append({
                    "title":            f"{co['company']} — {link.get_text(strip=True)[:80]}",
                    "text":             pdf_text[:5000],
                    "url":              full_url,
                    "source":           "IR_PRESENTATION",
                    "company_hint":     co["company"],
                    "signal_type_hint": "EXPAND",
                    "published":        datetime.now().isoformat(),
                })
                print(f"[FilingPDFCrawler] IR PDF: {co['company']}")
        return articles


# ── LINKEDIN JOB SURGE CRAWLER ────────────────────────────────────────────────

class LinkedInSignalCrawler(BaseCrawler):
    SEARCH_URL = "https://www.linkedin.com/jobs/search/"

    # Reduced to top 4 cities — each fetch takes 4-8s, 7 cities risked 56s hang
    CITY_CODES = {
        "Bengaluru": "bengaluru-karnataka-india",
        "Mumbai":    "mumbai-maharashtra-india",
        "Hyderabad": "hyderabad-telangana-india",
        "Pune":      "pune-maharashtra-india",
    }

    def __init__(self):
        super().__init__(delay=2)
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        })

    def crawl(self, min_jobs: int = 15) -> list:
        articles = []
        company_city_count = {}

        for city_name, city_slug in self.CITY_CODES.items():
            url  = f"{self.SEARCH_URL}?location={city_slug}&f_TPR=r86400&position=1&pageNum=0"
            resp = safe_get(self.session, url, timeout=10)
            if not resp:
                continue
            soup = BeautifulSoup(resp.text, "lxml")
            for card in soup.select("div.base-card"):
                el = card.select_one(".base-search-card__subtitle")
                if not el:
                    continue
                company = el.get_text(strip=True)
                key     = (company, city_name)
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


# MeetingMinutesCrawler REMOVED — all sources (BBMP, HMDA, NASSCOM, CII, SEEPZ)
# return 403/404/SSL errors from GitHub Actions IPs. BSE + RSS intent layer
# covers the same signals faster.
