"""
Primary Sources Crawler
=======================
Crawls regulatory & statutory filings BEFORE IPCs pick them up.

Sources:
- BSE India announcements (XML feed)
- MCA company filings (registered office changes)
- RERA project registrations (state portals)
- Municipal building plan approvals
- Company investor presentations / annual reports (PDF)
- AGM/EGM meeting minutes (PDFs on company websites)
"""

import re
import time
import requests
import feedparser
import pdfplumber
from io import BytesIO
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from crawler.base_crawler import BaseCrawler


# ── BSE CRAWLER ───────────────────────────────────────────────────────────────

class BSECrawler(BaseCrawler):
    """
    Scrapes BSE corporate announcements for office/facility disclosures.
    BSE has a public XML feed for announcements filtered by category.
    """
    BASE = "https://www.bseindia.com"
    ANNOUNCE_URL = "https://www.bseindia.com/corporates/ann.html"
    API_URL = "https://api.bseindia.com/BseIndiaAPI/api/AnnGetData/w"

    # Categories that reveal CRE intent
    CRE_CATEGORIES = ["General", "Press Release", "Updates", "Outcome of Board Meeting"]

    def __init__(self):
        super().__init__(delay=2)
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://www.bseindia.com/",
        })

    def crawl(self, days_back: int = 2) -> list:
        articles = []
        try:
            # BSE announcement feed
            params = {
                "strCat": "-1",
                "strPrevDate": (datetime.now() - timedelta(days=days_back)).strftime("%Y%m%d"),
                "strScrip": "",
                "strSearch": "P",
                "strToDate": datetime.now().strftime("%Y%m%d"),
                "strType": "C",
                "subcategory": "-1",
            }
            resp = self.session.get(self.API_URL, params=params, timeout=20)
            data = resp.json()

            for item in data.get("Table", []):
                headline = item.get("HEADLINE", "")
                company = item.get("SLONGNAME", "")
                scrip = item.get("SCRIP_CD", "")
                ann_date = item.get("NEWS_DT", "")
                pdf_name = item.get("ATTACHMENTNAME", "")

                # Quick CRE keyword filter on headline
                cre_keywords = [
                    "office", "facility", "campus", "lease", "space", "sq ft",
                    "expansion", "relocation", "headquarters", "hq", "centre",
                    "new premises", "shift", "capex", "plant", "new location",
                ]
                if not any(kw in headline.lower() for kw in cre_keywords):
                    continue

                article = {
                    "title": f"{company}: {headline}",
                    "text": headline,
                    "url": f"https://www.bseindia.com/xml-data/corpfiling/AttachLive/{pdf_name}" if pdf_name else self.ANNOUNCE_URL,
                    "source": "BSE_FILING",
                    "company_hint": company,
                    "published": ann_date,
                    "signal_type_hint": "FILING",
                }

                # Try to fetch and extract PDF text
                if pdf_name:
                    pdf_url = f"https://www.bseindia.com/xml-data/corpfiling/AttachLive/{pdf_name}"
                    pdf_text = self._extract_pdf(pdf_url)
                    if pdf_text:
                        article["text"] = pdf_text[:3000]

                articles.append(article)
                print(f"[BSECrawler] CRE signal: {company} — {headline}")

        except Exception as e:
            print(f"[BSECrawler] Error: {e}")

        print(f"[BSECrawler] Found {len(articles)} CRE-relevant filings")
        return articles

    def _extract_pdf(self, pdf_url: str) -> str:
        try:
            resp = self.session.get(pdf_url, timeout=20)
            resp.raise_for_status()
            with pdfplumber.open(BytesIO(resp.content)) as pdf:
                return "\n".join(page.extract_text() or "" for page in pdf.pages[:5])
        except Exception as e:
            print(f"[BSECrawler] PDF error {pdf_url}: {e}")
            return ""


# ── MCA CRAWLER ───────────────────────────────────────────────────────────────

class MCACrawler(BaseCrawler):
    """
    Crawls MCA21 for registered office change filings (INC-22, INC-22A).
    These reveal office relocations before any press release.
    """
    # MCA free data portal
    BASE_URL = "https://www.mca.gov.in/content/mca/global/en/mca/master-data.html"
    # MCA has a company search API
    SEARCH_URL = "https://efiling.mca.gov.in/CompanyLLPMasterData/getCompanyData"

    # Form types that indicate office/space change
    CRE_FORMS = [
        "INC-22",   # Notice of situation of registered office
        "INC-22A",  # Active company tagging
        "INC-20A",  # Declaration for commencement of business
        "PAS-3",    # Return of allotment (new entity = new space)
        "MGT-14",   # Filing of resolutions (board decisions incl. office moves)
        "CHG-1",    # Mortgage/charge on property
    ]

    def __init__(self):
        super().__init__(delay=3)

    def crawl_recent_office_changes(self) -> list:
        """
        Fetch recently filed INC-22 (office change) forms from MCA open data.
        MCA publishes monthly data dumps — we parse the latest.
        """
        articles = []
        try:
            # MCA open data: company master data with recent filings
            url = "https://www.mca.gov.in/content/mca/global/en/mca/master-data/MCAdata.html"
            html = self.fetch(url)
            if not html:
                return []

            soup = BeautifulSoup(html, "lxml")
            # Find links to monthly data files
            for link in soup.find_all("a", href=True):
                href = link["href"]
                if "company_master" in href.lower() and href.endswith(".zip"):
                    articles.append({
                        "title": "MCA Company Master Data Update",
                        "text": f"MCA filed: {link.get_text(strip=True)}",
                        "url": href if href.startswith("http") else "https://www.mca.gov.in" + href,
                        "source": "MCA_FILING",
                        "signal_type_hint": "OFFICE",
                        "published": datetime.now().isoformat(),
                    })
                    break  # Just latest

        except Exception as e:
            print(f"[MCACrawler] Error: {e}")

        return articles

    def crawl_roc_filings(self) -> list:
        """
        Scrape ROC filing announcements from MCA website.
        Focus on large companies filing office change forms.
        """
        articles = []
        urls_to_check = [
            "https://www.mca.gov.in/content/mca/global/en/mca/news-and-updates.html",
        ]
        for url in urls_to_check:
            html = self.fetch(url)
            if not html:
                continue
            soup = BeautifulSoup(html, "lxml")
            for item in soup.select(".news-item, .update-item, li"):
                text = item.get_text(strip=True)
                if any(kw in text.lower() for kw in ["office", "premises", "address", "filing"]):
                    articles.append({
                        "title": text[:200],
                        "text": text,
                        "url": url,
                        "source": "MCA_NEWS",
                        "signal_type_hint": "OFFICE",
                        "published": datetime.now().isoformat(),
                    })
        return articles


# ── RERA CRAWLER ──────────────────────────────────────────────────────────────

class RERAcrawler(BaseCrawler):
    """
    Crawls state RERA portals for new project registrations.
    A newly registered commercial project = upcoming leasing opportunity.
    """
    PORTALS = {
        "Maharashtra": "https://maharera.mahaonline.gov.in",
        "Karnataka": "https://rera.karnataka.gov.in",
        "Telangana": "https://rera.telangana.gov.in",
        "DelhiNCR": "https://rera.delhi.gov.in",
        "Haryana": "https://haryanarera.gov.in",
    }

    # API endpoints discovered via browser dev tools
    MH_API = "https://maharera.mahaonline.gov.in/Layouts/MahaRERA/Handler/MahaRERAHandler.ashx"

    def __init__(self):
        super().__init__(delay=3)

    def crawl_maharera(self) -> list:
        """Crawl MahaRERA for recently registered commercial projects."""
        articles = []
        try:
            params = {
                "reqType": "SearchProject",
                "ProjectType": "2",  # Commercial
                "Status": "1",       # Registered
                "pageSize": "20",
                "pageIndex": "1",
            }
            resp = self.session.post(self.MH_API, data=params, timeout=20)
            data = resp.json()

            for project in data.get("Table", []):
                name = project.get("ProjectName", "")
                promoter = project.get("PromoterName", "")
                district = project.get("District", "")
                reg_date = project.get("RegistrationDate", "")

                articles.append({
                    "title": f"RERA Registered: {name} by {promoter} in {district}",
                    "text": f"New commercial project registered under MahaRERA. Project: {name}. "
                            f"Promoter: {promoter}. District: {district}. Date: {reg_date}. "
                            f"This indicates upcoming commercial space availability for leasing.",
                    "url": f"https://maharera.mahaonline.gov.in",
                    "source": "RERA_MH",
                    "company_hint": promoter,
                    "location_hint": district,
                    "signal_type_hint": "LEASE",
                    "published": reg_date or datetime.now().isoformat(),
                })
                print(f"[RERAcrawler] MH: {name} in {district}")

        except Exception as e:
            print(f"[RERAcrawler] MahaRERA error: {e}")

        return articles

    def crawl_all(self) -> list:
        articles = []
        articles.extend(self.crawl_maharera())
        # Add more state portals as needed
        return articles


# ── AGM / INVESTOR PRESENTATION CRAWLER ──────────────────────────────────────

class FilingPDFCrawler(BaseCrawler):
    """
    Crawls AGM minutes, investor presentations, annual reports
    from company investor relations pages and NSE filings.

    These contain:
    - "We plan to add X sq ft in FY26"
    - "New campus in Pune coming Q2"
    - "Headcount to grow from 5000 to 8000 — facility expansion planned"
    """

    NSE_FILINGS_URL = "https://www.nseindia.com/companies-listing/corporate-filings-announcements"
    NSE_API = "https://www.nseindia.com/api/corporate-announcements"

    # Known IR pages of major space-consuming companies
    IR_PAGES = [
        # IT/Tech (biggest office space consumers in India)
        {"company": "Infosys", "url": "https://www.infosys.com/investors/reports-filings/quarterly-results.html"},
        {"company": "TCS", "url": "https://www.tcs.com/investor-relations/financial-reporting"},
        {"company": "Wipro", "url": "https://www.wipro.com/investors/"},
        {"company": "HCL Tech", "url": "https://www.hcltech.com/investors"},
        {"company": "Tech Mahindra", "url": "https://www.techmahindra.com/en-in/investors/"},
        {"company": "LTIMindtree", "url": "https://www.ltimindtree.com/investors/"},
        # GCCs (fastest growing space consumers)
        {"company": "JPMorgan India", "url": "https://www.jpmorgan.com/india"},
        {"company": "Goldman Sachs India", "url": "https://www.goldmansachs.com/worldwide/india/"},
        # E-commerce / D2C
        {"company": "Meesho", "url": "https://meesho.io/press"},
        {"company": "Zepto", "url": "https://www.zepto.com/"},
    ]

    def __init__(self):
        super().__init__(delay=3)
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://www.nseindia.com/",
        })

    def crawl_nse_announcements(self, days_back: int = 3) -> list:
        """Fetch NSE announcements and extract CRE-relevant PDFs."""
        articles = []
        try:
            # First hit NSE to get cookies
            self.session.get("https://www.nseindia.com", timeout=15)
            time.sleep(2)

            params = {
                "index": "equities",
                "from_date": (datetime.now() - timedelta(days=days_back)).strftime("%d-%m-%Y"),
                "to_date": datetime.now().strftime("%d-%m-%Y"),
            }
            resp = self.session.get(self.NSE_API, params=params, timeout=20)
            data = resp.json()

            cre_keywords = [
                "office", "campus", "facility", "space", "sq ft", "lease",
                "expansion", "relocation", "headquarters", "new premises",
                "capex", "infrastructure", "real estate", "property",
            ]

            for item in data.get("data", []):
                subject = item.get("subject", "")
                company = item.get("company", "")
                attachment = item.get("attchmntFile", "")

                if not any(kw in subject.lower() for kw in cre_keywords):
                    continue

                article = {
                    "title": f"{company}: {subject}",
                    "text": subject,
                    "url": f"https://nsearchives.nseindia.com/corporate/{attachment}" if attachment else self.NSE_FILINGS_URL,
                    "source": "NSE_FILING",
                    "company_hint": company,
                    "signal_type_hint": "FILING",
                    "published": item.get("an_dt", datetime.now().isoformat()),
                }

                if attachment:
                    pdf_text = self._extract_pdf(
                        f"https://nsearchives.nseindia.com/corporate/{attachment}"
                    )
                    if pdf_text:
                        article["text"] = pdf_text[:4000]

                articles.append(article)
                print(f"[FilingPDFCrawler] NSE: {company} — {subject[:60]}")

        except Exception as e:
            print(f"[FilingPDFCrawler] NSE error: {e}")

        return articles

    def crawl_ir_pages(self) -> list:
        """Crawl IR pages of top companies for new presentation PDFs."""
        articles = []
        for company_info in self.IR_PAGES:
            html = self.fetch(company_info["url"])
            if not html:
                continue
            soup = BeautifulSoup(html, "lxml")
            for link in soup.find_all("a", href=True):
                href = link["href"]
                text = link.get_text(strip=True).lower()
                if not href.endswith(".pdf"):
                    continue
                if not any(kw in text for kw in ["investor", "presentation", "result", "annual", "quarterly", "agm"]):
                    continue

                full_url = href if href.startswith("http") else company_info["url"].rstrip("/") + "/" + href.lstrip("/")
                pdf_text = self._extract_pdf(full_url)
                if not pdf_text:
                    continue

                # Only keep if CRE-relevant
                cre_hits = sum(1 for kw in [
                    "sq ft", "sqft", "office", "campus", "facility", "lease",
                    "space", "expansion", "new location", "headquarter"
                ] if kw in pdf_text.lower())

                if cre_hits < 2:
                    continue

                articles.append({
                    "title": f"{company_info['company']} — {link.get_text(strip=True)}",
                    "text": pdf_text[:5000],
                    "url": full_url,
                    "source": "IR_PRESENTATION",
                    "company_hint": company_info["company"],
                    "signal_type_hint": "EXPAND",
                    "published": datetime.now().isoformat(),
                })
                print(f"[FilingPDFCrawler] IR PDF: {company_info['company']}")

        return articles

    def _extract_pdf(self, pdf_url: str) -> str:
        try:
            resp = self.session.get(pdf_url, timeout=25)
            resp.raise_for_status()
            with pdfplumber.open(BytesIO(resp.content)) as pdf:
                return "\n".join(page.extract_text() or "" for page in pdf.pages[:8])
        except Exception as e:
            print(f"[FilingPDFCrawler] PDF error: {e}")
            return ""


# ── LINKEDIN JOB SURGE CRAWLER ────────────────────────────────────────────────

class LinkedInSignalCrawler(BaseCrawler):
    """
    Detects hiring surges in specific cities = upcoming space demand.
    Uses LinkedIn public job search (no auth needed for basic search).

    A company posting 50+ jobs in Bengaluru = likely expanding office.
    """
    SEARCH_URL = "https://www.linkedin.com/jobs/search/"

    # Cities to monitor
    CITY_CODES = {
        "Bengaluru": "bengaluru-karnataka-india",
        "Mumbai": "mumbai-maharashtra-india",
        "Hyderabad": "hyderabad-telangana-india",
        "Pune": "pune-maharashtra-india",
        "Delhi NCR": "delhi-india",
        "Chennai": "chennai-tamil-nadu-india",
        "Gurugram": "gurugram-haryana-india",
    }

    SPACE_HUNGRY_ROLES = [
        "Software Engineer", "Data Engineer", "Product Manager",
        "Business Analyst", "Operations Manager", "Finance Manager",
        "HR Manager", "General Manager",
    ]

    def __init__(self):
        super().__init__(delay=4)
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        })

    def crawl(self, min_jobs: int = 20) -> list:
        """Find companies with large hiring surges in Indian metros."""
        articles = []
        company_city_count = {}

        for city_name, city_slug in self.CITY_CODES.items():
            try:
                url = f"{self.SEARCH_URL}?location={city_slug}&f_TPR=r86400&position=1&pageNum=0"
                html = self.fetch(url)
                if not html:
                    continue

                soup = BeautifulSoup(html, "lxml")
                job_cards = soup.select("div.base-card")

                for card in job_cards:
                    company_el = card.select_one(".base-search-card__subtitle")
                    if not company_el:
                        continue
                    company = company_el.get_text(strip=True)
                    key = (company, city_name)
                    company_city_count[key] = company_city_count.get(key, 0) + 1

            except Exception as e:
                print(f"[LinkedInCrawler] Error for {city_name}: {e}")

        # Emit signals for companies with hiring surge
        for (company, city), count in company_city_count.items():
            if count >= min_jobs:
                articles.append({
                    "title": f"{company} hiring surge in {city} ({count}+ roles)",
                    "text": (
                        f"{company} is actively hiring {count}+ positions in {city} on LinkedIn. "
                        f"Large-scale hiring in a single metro is a strong indicator of upcoming "
                        f"office space expansion or new facility requirement."
                    ),
                    "url": f"https://www.linkedin.com/jobs/search/?keywords={company}&location={city}",
                    "source": "LINKEDIN_JOBS",
                    "company_hint": company,
                    "location_hint": city,
                    "signal_type_hint": "HIRING",
                    "published": datetime.now().isoformat(),
                })
                print(f"[LinkedInCrawler] Surge: {company} in {city} — {count} jobs")

        return articles


# ── MEETING MINUTES CRAWLER ───────────────────────────────────────────────────

class MeetingMinutesCrawler(BaseCrawler):
    """
    Crawls publicly available meeting minutes from:
    - Municipal corporations (MCGM, BBMP, HMDA, DTCP agendas)
    - Industry bodies (CII, NASSCOM, FICCI press releases)
    - SEZ development commissioners
    """

    SOURCES = [
        # Municipal corporations
        {
            "name": "MCGM Mumbai",
            "url": "https://mcgm.gov.in/irj/portal/anonymous/qlstandingcommittee",
            "type": "MUNICIPAL",
        },
        {
            "name": "BBMP Bengaluru",
            "url": "https://bbmp.gov.in/en/council-meetings",
            "type": "MUNICIPAL",
        },
        {
            "name": "HMDA Hyderabad",
            "url": "https://www.hmda.gov.in/",
            "type": "MUNICIPAL",
        },
        # Industry bodies
        {
            "name": "NASSCOM",
            "url": "https://nasscom.in/media-release",
            "type": "INDUSTRY",
        },
        {
            "name": "CII",
            "url": "https://www.cii.in/PressRelease.aspx",
            "type": "INDUSTRY",
        },
        # SEZ commissioners
        {
            "name": "SEEPZ SEZ",
            "url": "https://www.seepz.gov.in/news-updates",
            "type": "SEZ",
        },
    ]

    CRE_TRIGGERS = [
        "building plan", "occupancy certificate", "commencement certificate",
        "layout approval", "noc", "development permission",
        "commercial complex", "it park", "tech park",
        "office space", "co-working", "special economic zone",
        "new office", "expansion", "sq ft", "sqft",
        "relocation", "new campus", "facility",
    ]

    def __init__(self):
        super().__init__(delay=3)

    def crawl(self) -> list:
        articles = []
        for source in self.SOURCES:
            html = self.fetch(source["url"])
            if not html:
                continue

            soup = BeautifulSoup(html, "lxml")
            # Look for PDF links to meeting minutes/agendas
            for link in soup.find_all("a", href=True):
                href = link["href"]
                link_text = link.get_text(strip=True).lower()

                is_pdf = href.endswith(".pdf") or "pdf" in href.lower()
                is_minutes = any(kw in link_text for kw in [
                    "minute", "agenda", "meeting", "resolution",
                    "notice", "circular", "order", "approval"
                ])

                if not (is_pdf or is_minutes):
                    continue

                full_url = href if href.startswith("http") else source["url"].rstrip("/") + "/" + href.lstrip("/")

                text = ""
                if is_pdf:
                    text = self._extract_pdf(full_url)
                else:
                    sub_html = self.fetch(full_url)
                    if sub_html:
                        sub_soup = BeautifulSoup(sub_html, "lxml")
                        text = sub_soup.get_text(separator=" ", strip=True)

                if not text:
                    continue

                # Check for CRE triggers
                hits = sum(1 for kw in self.CRE_TRIGGERS if kw in text.lower())
                if hits < 2:
                    continue

                articles.append({
                    "title": f"{source['name']}: {link.get_text(strip=True)[:100]}",
                    "text": text[:5000],
                    "url": full_url,
                    "source": f"MINUTES_{source['type']}",
                    "signal_type_hint": "OFFICE",
                    "published": datetime.now().isoformat(),
                })
                print(f"[MeetingMinutesCrawler] {source['name']}: {link.get_text(strip=True)[:60]}")

        return articles

    def _extract_pdf(self, pdf_url: str) -> str:
        try:
            resp = self.session.get(pdf_url, timeout=25)
            resp.raise_for_status()
            with pdfplumber.open(BytesIO(resp.content)) as pdf:
                return "\n".join(page.extract_text() or "" for page in pdf.pages[:10])
        except Exception as e:
            print(f"[MeetingMinutesCrawler] PDF error: {e}")
            return ""
