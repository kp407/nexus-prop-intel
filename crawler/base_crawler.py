import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class BaseCrawler:
    def __init__(self, delay=2, max_retries=3):
        self.delay = delay
        self.session = requests.Session()
        retry = Retry(total=max_retries, backoff_factor=1,
                      status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.seen_urls = set()

    def fetch(self, url, headers=None):
        if url in self.seen_urls:
            return None
        self.seen_urls.add(url)
        try:
            time.sleep(self.delay)
            default_headers = {"User-Agent": "Mozilla/5.0 (NexusPropIntel/1.0)"}
            if headers:
                default_headers.update(headers)
            resp = self.session.get(url, headers=default_headers, timeout=15)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            print(f"[BaseCrawler] Error fetching {url}: {e}")
            return None
