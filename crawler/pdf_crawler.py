import pdfplumber
import requests
from io import BytesIO

class PDFCrawler:
    def extract_from_url(self, pdf_url: str) -> str:
        try:
            response = requests.get(pdf_url, timeout=20)
            response.raise_for_status()
            with pdfplumber.open(BytesIO(response.content)) as pdf:
                text = "\n".join(page.extract_text() or "" for page in pdf.pages)
            return text
        except Exception as e:
            print(f"[PDFCrawler] Error {pdf_url}: {e}")
            return ""
