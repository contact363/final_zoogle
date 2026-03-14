"""
Base spider with shared extraction helpers.
All Zoogle spiders inherit from this.
"""
import re
from typing import Optional
import scrapy
from w3lib.html import remove_tags


class BaseZoogleSpider(scrapy.Spider):

    # Subclasses override these
    website_id: int = None
    start_urls: list = []

    # ── Price extraction ──────────────────────────────────────────────────────

    def extract_price(self, text: str) -> tuple[Optional[float], str]:
        """
        Returns (price_float, currency).
        Handles: $12,500 | €45.000 | £8,900 | 12500 USD
        """
        if not text:
            return None, "USD"

        currency = "USD"
        if "€" in text:
            currency = "EUR"
        elif "£" in text:
            currency = "GBP"

        # Strip non-numeric except decimal separator
        clean = re.sub(r"[^\d.,]", "", text)
        # European format: 12.500,00 → 12500.00
        if re.match(r"^\d{1,3}(\.\d{3})+(,\d+)?$", clean):
            clean = clean.replace(".", "").replace(",", ".")
        else:
            clean = clean.replace(",", "")

        try:
            return float(clean), currency
        except ValueError:
            return None, currency

    # ── Text cleaning ─────────────────────────────────────────────────────────

    def clean_text(self, value) -> Optional[str]:
        if not value:
            return None
        if isinstance(value, list):
            value = " ".join(value)
        return re.sub(r"\s+", " ", remove_tags(str(value))).strip() or None

    # ── Image URL normalization ───────────────────────────────────────────────

    def normalize_image_urls(self, urls: list, base_url: str) -> list:
        from urllib.parse import urljoin, urlparse
        result = []
        for u in urls:
            u = u.strip()
            if not u or u.startswith("data:"):
                continue
            if not u.startswith("http"):
                u = urljoin(base_url, u)
            result.append(u)
        return result[:10]   # max 10 images per machine

    # ── Generic spec table parser ─────────────────────────────────────────────

    def parse_spec_table(self, response, css_selector: str) -> dict:
        specs = {}
        for row in response.css(css_selector):
            cells = row.css("td, th")
            if len(cells) >= 2:
                key = self.clean_text(cells[0].css("::text").getall())
                val = self.clean_text(cells[1].css("::text").getall())
                if key and val:
                    specs[key] = val
        return specs
