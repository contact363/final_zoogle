"""
BaseZoogleSpider — shared extraction helpers.
All Zoogle spiders inherit from this.
"""
import re
from typing import Optional
from urllib.parse import urljoin, urlparse

import scrapy
from w3lib.html import remove_tags


class BaseZoogleSpider(scrapy.Spider):

    website_id:   int  = None
    start_urls:   list = []

    # ── Price extraction ──────────────────────────────────────────────────────

    def extract_price(self, text: str) -> tuple[Optional[float], str]:
        """
        Returns (price_float, currency_code).
        Handles: $12,500 | €45.000,00 | £8,900 | 12500 USD | 12500
        Returns (None, 'USD') when the price cannot be parsed.
        """
        if not text:
            return None, "USD"

        text = str(text).strip()

        # Currency detection
        currency = "USD"
        if "€" in text or "eur" in text.lower():
            currency = "EUR"
        elif "£" in text or "gbp" in text.lower():
            currency = "GBP"
        elif "chf" in text.lower():
            currency = "CHF"
        elif "cad" in text.lower():
            currency = "CAD"
        elif "aud" in text.lower():
            currency = "AUD"

        # Strip everything except digits, commas, dots
        clean = re.sub(r"[^\d.,]", "", text)
        if not clean:
            return None, currency

        # European format: 12.500,99 → 12500.99
        if re.match(r"^\d{1,3}(\.\d{3})+(,\d{1,2})?$", clean):
            clean = clean.replace(".", "").replace(",", ".")
        else:
            # US format: 12,500.99 → 12500.99
            clean = clean.replace(",", "")

        try:
            value = float(clean)
            # Sanity check: prices outside this range are probably parse errors
            if value <= 0 or value > 50_000_000:
                return None, currency
            return value, currency
        except ValueError:
            return None, currency

    # ── Text cleaning ─────────────────────────────────────────────────────────

    def clean_text(self, value) -> Optional[str]:
        """Strip HTML tags, join lists, collapse whitespace."""
        if not value:
            return None
        if isinstance(value, list):
            value = " ".join(str(v) for v in value if v)
        cleaned = re.sub(r"\s+", " ", remove_tags(str(value))).strip()
        return cleaned or None

    # ── Image URL normalisation ───────────────────────────────────────────────

    def normalize_image_urls(self, urls: list, base_url: str, max_count: int = 10) -> list:
        """
        Convert relative URLs to absolute, remove data: URIs, deduplicate.
        Returns at most *max_count* URLs.
        """
        seen: set[str] = set()
        result: list[str] = []
        for u in urls:
            if not u:
                continue
            u = u.strip()
            if not u or u.startswith("data:"):
                continue
            if not u.startswith("http"):
                u = urljoin(base_url, u)
            # Validate basic URL shape
            parsed = urlparse(u)
            if not parsed.netloc:
                continue
            if u not in seen:
                seen.add(u)
                result.append(u)
            if len(result) >= max_count:
                break
        return result

    # ── Spec table parser ─────────────────────────────────────────────────────

    def parse_spec_table(self, response, css_selector: str) -> dict:
        """Parse a <table> of key/value spec rows."""
        specs: dict = {}
        for row in response.css(css_selector):
            cells = row.css("td, th")
            if len(cells) >= 2:
                key = self.clean_text(cells[0].css("::text").getall())
                val = self.clean_text(cells[1].css("::text").getall())
                if key and val and len(key) < 100 and len(val) < 500:
                    specs[key] = val
        return specs

    # ── Text truncation ───────────────────────────────────────────────────────

    @staticmethod
    def truncate(text: Optional[str], max_len: int = 2000) -> Optional[str]:
        if not text:
            return None
        return text[:max_len] if len(text) > max_len else text
