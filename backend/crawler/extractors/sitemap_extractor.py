"""
Sitemap Extractor — Phase 1 helper.

Probes common sitemap locations and returns product/machine URLs.
"""
from __future__ import annotations

import logging
import re
from typing import List, Optional
from urllib.parse import urljoin, urlparse
from xml.etree import ElementTree as ET

import requests

logger = logging.getLogger(__name__)

SITEMAP_PATHS = [
    "/sitemap.xml",
    "/sitemap_index.xml",
    "/sitemap-index.xml",
    "/product-sitemap.xml",
    "/sitemap_products.xml",
    "/sitemap-products.xml",
    "/products-sitemap.xml",
    "/wp-sitemap.xml",
    "/page-sitemap.xml",
]

# URL path segments that indicate a product/machine detail page
PRODUCT_PATH_PATTERNS = re.compile(
    r"/(product|machine|equipment|maschine|produit|prodotto|"
    r"maquina|produkt|apparaat|listing|item|detail|used|gebraucht)s?/",
    re.IGNORECASE,
)

# URL path segments to exclude (not product pages)
EXCLUDE_PATTERNS = re.compile(
    r"/(category|categorie|tag|author|page|search|blog|news|"
    r"about|contact|cart|checkout|account|login|register)/",
    re.IGNORECASE,
)

NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}


def _session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )
    return s


def _is_product_url(url: str) -> bool:
    if EXCLUDE_PATTERNS.search(url):
        return False
    return bool(PRODUCT_PATH_PATTERNS.search(url))


def _parse_sitemap_xml(xml_text: str, base_url: str) -> tuple[List[str], List[str]]:
    """
    Returns (product_urls, child_sitemap_urls).
    Handles both sitemap index and regular sitemaps.
    """
    product_urls: List[str] = []
    sitemap_urls: List[str] = []

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return product_urls, sitemap_urls

    tag = root.tag.lower()

    if "sitemapindex" in tag:
        # Sitemap index — contains links to child sitemaps
        for loc in root.findall(".//sm:loc", NS):
            if loc.text:
                sitemap_urls.append(loc.text.strip())
        # Also try without namespace
        if not sitemap_urls:
            for loc in root.findall(".//loc"):
                if loc.text:
                    sitemap_urls.append(loc.text.strip())
    else:
        # Regular sitemap — contains page URLs
        locs = root.findall(".//sm:loc", NS) or root.findall(".//loc")
        for loc in locs:
            if loc.text:
                url = loc.text.strip()
                if _is_product_url(url):
                    product_urls.append(url)

    return product_urls, sitemap_urls


def fetch_product_urls(base_url: str, max_urls: int = 50_000) -> List[str]:
    """
    Probe all known sitemap paths for the given domain.
    Returns a deduplicated list of product/machine URLs.
    """
    session = _session()
    all_product_urls: List[str] = []
    visited_sitemaps: set = set()

    def _process_sitemap(url: str, depth: int = 0) -> None:
        if url in visited_sitemaps or depth > 4:
            return
        visited_sitemaps.add(url)

        try:
            r = session.get(url, timeout=15, allow_redirects=True)
            if r.status_code != 200:
                return
        except Exception as exc:
            logger.debug("Sitemap fetch failed %s: %s", url, exc)
            return

        products, child_sitemaps = _parse_sitemap_xml(r.text, base_url)
        all_product_urls.extend(products)

        for child_url in child_sitemaps:
            if len(all_product_urls) >= max_urls:
                break
            _process_sitemap(child_url, depth + 1)

    # Try each candidate path
    for path in SITEMAP_PATHS:
        if len(all_product_urls) >= max_urls:
            break
        _process_sitemap(urljoin(base_url, path))

    # Deduplicate
    seen: set = set()
    result: List[str] = []
    for url in all_product_urls:
        if url not in seen:
            seen.add(url)
            result.append(url)
            if len(result) >= max_urls:
                break

    logger.info("Sitemap found %d product URLs for %s", len(result), base_url)
    return result
