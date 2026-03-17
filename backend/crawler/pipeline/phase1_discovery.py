"""
Phase 1 — Discovery Engine

Determines how to crawl a website and finds the entry points (category URLs
or API endpoints).  Runs entirely in the Celery task process (no Scrapy).

Detection order:
  1. Pre-configured rules (training_rules from DB)
  2. API detection  (Supabase / Shopify / WooCommerce / REST JSON)
  3. Sitemap        (product URLs found directly)
  4. Category pages (nav scan → listing pages)
  5. HTML pattern   (fallback — use homepage itself as start URL)
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import List, Optional
from urllib.parse import urljoin

import requests

from crawler.extractors.api_extractor import (
    APIConfig,
    detect_api,
    build_config_from_rules,
)
from crawler.extractors.sitemap_extractor import fetch_product_urls
from crawler.extractors.html_extractor import find_category_urls

logger = logging.getLogger(__name__)

# ── Result data class ─────────────────────────────────────────────────────────

@dataclass
class DiscoveryResult:
    method: str                          # api | sitemap | category | html | failed
    category_urls: List[str] = field(default_factory=list)
    product_urls: List[str] = field(default_factory=list)   # direct (from sitemap)
    estimated_count: int = 0
    api_config: Optional[APIConfig] = None
    error: Optional[str] = None

    @property
    def success(self) -> bool:
        return self.method != "failed"

    @property
    def has_direct_urls(self) -> bool:
        """True when sitemap/API gave us product URLs directly (skip Phase 2)."""
        return bool(self.product_urls) or self.api_config is not None


# ── HTTP helper ───────────────────────────────────────────────────────────────

def _get(url: str, timeout: int = 15) -> Optional[requests.Response]:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    try:
        r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        return r
    except Exception as exc:
        logger.debug("GET %s failed: %s", url, exc)
        return None


# ── Main discovery function ───────────────────────────────────────────────────

def run_discovery(
    website_id: int,
    website_url: str,
    training_rules: Optional[dict] = None,
) -> DiscoveryResult:
    """
    Run the full discovery pipeline for a website.
    Returns a DiscoveryResult with category/product URLs and method used.
    """
    logger.info("[Discovery] Starting for website_id=%d url=%s", website_id, website_url)

    # Normalise URL
    if not website_url.startswith(("http://", "https://")):
        website_url = "https://" + website_url
    website_url = website_url.rstrip("/")

    # ── Step 0: Pre-configured rules ─────────────────────────────────────────
    if training_rules:
        crawl_type = training_rules.get("crawl_type", "auto")

        if crawl_type == "api" or training_rules.get("api_url"):
            config = build_config_from_rules(training_rules)
            if config:
                logger.info("[Discovery] Using pre-configured API for website_id=%d", website_id)
                return DiscoveryResult(
                    method="api",
                    api_config=config,
                    estimated_count=0,
                )

        if crawl_type == "html" and training_rules.get("listing_selector"):
            # Has CSS selectors — skip API detection, go straight to category scan
            pass  # falls through to category/html detection below

    # ── Step 1: Fetch homepage ────────────────────────────────────────────────
    resp = _get(website_url)
    if not resp or resp.status_code >= 400:
        logger.warning(
            "[Discovery] Cannot reach %s (status %s)",
            website_url,
            resp.status_code if resp else "N/A",
        )
        return DiscoveryResult(
            method="failed",
            error=f"Homepage unreachable: {resp.status_code if resp else 'timeout'}",
        )

    homepage_html = resp.text

    # ── Step 2: API detection ─────────────────────────────────────────────────
    api_result = detect_api(website_url, homepage_html)
    if api_result.found and api_result.config:
        logger.info(
            "[Discovery] API detected (%s) for website_id=%d",
            api_result.config.api_type, website_id,
        )
        return DiscoveryResult(
            method="api",
            api_config=api_result.config,
            estimated_count=api_result.sample_count,
        )

    # ── Step 3: Sitemap detection ─────────────────────────────────────────────
    sitemap_urls = fetch_product_urls(website_url)
    if sitemap_urls:
        logger.info(
            "[Discovery] Sitemap found %d product URLs for website_id=%d",
            len(sitemap_urls), website_id,
        )
        return DiscoveryResult(
            method="sitemap",
            product_urls=sitemap_urls,
            estimated_count=len(sitemap_urls),
        )

    # ── Step 4: Category/navigation scan ─────────────────────────────────────
    category_urls = find_category_urls(homepage_html, website_url)
    if category_urls:
        logger.info(
            "[Discovery] Found %d category URLs for website_id=%d",
            len(category_urls), website_id,
        )
        return DiscoveryResult(
            method="category",
            category_urls=category_urls,
            estimated_count=len(category_urls) * 50,  # rough estimate
        )

    # ── Step 5: Fallback — use homepage as listing page ───────────────────────
    logger.info(
        "[Discovery] Fallback to homepage as listing for website_id=%d", website_id
    )
    return DiscoveryResult(
        method="html",
        category_urls=[website_url],
        estimated_count=0,
    )
