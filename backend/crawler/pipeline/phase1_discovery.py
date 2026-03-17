"""
Phase 1 — Discovery Engine

Determines how to crawl a website and finds entry points (category URLs
or API endpoints). Runs entirely in the Celery/direct task process.

Detection order (each wrapped in try/except — never crashes):
  1. Pre-configured rules (training_rules from DB)
  2. API detection  (Supabase / Shopify / WooCommerce / REST JSON)
  3. Sitemap        (product URLs found directly)
  4. Category pages (nav scan → listing pages)
  5. HTML fallback  (use homepage itself as start URL)

GUARANTEE: run_discovery() ALWAYS returns a DiscoveryResult with
method != "failed" unless the site is completely unreachable AND
every fallback also throws. Even then it still returns method="html"
with the homepage URL so Phase 2 can attempt a crawl.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import List, Optional

import requests

logger = logging.getLogger(__name__)

DISCOVERY_TIMEOUT = 15   # seconds per HTTP request
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/json,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
}


# ── Result data class ─────────────────────────────────────────────────────────

@dataclass
class DiscoveryResult:
    method: str                        # api | sitemap | category | html
    category_urls: List[str] = field(default_factory=list)
    product_urls: List[str] = field(default_factory=list)
    estimated_count: int = 0
    api_config: object = None          # APIConfig or None
    notes: List[str] = field(default_factory=list)

    # Always True — discovery never returns a "failed" result
    @property
    def success(self) -> bool:
        return True

    @property
    def has_direct_urls(self) -> bool:
        return bool(self.product_urls) or self.api_config is not None


# ── HTTP helper ───────────────────────────────────────────────────────────────

def _get(url: str, timeout: int = DISCOVERY_TIMEOUT) -> Optional[requests.Response]:
    try:
        r = requests.get(
            url,
            headers=REQUEST_HEADERS,
            timeout=timeout,
            allow_redirects=True,
        )
        return r
    except Exception as exc:
        logger.debug("GET %s failed: %s", url, exc)
        return None


def _normalize_url(url: str) -> str:
    url = url.strip()
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    return url.rstrip("/")


# ── Main discovery function ───────────────────────────────────────────────────

def run_discovery(
    website_id: int,
    website_url: str,
    training_rules: Optional[dict] = None,
) -> DiscoveryResult:
    """
    Run the full discovery pipeline. NEVER raises. ALWAYS returns a usable result.
    Even if the site is down, returns method='html' with the homepage URL
    so the caller can decide what to do.
    """
    website_url = _normalize_url(website_url)
    notes: List[str] = []
    logger.info("[Discovery] website_id=%d url=%s", website_id, website_url)

    # ── Step 0: Pre-configured training rules ─────────────────────────────────
    if training_rules:
        try:
            result = _check_training_rules(website_id, website_url, training_rules, notes)
            if result:
                return result
        except Exception as exc:
            notes.append(f"Training rules check error: {exc}")
            logger.warning("[Discovery] training rules error: %s", exc)

    # ── Step 1: Fetch homepage ────────────────────────────────────────────────
    homepage_html = ""
    try:
        resp = _get(website_url)
        if resp and resp.status_code < 400:
            homepage_html = resp.text
            logger.info("[Discovery] homepage fetched (%d bytes)", len(homepage_html))
        else:
            status = resp.status_code if resp else "timeout"
            notes.append(f"Homepage returned {status}")
            logger.warning("[Discovery] homepage fetch issue: %s for %s", status, website_url)
    except Exception as exc:
        notes.append(f"Homepage fetch error: {exc}")
        logger.warning("[Discovery] homepage fetch exception: %s", exc)

    # ── Step 2: API detection ─────────────────────────────────────────────────
    if homepage_html:
        try:
            from crawler.extractors.api_extractor import detect_api
            api_result = detect_api(website_url, homepage_html)
            if api_result.found and api_result.config:
                logger.info(
                    "[Discovery] API detected: %s for website_id=%d",
                    api_result.config.api_type, website_id,
                )
                return DiscoveryResult(
                    method="api",
                    api_config=api_result.config,
                    estimated_count=api_result.sample_count,
                    notes=notes,
                )
            notes.append("API detection: no API found")
        except Exception as exc:
            notes.append(f"API detection error: {exc}")
            logger.warning("[Discovery] API detection error: %s", exc)

    # ── Step 3: Sitemap detection ─────────────────────────────────────────────
    try:
        from crawler.extractors.sitemap_extractor import fetch_product_urls
        sitemap_urls = fetch_product_urls(website_url)
        if sitemap_urls:
            logger.info(
                "[Discovery] Sitemap: %d product URLs for website_id=%d",
                len(sitemap_urls), website_id,
            )
            return DiscoveryResult(
                method="sitemap",
                product_urls=sitemap_urls,
                estimated_count=len(sitemap_urls),
                notes=notes,
            )
        notes.append("Sitemap: no product URLs found")
    except Exception as exc:
        notes.append(f"Sitemap error: {exc}")
        logger.warning("[Discovery] sitemap error: %s", exc)

    # ── Step 4: Category/nav scan ─────────────────────────────────────────────
    if homepage_html:
        try:
            from crawler.extractors.html_extractor import find_category_urls
            category_urls = find_category_urls(homepage_html, website_url)
            if category_urls:
                logger.info(
                    "[Discovery] Category scan: %d URLs for website_id=%d",
                    len(category_urls), website_id,
                )
                return DiscoveryResult(
                    method="category",
                    category_urls=category_urls,
                    estimated_count=len(category_urls) * 50,
                    notes=notes,
                )
            notes.append("Category scan: no listing URLs found")
        except Exception as exc:
            notes.append(f"Category scan error: {exc}")
            logger.warning("[Discovery] category scan error: %s", exc)

    # ── Step 5: Final fallback — always succeeds ──────────────────────────────
    logger.info(
        "[Discovery] Fallback to homepage URL for website_id=%d (notes: %s)",
        website_id, "; ".join(notes),
    )
    return DiscoveryResult(
        method="html",
        category_urls=[website_url],
        estimated_count=0,
        notes=notes,
    )


# ── Training rules helper ─────────────────────────────────────────────────────

def _check_training_rules(
    website_id: int,
    website_url: str,
    rules: dict,
    notes: List[str],
) -> Optional[DiscoveryResult]:
    """Return a DiscoveryResult if training rules provide a direct config, else None."""
    from crawler.extractors.api_extractor import build_config_from_rules

    crawl_type = rules.get("crawl_type", "auto")

    if crawl_type == "api" or rules.get("api_url"):
        config = build_config_from_rules(rules)
        if config:
            notes.append(f"Using pre-configured API ({config.api_type})")
            logger.info("[Discovery] Pre-configured API for website_id=%d", website_id)
            return DiscoveryResult(
                method="api",
                api_config=config,
                estimated_count=0,
                notes=notes,
            )

    if rules.get("category_urls"):
        raw = rules["category_urls"]
        urls = [u.strip() for u in raw.split("\n") if u.strip()] if isinstance(raw, str) else raw
        if urls:
            notes.append(f"Using {len(urls)} pre-configured category URLs")
            return DiscoveryResult(
                method="category",
                category_urls=urls,
                estimated_count=len(urls) * 50,
                notes=notes,
            )

    return None
