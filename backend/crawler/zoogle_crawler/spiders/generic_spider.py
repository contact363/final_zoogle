"""
Zoogle Adaptive Spider  (generic)
══════════════════════════════════

Architecture
────────────
Phase 1 – Discovery
  • Fetch /sitemap.xml (+ sitemap index) for a full URL dump.
  • Walk every nav / header / sidebar / footer link for category pages.
  • Score every URL and dispatch to the correct handler.

Phase 2 – Listing / Category Pages
  • Detect 60+ card patterns and yield partial MachineItems immediately.
  • Schedule every card's detail URL for full extraction.
  • Follow 30+ pagination patterns (rel=next, page=N, offset=N, /page/N/).
  • Persist crawling on per-page errors — one bad page never kills the run.

Phase 3 – Detail Pages
  • Try in order: JSON-LD → embedded JS state → CSS heuristics.
  • If the page is a JS-rendered SPA (empty body), optionally re-render
    with a headless Playwright subprocess and re-run extraction.

Anti-blocking
─────────────
  • SmartHeadersMiddleware rotates 30+ UAs with full browser fingerprints.
  • RetryWithBackoffMiddleware does exponential back-off + Retry-After.
  • BotDetectionMiddleware flags Cloudflare / captcha pages.
  • RateLimiterMiddleware enforces per-domain cooldowns.
  • Per-request referer chaining.
  • Randomised delay (DOWNLOAD_DELAY + RANDOMIZE_DOWNLOAD_DELAY).

Memory safety
─────────────
  • _visited set is capped at MAX_VISITED (50 k) — oldest entries evicted.
  • Playwright runs in an isolated subprocess (zero RAM cost to main process).
  • Image downloads limited to 3 per machine (IMAGES_STORE-independent).
"""

import re
import json
import logging
from collections import deque
from urllib.parse import urlparse, urljoin

import scrapy
from scrapy.http import Response, TextResponse

from zoogle_crawler.items import MachineItem
from zoogle_crawler.spiders.base_spider import BaseZoogleSpider
from zoogle_crawler.page_analyzer import (
    skip_url, score_url, is_detail_url, is_listing_url, is_worth_following,
    same_domain, looks_like_pagination_url, build_page_url, _strip_www,
    MACHINE_WORDS,
    NAV_SELECTORS, CARD_SELECTORS,
    CARD_TITLE_SELECTORS, CARD_PRICE_SELECTORS, CARD_LOCATION_SELECTORS,
    CARD_LINK_SELECTORS, CARD_IMAGE_SELECTORS,
    TITLE_SELECTORS, PRICE_SELECTORS, DESCRIPTION_SELECTORS,
    LOCATION_SELECTORS, IMAGE_SELECTORS, PAGINATION_SELECTORS,
    SPEC_TABLE_SELECTORS, BREADCRUMB_SELECTORS,
)

logger = logging.getLogger(__name__)

# Bump this every deploy so the crawl log shows which version is running.
# If you see this in the crawl log, the latest code is active on Render.
SPIDER_VERSION = "2026-03-15-v7"   # scalable rule system — no per-site spiders

# Maximum number of URLs to keep in the visited set.
# Once exceeded, the oldest 10 % are removed to free memory.
MAX_VISITED = 50_000
EVICT_CHUNK  = 5_000  # remove this many when limit hit


# ─────────────────────────────────────────────────────────────────────────────
class GenericSpider(BaseZoogleSpider):
    """
    One spider for any industrial-machine website.
    Pass website_id, start_url, and crawl_log_id as Scrapy -a arguments.
    """

    name = "generic"

    custom_settings = {
        "DEPTH_LIMIT": 8,
        "CLOSESPIDER_ITEMCOUNT": 5000,
        "DOWNLOAD_DELAY": 0.5,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "ROBOTSTXT_OBEY": False,
        "RETRY_TIMES": 4,
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 408, 429, 403, 520, 521, 522, 524],
        "DOWNLOAD_TIMEOUT": 30,
        # Cap response size at 10 MB to prevent RAM spikes on huge HTML blobs
        "DOWNLOAD_MAXSIZE": 10 * 1024 * 1024,
        # Pass ALL HTTP responses (including 404) to spider callbacks — so
        # _parse_sitemap, _parse_listing_page etc. can handle them gracefully
        # instead of having them silently dropped by HttpErrorMiddleware.
        "HTTPERROR_ALLOWED_CODES": [404, 410],
        # Follow up to 20 redirect hops — industrial sites often chain multiple
        # www → canonical → language-prefix redirects (default Scrapy limit is 20,
        # but make it explicit so RedirectMiddleware is clearly configured).
        "REDIRECT_ENABLED":   True,
        "REDIRECT_MAX_TIMES": 20,
        # Use our enhanced middlewares (override settings.py order if needed)
        "DOWNLOADER_MIDDLEWARES": {
            "scrapy.downloadermiddlewares.retry.RetryMiddleware": None,  # disable default
            "zoogle_crawler.middlewares.RetryWithBackoffMiddleware": 350,
            "zoogle_crawler.middlewares.SmartHeadersMiddleware":     400,
            "zoogle_crawler.middlewares.BotDetectionMiddleware":     410,
            "zoogle_crawler.middlewares.RateLimiterMiddleware":      420,
            "zoogle_crawler.middlewares.ProxyMiddleware":            430,
        },
    }

    def __init__(self, website_id=None, start_url=None, crawl_log_id=None,
                 training_rules=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.website_id  = int(website_id)  if website_id   else None
        self.crawl_log_id = int(crawl_log_id) if crawl_log_id else None

        # Ensure the URL has a scheme — Scrapy raises ValueError otherwise.
        # Websites are sometimes stored as "www.example.com" without https://.
        if start_url and "://" not in start_url:
            start_url = "https://" + start_url
            logger.info(f"Added missing scheme to start URL: {start_url!r}")

        self.start_urls  = [start_url] if start_url else []

        # Store original netloc for logging, then normalise immediately so
        # same_domain() comparisons work even before the homepage response
        # arrives (i.e. while sitemaps/listing paths are still in flight).
        self._original_domain = urlparse(start_url).netloc.lower() if start_url else ""
        self._base_domain     = _strip_www(self._original_domain)
        logger.info(
            f"[SPIDER v={SPIDER_VERSION}] init — "
            f"website_id={self.website_id} "
            f"domain={self._original_domain!r} "
            f"base={self._base_domain!r}"
        )

        # ── Training rules ────────────────────────────────────────────────────
        # Full rule set (CSS selectors + API config + URL patterns + delays).
        # Passed as a JSON string by crawl_tasks.  One spider handles all
        # websites — behaviour is controlled entirely by these rules.
        self._rules: dict = {}
        if training_rules:
            try:
                self._rules = json.loads(training_rules)
                logger.info(
                    f"Training rules active for {self._base_domain}: "
                    + ", ".join(
                        f"{k}={v!r}" for k, v in self._rules.items() if v
                    )
                )
            except Exception as exc:
                logger.warning(f"Could not parse training_rules JSON: {exc}")

        # ── Extended rule helpers ─────────────────────────────────────────────
        # Crawl mode: auto | html | api | playwright
        self._crawl_type: str = self._rules.get("crawl_type", "auto")

        # Compiled skip-URL patterns (built-in + site-specific)
        _builtin_skips = [
            r"/contact", r"/about", r"/blog", r"/news", r"/faq",
            r"/privacy", r"/terms", r"/login", r"/register",
            r"/cart", r"/checkout", r"/account", r"/sitemap\.html",
        ]
        _extra_skips_raw = self._rules.get("skip_url_patterns")
        _extra_skips: list = []
        if _extra_skips_raw:
            try:
                _extra_skips = json.loads(_extra_skips_raw)
            except Exception:
                pass
        self._skip_patterns: list = [
            re.compile(p, re.I) for p in (_builtin_skips + _extra_skips)
        ]

        # Optional regex that product detail URLs must match
        _plp = self._rules.get("product_link_pattern")
        self._product_link_re = re.compile(_plp, re.I) if _plp else None

        # Field mapping for API responses: API key → MachineItem field
        _fmj = self._rules.get("field_map_json")
        self._field_map: dict = {}
        if _fmj:
            try:
                self._field_map = json.loads(_fmj)
            except Exception:
                pass

        # Per-site download delay override (applied in start_requests)
        _delay = self._rules.get("request_delay")
        if _delay is not None:
            try:
                self.custom_settings = dict(self.custom_settings)   # make mutable copy
                self.custom_settings["DOWNLOAD_DELAY"] = float(_delay)
            except (TypeError, ValueError):
                pass

        # Per-site max items override
        _max = self._rules.get("max_items")
        if _max is not None:
            try:
                self.custom_settings = dict(self.custom_settings)
                self.custom_settings["CLOSESPIDER_ITEMCOUNT"] = int(_max)
            except (TypeError, ValueError):
                pass

        # Tracks the actual host after any www/non-www redirect (e.g. https://www.example.com)
        # Set by _update_base_domain when a redirect is detected.
        self._redirected_base: str = ""

        # Deque for O(1) insertion + ordered eviction
        self._visited_order: deque[str] = deque()
        self._visited: set[str] = set()

        # Check Playwright availability once at startup (informational only)
        from zoogle_crawler.js_renderer import JSRenderer
        self._js_available = JSRenderer.is_available()
        if self._js_available:
            logger.info("Playwright detected — JS fallback enabled")
        else:
            logger.info("Playwright not installed — JS fallback disabled")

    # ── Visited-URL tracking (memory-capped) ─────────────────────────────────

    def _mark_visited(self, url: str):
        if url in self._visited:
            return
        self._visited.add(url)
        self._visited_order.append(url)
        if len(self._visited) > MAX_VISITED:
            # Evict oldest chunk
            for _ in range(EVICT_CHUNK):
                if self._visited_order:
                    self._visited.discard(self._visited_order.popleft())

    def _is_visited(self, url: str) -> bool:
        return url in self._visited

    # ── Domain redirect detection ─────────────────────────────────────────────

    def _update_base_domain(self, response) -> None:
        """
        Detect www ↔ non-www (or any hostname) redirect and update
        self._base_domain so that same_domain() keeps working for every
        subsequent link on the site.

        Logs three values on first redirect so the crawl is auditable:
          • original domain  — as supplied in the start URL
          • redirected domain — actual netloc of the response URL
          • normalized base  — www-stripped canonical used for all comparisons
        """
        redirected = urlparse(response.url).netloc.lower()
        if not redirected:
            return
        canonical = _strip_www(redirected)
        if canonical == self._base_domain:
            return  # nothing changed

        logger.info(
            f"[www-redirect] original={self._original_domain!r} → "
            f"redirected={redirected!r} → canonical={canonical!r} — "
            f"base_domain updated"
        )
        self._base_domain = canonical
        self._redirected_base = f"{urlparse(response.url).scheme}://{redirected}"

    # ── Phase 1: Entry point ──────────────────────────────────────────────────

    def start_requests(self):
        """
        Seed the crawl queue with:
          1. The homepage (→ parse callback)
          2. Common sitemap paths — sent immediately so they're crawled
             even if parse() is slow or the homepage is a JS SPA.
          3. Common machine listing paths for sites without sitemaps.
        Sending these here (not inside parse()) guarantees they enter the
        Scrapy scheduler regardless of what the homepage response looks like.
        """
        start_url = self.start_urls[0] if self.start_urls else None
        if not start_url:
            return

        logger.info(
            f"[SPIDER v={SPIDER_VERSION}] start_requests — url={start_url!r} "
            f"crawl_type={self._crawl_type!r}"
        )

        # ── API-mode shortcut ─────────────────────────────────────────────────
        # If the admin configured a direct REST endpoint we probe it immediately
        # and ALSO continue with normal HTML crawl as a fallback.
        _api_url = self._rules.get("api_url")
        if _api_url:
            logger.info(f"Direct API URL configured: {_api_url!r} — scheduling first page")
            yield from self._build_direct_api_requests(_api_url, start_url)
            if self._crawl_type == "api":
                # Pure API mode — skip all HTML crawling
                return

        # 1. Homepage
        yield scrapy.Request(start_url, callback=self.parse, errback=self._errback)

        # 2. Sitemaps — dont_filter=True so they bypass duplicate filter
        base = start_url.rstrip("/")
        for path in ("/sitemap.xml", "/sitemap_index.xml", "/sitemap/",
                     "/sitemap-0.xml", "/page-sitemap.xml", "/product-sitemap.xml"):
            url = base + path
            if not self._is_visited(url):
                self._mark_visited(url)
                yield scrapy.Request(
                    url, callback=self._parse_sitemap,
                    dont_filter=True, errback=self._errback,
                )

        # 3. Common machine listing paths (handles sites with no sitemap).
        # Includes German, French and generic paths commonly used by industrial
        # machine dealers.
        for path in (
            "/machines", "/maschinen", "/inventory", "/used-machines",
            "/gebrauchtmaschinen", "/search", "/catalog", "/listings",
            "/products", "/equipment", "/stock", "/shop",
            "/machines/", "/maschinen/", "/inventory/", "/catalog/",
            # German machine dealer paths
            "/angebote", "/angebote/", "/produkte", "/produkte/",
            "/gebraucht", "/gebraucht/", "/occasion", "/occasion/",
            "/haendler", "/haendler/",
            "/maschinenpark", "/maschinenpark/",
            "/verkauf", "/verkauf/",
            "/lagerbestand", "/lagerbestand/",
            "/bestand", "/bestand/",
            "/werkzeugmaschinen", "/werkzeugmaschinen/",
            "/zerspanung", "/zerspanung/",
            "/drehen", "/fraesen", "/schleifen",
            # French paths
            "/occasions", "/occasions/", "/annonces", "/annonces/",
            # Italian paths
            "/macchine", "/usato",
            # Generic English variants
            "/for-sale", "/used-equipment", "/second-hand",
            "/pre-owned", "/available-machines",
            "/used", "/sale", "/buy",
            # Common category/filter paths
            "/category", "/categories", "/tag", "/type",
        ):
            url = base + path
            if not self._is_visited(url):
                self._mark_visited(url)
                yield scrapy.Request(
                    url, callback=self._parse_listing_page,
                    errback=self._errback,
                    meta={"referer": start_url},
                )

        # 4. JSON API probe paths — for Next.js / headless-CMS sites that
        #    expose machine data via internal REST endpoints.
        # dont_filter=True bypasses Scrapy's fingerprint-dedup filter so
        # these are ALWAYS sent even if something else queued the same URL.
        _json_api_paths = (
            "/api/subcategory/all", "/api/subcategories",
            "/api/categories", "/api/machine-types",
            "/api/products", "/api/machines",
            "/api/stock", "/api/inventory",
        )
        logger.info(f"[SPIDER v={SPIDER_VERSION}] scheduling {len(_json_api_paths)} JSON API probes for {base!r}")
        for path in _json_api_paths:
            url = base + path
            yield scrapy.Request(
                url, callback=self._parse_json_api,
                errback=self._errback,
                dont_filter=True,
                meta={"referer": start_url, "json_api_probe": True},
                headers={"Accept": "application/json"},
            )

    def parse(self, response: Response):
        """
        Homepage entry point.

        Calls _update_base_domain() so that www ↔ non-www (or any hostname)
        redirects are detected on the very first response and self._base_domain
        is updated before any link-following begins.
        Also probes inline scripts / JS bundles for embedded API credentials
        (e.g. Supabase anon key) so we can call the backend API directly.
        For Next.js / headless sites, directly re-probes the JSON API paths
        from the homepage response as a backup to start_requests probing.
        """
        self._update_base_domain(response)
        yield from self._process_page(response)
        yield from self._try_extract_api_from_scripts(response)
        yield from self._probe_json_apis_from_homepage(response)

    # ── Phase 1a: Sitemap ─────────────────────────────────────────────────────

    def _parse_sitemap(self, response: Response):
        """Parse sitemap.xml or a sitemap index. Silently skips non-XML responses."""
        self._update_base_domain(response)
        try:
            # Skip if not an XML/text response (e.g. HTML 404 page returned for missing sitemap)
            content_type = (response.headers.get("Content-Type", b"") or b"").decode("utf-8", errors="ignore").lower()
            body = response.text

            # If this looks like an HTML page (e.g. 404 or redirect), treat as listing page
            if response.status != 200 or ("<html" in body[:500].lower() and "<loc>" not in body):
                if response.status == 200:
                    logger.debug(f"Sitemap URL returned HTML, treating as listing page: {response.url}")
                    yield from self._parse_listing_page(response)
                return

            # Sitemap index → recurse into child sitemaps
            for url in re.findall(r"<sitemap>\s*<loc>(.*?)</loc>", body, re.DOTALL):
                url = url.strip()
                if url and not self._is_visited(url):
                    self._mark_visited(url)
                    yield scrapy.Request(
                        url, callback=self._parse_sitemap,
                        dont_filter=True, errback=self._errback,
                    )

            # Regular <loc> entries
            for url in re.findall(r"<loc>(.*?)</loc>", body, re.DOTALL):
                url = url.strip()
                if not url or self._is_visited(url):
                    continue
                if not same_domain(url, self._base_domain):
                    continue
                if skip_url(url):
                    continue

                self._mark_visited(url)
                s = score_url(url)
                if s >= 5:
                    yield scrapy.Request(url, callback=self._parse_detail_page,
                                         errback=self._errback)
                else:
                    yield scrapy.Request(url, callback=self._parse_listing_page,
                                         errback=self._errback)
        except Exception as exc:
            logger.error(f"Sitemap parse error at {response.url}: {exc}")

    # ── Dispatch ──────────────────────────────────────────────────────────────

    def _process_page(self, response: Response):
        """Route any response to the correct handler."""
        try:
            url = response.url
            if is_detail_url(url) and self._looks_like_detail(response):
                yield from self._parse_detail_page(response)
            else:
                yield from self._parse_listing_page(response)
        except Exception as exc:
            logger.error(f"Process page error at {response.url}: {exc}")

    # ── Phase 2: Listing / Category page ──────────────────────────────────────

    def _parse_listing_page(self, response: Response):
        """
        Extract machine cards, follow detail links, follow pagination.
        Falls back to detail-page extraction when no card containers match.
        Wrapped in try/except so one bad listing page never halts the crawl.
        """
        self._update_base_domain(response)
        try:
            category = self._extract_category(response)

            # ── If admin has configured a listing_selector, use trained extraction ──
            if self._rules.get("listing_selector"):
                yield from self._extract_trained_cards(response, category)
                yield from self._follow_all_links(response)
                yield from self._follow_pagination(response)
                return

            # Try structured card extraction first
            # _extract_machine_cards yields both MachineItem objects and
            # scrapy.Request objects (for detail-page scheduling).
            # Separate them so we can check whether any *items* were found.
            machine_items_found = []
            for obj in self._extract_machine_cards(response, category):
                yield obj
                from zoogle_crawler.items import MachineItem
                if isinstance(obj, MachineItem):
                    machine_items_found.append(obj)

            # Try JSON-LD on every page
            jsonld_items = list(self._extract_jsonld(response))
            yield from jsonld_items

            if not machine_items_found and not jsonld_items:
                if self._looks_like_detail(response):
                    # Page has machine-related signals — try CSS detail extraction
                    # (handles detail pages that scored too low for _parse_detail_page)
                    logger.debug(f"No cards/JSON-LD on {response.url} — trying detail CSS extraction")
                    yield from self._parse_detail_css(response)
                else:
                    # No structured content found — do link-based machine discovery:
                    # follow links whose text/URL contains machine keywords.
                    yield from self._schedule_machine_links(response)

            yield from self._follow_all_links(response)
            yield from self._follow_pagination(response)
        except Exception as exc:
            logger.error(f"Listing page error at {response.url}: {exc}")

    # ── Category extraction ───────────────────────────────────────────────────

    def _extract_category(self, response: Response) -> str | None:
        """
        Derive the machine category from the page context:
        breadcrumbs → <title> → URL path segments → meta keywords.
        """
        # 1. Breadcrumbs
        crumbs = [t.strip() for t in response.css(
            ", ".join(BREADCRUMB_SELECTORS)
        ).getall() if t.strip()]
        if len(crumbs) >= 2:
            # Last meaningful crumb before the current page is usually the category
            category = crumbs[-2] if len(crumbs) >= 3 else crumbs[-1]
            if 3 < len(category) < 60:
                return category

        # 2. <title> first word cluster
        title_tag = response.css("title::text").get("").strip()
        if title_tag and 4 < len(title_tag) < 60:
            # Remove the site name suffix (often after " | " or " - ")
            for sep in (" | ", " - ", " – ", " — ", " :: "):
                if sep in title_tag:
                    title_tag = title_tag.split(sep)[0].strip()
                    break
            return title_tag or None

        # 3. URL path last segment
        path = urlparse(response.url).path.rstrip("/")
        if path:
            segment = path.split("/")[-1].replace("-", " ").replace("_", " ").title()
            if 3 < len(segment) < 60:
                return segment

        return None

    # ── Phase 2a: Extract machine cards ───────────────────────────────────────

    def _extract_machine_cards(self, response: Response, category: str | None = None):
        """
        Detect product-card elements and yield partial MachineItems from them.
        Schedules detail-page requests for full extraction.
        """
        for container_sel in CARD_SELECTORS:
            try:
                cards = response.css(container_sel)
                if not cards:
                    continue

                # Skip selectors that match only 1 element AND it's a page wrapper
                # (e.g. bare "article" matches the single <article> body wrapper)
                if len(cards) == 1 and container_sel in ("article", "section"):
                    # Only use single-article if it looks like a product detail, not a wrapper
                    wrapper_text_len = len(cards[0].css("::text").getall())
                    if wrapper_text_len > 50:  # too many text nodes → page wrapper
                        continue

                extracted_any = False
                for card in cards:
                    try:
                        item = self._extract_card(card, response, category)
                        if item:
                            extracted_any = True
                            yield item
                            # Schedule full detail extraction
                            detail_url = item["machine_url"]
                            if not self._is_visited(detail_url):
                                self._mark_visited(detail_url)
                                yield response.follow(
                                    detail_url,
                                    callback=self._parse_detail_page,
                                    errback=self._errback,
                                    meta={"referer": response.url},
                                )
                    except Exception as exc:
                        logger.debug(f"Card extraction error: {exc}")
                        continue

                if extracted_any:
                    # Found the right card selector — stop trying others
                    return

            except Exception as exc:
                logger.debug(f"Card selector '{container_sel}' error: {exc}")
                continue

        # ── Generic XPath fallback: detect repeated div/li elements with img+link ──
        # This catches custom CMS layouts where no known CSS class is used.
        yield from self._extract_generic_cards(response, category)

    def _extract_generic_cards(self, response: Response, category: str | None):
        """
        Last-resort card detection via XPath pattern matching.
        Finds any <li> or <div> elements that each contain both a link and an image,
        and share the same parent — indicating a machine listing grid/list.
        """
        try:
            # Find all <li> or <div> nodes that contain both an <a> and an <img>
            candidates = response.xpath(
                "//*[self::li or self::div][.//a[@href] and .//img]"
            )
            if len(candidates) < 2:
                return  # Need at least 2 to indicate a list pattern

            # Group by parent XPath to find sibling groups
            parent_groups: dict[str, list] = {}
            for node in candidates:
                parent_path = node.xpath("..").xpath("name()").get("unknown")
                parent_class = node.xpath("../@class").get("") or ""
                group_key = f"{parent_path}::{parent_class[:40]}"
                parent_groups.setdefault(group_key, []).append(node)

            # Only process groups with 2+ siblings (true listing patterns)
            for group_key, nodes in parent_groups.items():
                if len(nodes) < 2:
                    continue

                extracted_any = False
                for card in nodes:
                    try:
                        item = self._extract_card(card, response, category)
                        if item:
                            extracted_any = True
                            yield item
                            detail_url = item["machine_url"]
                            if not self._is_visited(detail_url):
                                self._mark_visited(detail_url)
                                yield response.follow(
                                    detail_url,
                                    callback=self._parse_detail_page,
                                    errback=self._errback,
                                    meta={"referer": response.url},
                                )
                    except Exception as exc:
                        logger.debug(f"Generic card extraction error: {exc}")

                if extracted_any:
                    return  # Found a working pattern
        except Exception as exc:
            logger.debug(f"_extract_generic_cards error: {exc}")

    def _extract_card(self, card, response: Response, category: str | None) -> MachineItem | None:
        """Extract a MachineItem from a single card element."""
        # Link is required to identify the machine
        link = None
        for sel in CARD_LINK_SELECTORS:
            link = card.css(sel).get()
            if link:
                break
        if not link:
            return None

        full_url = urljoin(response.url, link.strip()).split("#")[0]
        if not same_domain(full_url, self._base_domain) or skip_url(full_url):
            return None

        # Title — try specific selectors, then any heading, then link text
        title = None
        for sel in CARD_TITLE_SELECTORS:
            title = self.clean_text(card.css(sel).getall())
            if title and len(title) > 2:
                break
        if not title:
            # Try ALL text within any heading tag inside this card
            for hsel in ("h1 *::text", "h2 *::text", "h3 *::text", "h4 *::text",
                         "h1::text", "h2::text", "h3::text", "h4::text"):
                title = self.clean_text(card.css(hsel).getall())
                if title and len(title) > 2:
                    break
        if not title:
            # Last resort: use the link anchor text if it's descriptive enough
            link_text = self.clean_text(card.css("a *::text, a::text").getall())
            if link_text and len(link_text) > 4:
                title = link_text
        if not title:
            return None

        # Price
        price_raw = None
        for sel in CARD_PRICE_SELECTORS:
            price_raw = self.clean_text(card.css(sel).getall())
            if price_raw:
                break
        price, currency = self.extract_price(price_raw or "")

        # Location
        location = None
        for sel in CARD_LOCATION_SELECTORS:
            location = self.clean_text(card.css(sel).getall())
            if location:
                break

        # Image (with lazy-load support)
        images = []
        for sel in CARD_IMAGE_SELECTORS:
            img = card.css(sel).get()
            if img and not img.startswith("data:"):
                images = self.normalize_image_urls([img], response.url)
                break

        brand, model = self._split_brand_model(title)
        machine_type = self._infer_machine_type_from_text(
            f"{title} {category or ''} {response.url}"
        ) or (category if category and self._looks_like_type(category) else None)

        return MachineItem(
            machine_url=full_url,
            website_id=self.website_id,
            website_source=self._base_domain,
            category=category,
            machine_type=machine_type,
            brand=brand,
            model=model,
            price=price,
            currency=currency,
            location=location,
            description=None,
            images=images,
            specs={},
        )

    # ── Phase 3: Detail page ──────────────────────────────────────────────────

    def _parse_detail_page(self, response: Response):
        """
        Full extraction pipeline:
        1. JSON-LD
        2. Embedded JS state (__NEXT_DATA__, window.__INITIAL_STATE__, etc.)
        3. CSS heuristics
        4. [Optional] JS render + retry CSS heuristics
        """
        self._update_base_domain(response)
        try:
            # 1. JSON-LD (fastest, most structured)
            jsonld = list(self._extract_jsonld(response))
            if jsonld:
                yield from jsonld
                return

            # 2. Embedded script JSON
            script_items = list(self._extract_script_json(response))
            if script_items:
                yield from script_items
                return

            # 3. CSS heuristics
            css_items = list(self._parse_detail_css(response))
            if css_items:
                yield from css_items
                return

            # 4. Page looks empty — try JS rendering if available
            if self._js_available and self._needs_js(response):
                yield from self._try_js_render(response)

        except Exception as exc:
            logger.error(f"Detail page error at {response.url}: {exc}")

    def _needs_js(self, response: Response) -> bool:
        """Check if the page body is too sparse to have been server-rendered."""
        from zoogle_crawler.js_renderer import is_js_page
        return is_js_page(response.text)

    def _try_js_render(self, response: Response):
        """Render the page with Playwright and re-run CSS heuristics."""
        try:
            from zoogle_crawler.js_renderer import JSRenderer
            logger.info(f"JS render attempt: {response.url}")
            rendered_html = JSRenderer.render_sync(response.url, timeout_s=25)
            if rendered_html:
                fake = response.replace(body=rendered_html.encode("utf-8", errors="replace"))
                # Try all extraction methods again on the rendered HTML
                jsonld = list(self._extract_jsonld(fake))
                if jsonld:
                    yield from jsonld
                    return
                yield from self._parse_detail_css(fake)
        except Exception as exc:
            logger.warning(f"JS render pipeline error at {response.url}: {exc}")

    def _looks_like_detail(self, response: Response) -> bool:
        """
        True if the page looks like a machine detail or listing page.
        Requires an h1 PLUS at least one machine-relevance signal:
          - a machine keyword in the heading/URL text
          - a price element
          - a spec table or definition list
          - structured machine data (itemprop, JSON-LD)
        This prevents extracting "About Us" / "Contact" pages as machines.
        """
        if not response.css("h1").get():
            return False

        # Fast keyword scan across headings + URL
        heading_text = " ".join(
            response.css("h1 *::text, h1::text, h2 *::text, h2::text").getall()
        ).lower()
        url_lower = response.url.lower()
        combined = heading_text + " " + url_lower

        if any(w in combined for w in MACHINE_WORDS):
            return True
        # Price element present → looks like a product page
        if response.css("[itemprop='price'], .price, [class*='price'], [data-price]").get():
            return True
        # Spec table or definition list → product detail page
        if response.css("table.specs, table.specifications, dl, .specs, .specifications").get():
            return True
        # JSON-LD or microdata → structured product data
        if response.css("script[type='application/ld+json'], [itemtype]").get():
            return True
        return False

    # ── JSON-LD extraction ────────────────────────────────────────────────────

    def _extract_jsonld(self, response: Response):
        for script in response.css("script[type='application/ld+json']::text").getall():
            try:
                data = json.loads(script)
            except (json.JSONDecodeError, ValueError):
                continue
            try:
                nodes = data if isinstance(data, list) else data.get("@graph", [data])
                for node in nodes:
                    if not isinstance(node, dict):
                        continue
                    node_type = node.get("@type", "")
                    if isinstance(node_type, list):
                        node_type = " ".join(node_type)
                    if any(t in node_type for t in ("Product", "Offer", "Vehicle", "Thing")):
                        item = self._jsonld_to_item(node, response.url)
                        if item:
                            yield item
                    elif "ItemList" in node_type:
                        for element in node.get("itemListElement", []):
                            if isinstance(element, dict):
                                item = self._jsonld_to_item(
                                    element.get("item", element), response.url
                                )
                                if item:
                                    yield item
            except Exception as exc:
                logger.debug(f"JSON-LD parse error: {exc}")

    def _jsonld_to_item(self, node: dict, page_url: str) -> MachineItem | None:
        name = node.get("name") or node.get("title") or ""
        if not name or len(str(name)) < 3:
            return None

        # Price
        price, currency = None, "USD"
        offers = node.get("offers") or node.get("Offers")
        if offers:
            if isinstance(offers, list):
                offers = offers[0]
            if isinstance(offers, dict):
                raw = offers.get("price") or offers.get("lowPrice")
                if raw:
                    try:
                        price = float(str(raw).replace(",", ""))
                    except ValueError:
                        pass
                currency = offers.get("priceCurrency", "USD")

        # URL
        url = node.get("url") or node.get("@id") or page_url
        if not str(url).startswith("http"):
            url = urljoin(page_url, str(url))

        # Description
        description = str(node.get("description") or "")[:2000] or None

        # Images
        images = []
        img = node.get("image")
        if isinstance(img, str):
            images = [img]
        elif isinstance(img, list):
            images = [i if isinstance(i, str) else (i.get("url", "") if isinstance(i, dict) else "") for i in img]
        elif isinstance(img, dict):
            images = [img.get("url", "")]
        images = self.normalize_image_urls([i for i in images if i], page_url)

        # Brand
        brand_raw = None
        brand_obj = node.get("brand") or node.get("manufacturer")
        if isinstance(brand_obj, str):
            brand_raw = brand_obj
        elif isinstance(brand_obj, dict):
            brand_raw = brand_obj.get("name")

        # Location
        location = None
        loc = node.get("location") or node.get("address") or node.get("areaServed")
        if isinstance(loc, str):
            location = loc
        elif isinstance(loc, dict):
            parts = [
                loc.get("addressLocality", ""),
                loc.get("addressRegion", ""),
                loc.get("addressCountry", ""),
            ]
            location = ", ".join(p for p in parts if p) or None

        brand, model = self._split_brand_model(str(name), brand_raw)
        machine_type = self._infer_machine_type_from_text(
            f"{name} {description or ''} {url}"
        )

        return MachineItem(
            machine_url=str(url),
            website_id=self.website_id,
            website_source=urlparse(str(url)).netloc or self._base_domain,
            category=None,
            machine_type=machine_type,
            brand=brand,
            model=model,
            price=price,
            currency=currency,
            location=location,
            description=description,
            images=images,
            specs={},
        )

    # ── Embedded script JSON ──────────────────────────────────────────────────

    def _extract_script_json(self, response: Response):
        """Extract machine data from __NEXT_DATA__, window.__INITIAL_STATE__, etc."""
        for script in response.css("script:not([src])::text").getall():
            if len(script) < 100:
                continue
            data = None

            # Next.js
            m = re.search(r'__NEXT_DATA__["\']?\s*[=:]\s*(\{.*?\})\s*[;<]',
                           script, re.DOTALL)
            if m:
                try:
                    data = json.loads(m.group(1))
                except Exception:
                    pass

            # window.__INITIAL_STATE__ / window.__STATE__
            if not data:
                m2 = re.search(r'window\.__(?:INITIAL_)?STATE__\s*=\s*(\{.*?\});',
                                script, re.DOTALL)
                if m2:
                    try:
                        data = json.loads(m2.group(1))
                    except Exception:
                        pass

            # Nuxt.js __NUXT__
            if not data:
                m3 = re.search(r'window\.__NUXT__\s*=\s*(\{.*?\});', script, re.DOTALL)
                if m3:
                    try:
                        data = json.loads(m3.group(1))
                    except Exception:
                        pass

            # window.dataLayer
            if not data:
                m4 = re.search(r'window\.dataLayer\s*=\s*(\[.*?\]);', script, re.DOTALL)
                if m4:
                    try:
                        data = json.loads(m4.group(1))
                    except Exception:
                        pass

            if data:
                yield from self._dig_for_machines(data, response.url)

    def _dig_for_machines(self, obj, page_url: str, depth: int = 0):
        if depth > 8:
            return
        _MACHINE_KEYS = {"machine", "brand", "model", "price", "equipment",
                          "listing", "product", "name", "title", "sku"}
        if isinstance(obj, dict):
            # Safe lower() — JSON can technically have non-string keys after parsing
            keys_lower = {k.lower() for k in obj.keys() if isinstance(k, str)}
            if len(keys_lower & _MACHINE_KEYS) >= 3:
                item = self._dict_to_item(obj, page_url)
                if item:
                    yield item
            for v in obj.values():
                yield from self._dig_for_machines(v, page_url, depth + 1)
        elif isinstance(obj, list):
            for elem in obj[:200]:  # cap to avoid huge lists
                yield from self._dig_for_machines(elem, page_url, depth + 1)

    def _dict_to_item(self, d: dict, page_url: str) -> MachineItem | None:
        def get(*keys):
            for k in keys:
                for variant in (k, k.lower(), k.upper(), k.title()):
                    v = d.get(variant)
                    if v and isinstance(v, (str, int, float)):
                        return str(v).strip()
            return None

        name = get("name", "title", "machineName", "productName", "listingTitle", "heading")
        if not name or len(name) < 4:
            return None

        url = get("url", "link", "href", "machineUrl", "listingUrl", "productUrl") or page_url
        if not str(url).startswith("http"):
            url = urljoin(page_url, str(url))

        price_raw = get("price", "askingPrice", "listPrice", "salePrice", "sellingPrice")
        price, currency = self.extract_price(price_raw or "")

        brand_raw  = get("brand", "manufacturer", "make", "brandName")
        model_raw  = get("model", "modelNumber", "modelName", "partNumber")
        location   = get("location", "city", "country", "address", "region", "state")
        description = get("description", "details", "summary", "body")

        brand, model = self._split_brand_model(name, brand_raw)
        if model_raw and not model:
            model = model_raw

        return MachineItem(
            machine_url=str(url),
            website_id=self.website_id,
            website_source=urlparse(str(url)).netloc or self._base_domain,
            category=None,
            machine_type=self._infer_machine_type_from_text(f"{name} {description or ''}"),
            brand=brand,
            model=model,
            price=price,
            currency=currency,
            location=location,
            description=description[:2000] if description else None,
            images=[],
            specs={},
        )

    # ── CSS heuristic detail extraction ───────────────────────────────────────

    def _parse_detail_css(self, response: Response):
        """Last-resort extraction via CSS selectors."""
        try:
            yield from self._parse_detail_css_inner(response)
        except Exception as exc:
            logger.error(f"_parse_detail_css error at {response.url}: {exc}")

    def _parse_detail_css_inner(self, response: Response):
        """Inner implementation — separated so _parse_detail_css can wrap with try/except."""
        # Title — try specific selectors first (most reliable)
        title = None
        for sel in TITLE_SELECTORS:
            title = self.clean_text(response.css(sel).getall())
            if title and len(title) > 2:
                break

        # Aggressive fallback: get ALL text within h1, including nested spans/em/strong
        if not title:
            h1_parts = response.css("h1 *::text, h1::text").getall()
            title = self.clean_text([t for t in h1_parts if t.strip()])

        # Try h2 if still nothing
        if not title:
            h2_parts = response.css("h2 *::text, h2::text").getall()
            title = self.clean_text([t for t in h2_parts if t.strip()])

        # Use <title> tag as last resort (strip site name suffix)
        if not title:
            page_title = response.css("title::text").get("").strip()
            for sep in (" | ", " - ", " – ", " — ", " :: ", " : "):
                if sep in page_title:
                    page_title = page_title.split(sep)[0].strip()
                    break
            if len(page_title) > 2:
                title = page_title

        if not title:
            return

        # Price
        price_raw = None
        for sel in PRICE_SELECTORS:
            price_raw = self.clean_text(response.css(sel).getall())
            if price_raw:
                break
        price, currency = self.extract_price(price_raw or "")

        # Location
        location = None
        for sel in LOCATION_SELECTORS:
            location = self.clean_text(response.css(sel).getall())
            if location:
                break

        # Description
        description = None
        for sel in DESCRIPTION_SELECTORS:
            description = self.clean_text(response.css(sel).getall())
            if description and len(description) > 20:
                break

        # Images — merge all sources, including lazy-load attrs
        image_urls: list[str] = []
        for sel in IMAGE_SELECTORS:
            found = [u for u in response.css(sel).getall()
                     if u and not u.startswith("data:")]
            image_urls.extend(found)
            if len(image_urls) >= 8:
                break
        # Also check all <img> elements for any data-* lazy src
        for attr in ("data-src", "data-lazy", "data-lazy-src", "data-original",
                     "data-image", "data-zoom-image", "data-full"):
            image_urls.extend(
                u for u in response.css(f"img::attr({attr})").getall()
                if u and not u.startswith("data:")
            )
        image_urls = self.normalize_image_urls(image_urls, response.url)

        # Specs — table rows first, then definition lists
        specs: dict = {}
        for sel in SPEC_TABLE_SELECTORS:
            specs = self.parse_spec_table(response, sel)
            if specs:
                break
        if not specs:
            keys = response.css(
                "dl dt::text, .spec-key::text, .spec-label::text, th::text"
            ).getall()
            vals = response.css(
                "dl dd::text, .spec-value::text, .spec-data::text, td::text"
            ).getall()
            specs = {k.strip(): v.strip()
                     for k, v in zip(keys, vals) if k.strip() and v.strip()}

        # Brand from dedicated elements
        brand_raw = None
        for sel in (
            "[itemprop='brand'] [itemprop='name']::text",
            "[itemprop='brand']::text",
            "meta[itemprop='brand']::attr(content)",
            ".brand::text", ".brand-name::text",
            ".manufacturer::text", ".make::text",
            "[data-brand]::text", "[data-make]::text",
        ):
            b = self.clean_text(response.css(sel).getall())
            if b:
                brand_raw = b
                break

        # Brand fallback: scan spec table rows for known label words
        if not brand_raw:
            _BRAND_LABELS = {"make", "brand", "manufacturer", "hersteller",
                             "marque", "fabricante", "марка", "fabrikant"}
            for row in response.css("table tr, dl, .spec-row, .specs-row"):
                cells = row.css("td, th, dt, .spec-label, .spec-key")
                if not cells:
                    continue
                label = (cells[0].css("::text").get("") or "").strip().lower().rstrip(":")
                if label in _BRAND_LABELS:
                    val_cells = row.css("td:nth-child(2), dd, .spec-value, .spec-data")
                    val = self.clean_text(val_cells.css("::text").getall())
                    if val:
                        brand_raw = val
                        break

        # Location fallback: scan spec table rows for location label words
        if not location:
            _LOC_LABELS = {"location", "city", "country", "address", "region",
                           "state", "standort", "lieu", "ort", "pays", "land",
                           "país", "ubicación", "sede"}
            for row in response.css("table tr, dl, .spec-row, .specs-row"):
                cells = row.css("td, th, dt, .spec-label, .spec-key")
                if not cells:
                    continue
                label = (cells[0].css("::text").get("") or "").strip().lower().rstrip(":")
                if label in _LOC_LABELS:
                    val_cells = row.css("td:nth-child(2), dd, .spec-value, .spec-data")
                    val = self.clean_text(val_cells.css("::text").getall())
                    if val:
                        location = val
                        break

        # Location fallback: structured address microdata
        if not location:
            parts = [
                response.css("[itemprop='addressLocality']::text").get(""),
                response.css("[itemprop='addressRegion']::text").get(""),
                response.css("[itemprop='addressCountry']::text, "
                             "[itemprop='addressCountry']::attr(content)").get(""),
            ]
            loc_str = ", ".join(p.strip() for p in parts if p.strip())
            if loc_str:
                location = loc_str

        brand, model = self._split_brand_model(title, brand_raw)
        machine_type = self._infer_machine_type(response)
        category     = self._extract_category(response)

        yield MachineItem(
            machine_url=response.url,
            website_id=self.website_id,
            website_source=urlparse(response.url).netloc,
            category=category,
            machine_type=machine_type,
            brand=brand,
            model=model,
            price=price,
            currency=currency,
            location=location,
            description=description[:2000] if description else None,
            images=image_urls,
            specs=specs,
        )

    # ── Machine link discovery fallback ──────────────────────────────────────

    def _schedule_machine_links(self, response: Response):
        """
        Fallback discovery: scan all <a> tags on the page.
        Any link whose anchor text OR href contains a machine keyword is
        scheduled as a detail-page request.  This handles sites where the
        card/grid HTML doesn't match any known CSS pattern.
        """
        try:
            seen: set[str] = set()

            for a_el in response.css("a[href]"):
                href = (a_el.css("::attr(href)").get() or "").strip()
                if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
                    continue

                # Check anchor text AND all descendant text (for icon-font links)
                anchor_text = " ".join(
                    t.strip() for t in a_el.css("*::text, ::text").getall() if t.strip()
                ).lower()
                href_lower = href.lower()

                has_machine_word = (
                    any(w in anchor_text for w in MACHINE_WORDS)
                    or any(w in href_lower for w in MACHINE_WORDS)
                )
                if not has_machine_word:
                    continue

                full_url = urljoin(response.url, href).split("#")[0]
                if not same_domain(full_url, self._base_domain) or skip_url(full_url):
                    continue
                if self._is_visited(full_url) or full_url in seen:
                    continue

                seen.add(full_url)
                self._mark_visited(full_url)
                # Use score to decide callback — high-score URLs → detail, rest → listing
                s = score_url(full_url)
                cb = self._parse_detail_page if s >= 5 else self._parse_listing_page
                yield response.follow(
                    full_url, callback=cb,
                    errback=self._errback,
                    meta={"referer": response.url},
                )
        except Exception as exc:
            logger.debug(f"_schedule_machine_links error at {response.url}: {exc}")

    # ── Trained extraction (uses admin-configured CSS selectors) ─────────────

    def _extract_trained_cards(self, response: Response, category: str | None = None):
        """
        Use admin-configured CSS selectors to extract machine cards.

        Called only when self._rules has a listing_selector.
        Each matched card is:
          1. Yielded as a partial MachineItem (for immediate indexing).
          2. Its detail URL is scheduled for full extraction via _parse_detail_page.

        Unknown / unmatched selectors are logged as warnings so the admin
        can diagnose and correct them without touching the crawler code.
        """
        listing_sel = self._rules.get("listing_selector", "")
        cards = response.css(listing_sel)

        if not cards:
            logger.warning(
                f"[trained] listing_selector {listing_sel!r} matched 0 elements at {response.url} "
                f"— check selector in Admin → Web Sources → Train"
            )
            # Fall through to generic extraction so the crawl isn't wasted
            yield from self._extract_machine_cards(response, category)
            return

        logger.info(f"[trained] {len(cards)} cards via {listing_sel!r} at {response.url}")

        title_sel    = self._rules.get("title_selector")
        url_sel      = self._rules.get("url_selector")
        price_sel    = self._rules.get("price_selector")
        desc_sel     = self._rules.get("description_selector")
        image_sel    = self._rules.get("image_selector")
        category_sel = self._rules.get("category_selector")

        for card in cards:
            # ── Link (required) ───────────────────────────────────────────────
            link = None
            if url_sel:
                link = card.css(url_sel).get()
                if not link:
                    logger.debug(f"[trained] url_selector {url_sel!r} matched nothing in card")
            if not link:
                link = card.css("a::attr(href)").get()
            if not link:
                continue

            full_url = urljoin(response.url, link.strip()).split("#")[0]
            if not same_domain(full_url, self._base_domain) or skip_url(full_url):
                continue

            # Schedule detail page
            if not self._is_visited(full_url):
                self._mark_visited(full_url)
                yield scrapy.Request(
                    full_url,
                    callback=self._parse_trained_detail,
                    errback=self._errback,
                    meta={"referer": response.url, "trained": True},
                )

            # ── Title ─────────────────────────────────────────────────────────
            title = None
            if title_sel:
                title = self.clean_text(card.css(title_sel).getall())
                if not title:
                    logger.debug(f"[trained] title_selector {title_sel!r} matched nothing in card")
            if not title:
                for sel in CARD_TITLE_SELECTORS:
                    title = self.clean_text(card.css(sel).getall())
                    if title:
                        break
            if not title:
                continue

            # ── Price ─────────────────────────────────────────────────────────
            price_raw = None
            if price_sel:
                price_raw = self.clean_text(card.css(price_sel).getall())
            price, currency = self.extract_price(price_raw or "")

            # ── Description ───────────────────────────────────────────────────
            description = None
            if desc_sel:
                description = self.clean_text(card.css(desc_sel).getall())

            # ── Image ─────────────────────────────────────────────────────────
            images: list[str] = []
            if image_sel:
                img = card.css(image_sel).get()
                if img and not img.startswith("data:"):
                    images = self.normalize_image_urls([img], response.url)
            if not images:
                for sel in CARD_IMAGE_SELECTORS:
                    img = card.css(sel).get()
                    if img and not img.startswith("data:"):
                        images = self.normalize_image_urls([img], response.url)
                        break

            # ── Category ──────────────────────────────────────────────────────
            cat = None
            if category_sel:
                cat = self.clean_text(card.css(category_sel).getall())
            cat = cat or category

            brand, model = self._split_brand_model(title)
            machine_type = self._infer_machine_type_from_text(
                f"{title} {cat or ''} {full_url}"
            )

            yield MachineItem(
                machine_url=full_url,
                website_id=self.website_id,
                website_source=self._base_domain,
                category=cat,
                machine_type=machine_type,
                brand=brand,
                model=model,
                price=price,
                currency=currency,
                location=None,
                description=description,
                images=images,
                specs={},
            )

    def _parse_trained_detail(self, response: Response):
        """
        Detail page handler for trained crawls.
        Uses trained detail selectors when present, otherwise falls back to
        the standard CSS/JSON-LD/script pipeline.
        """
        self._update_base_domain(response)
        try:
            title_sel = self._rules.get("title_selector")
            price_sel = self._rules.get("price_selector")
            desc_sel  = self._rules.get("description_selector")
            image_sel = self._rules.get("image_selector")

            # Use trained selectors if any detail-page fields were configured
            if title_sel or desc_sel or price_sel:
                title = None
                if title_sel:
                    title = self.clean_text(response.css(title_sel).getall())
                if not title:
                    title = self.clean_text(response.css("h1 *::text, h1::text").getall())
                if not title:
                    # Last resort: <title> tag
                    page_title = response.css("title::text").get("").strip()
                    for sep in (" | ", " - ", " – ", " — "):
                        if sep in page_title:
                            page_title = page_title.split(sep)[0].strip()
                            break
                    title = page_title or None

                if not title:
                    logger.debug(f"[trained] no title found on detail page {response.url}")
                    return

                price_raw = self.clean_text(response.css(price_sel).getall()) if price_sel else None
                price, currency = self.extract_price(price_raw or "")

                description = self.clean_text(response.css(desc_sel).getall()) if desc_sel else None

                images: list[str] = []
                if image_sel:
                    img_urls = response.css(image_sel).getall()
                    images = self.normalize_image_urls(img_urls, response.url)

                brand, model = self._split_brand_model(title)
                machine_type = self._infer_machine_type(response)
                category     = self._extract_category(response)

                yield MachineItem(
                    machine_url=response.url,
                    website_id=self.website_id,
                    website_source=self._base_domain,
                    category=category,
                    machine_type=machine_type,
                    brand=brand,
                    model=model,
                    price=price,
                    currency=currency,
                    location=None,
                    description=description[:2000] if description else None,
                    images=images,
                    specs={},
                )
            else:
                # No detail-page trained selectors — use standard pipeline
                yield from self._parse_detail_page(response)

        except Exception as exc:
            logger.error(f"_parse_trained_detail error at {response.url}: {exc}")

    # ── Link following ────────────────────────────────────────────────────────

    def _follow_all_links(self, response: Response):
        """Follow category and detail links from any page."""
        try:
            yield from self._follow_all_links_inner(response)
        except Exception as exc:
            logger.error(f"_follow_all_links error at {response.url}: {exc}")

    def _follow_all_links_inner(self, response: Response):
        """Inner implementation — separated so _follow_all_links can wrap with try/except."""
        seen: set[str] = set()

        # Priority: nav links (categories)
        nav_hrefs: list[str] = []
        for sel in NAV_SELECTORS:
            nav_hrefs.extend(response.css(sel).getall())

        all_hrefs = response.css("a::attr(href)").getall()

        for href in nav_hrefs + all_hrefs:
            href = (href or "").strip()
            if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
                continue

            url = urljoin(response.url, href).split("#")[0]
            if not same_domain(url, self._base_domain):
                continue
            if skip_url(url):
                continue
            if self._is_visited(url) or url in seen:
                continue

            seen.add(url)
            s = score_url(url)

            # Follow ALL intra-domain non-skipped links — machine listing URLs
            # frequently score 0 (e.g. ?cat=5&lang=de, ?id=123) so we cannot
            # gate on score >= 1.  Score is still used to choose the callback.
            self._mark_visited(url)
            if s >= 5:
                yield response.follow(
                    url, callback=self._parse_detail_page,
                    errback=self._errback,
                    meta={"referer": response.url},
                )
            else:
                yield response.follow(
                    url, callback=self._parse_listing_page,
                    errback=self._errback,
                    meta={"referer": response.url},
                )

    def _follow_pagination(self, response: Response):
        """
        Follow pagination links on listing pages.
        Handles: rel=next links, .pagination links, ?page=N, ?offset=N,
        /page/N/ path segments, WooCommerce, and numeric page links.
        When a trained pagination_selector is configured it is tried first.
        """
        seen: set[str] = set()
        current = response.url

        # ── Admin-configured pagination selector (highest priority) ──────────
        trained_pag = self._rules.get("pagination_selector")
        if trained_pag:
            for href in response.css(trained_pag).getall():
                url = urljoin(current, href).split("#")[0]
                if self._add_pagination_url(url, seen):
                    logger.debug(f"[trained] pagination → {url}")
                    yield response.follow(
                        url, callback=self._parse_listing_page,
                        errback=self._errback,
                        meta={"referer": current},
                    )

        # Standard CSS-based pagination selectors
        for sel in PAGINATION_SELECTORS:
            for href in response.css(sel).getall():
                url = urljoin(current, href).split("#")[0]
                if self._add_pagination_url(url, seen):
                    yield response.follow(
                        url, callback=self._parse_listing_page,
                        errback=self._errback,
                        meta={"referer": current},
                    )

        # Any link on the page that looks like a pagination URL
        for href in response.css("a::attr(href)").getall():
            url = urljoin(current, href).split("#")[0]
            if (looks_like_pagination_url(url)
                    and same_domain(url, self._base_domain)
                    and not skip_url(url)):
                if self._add_pagination_url(url, seen):
                    yield response.follow(
                        url, callback=self._parse_listing_page,
                        errback=self._errback,
                        meta={"referer": current},
                    )

        # Probe ahead if we're on page 1 and found results
        # (helps sites without explicit next buttons)
        page_match = re.search(r"[?&/]p(?:age)?[=/](\d+)", current, re.IGNORECASE)
        current_page = int(page_match.group(1)) if page_match else 1
        if current_page == 1:
            for next_page in (2, 3):
                candidate = build_page_url(current, next_page)
                if self._add_pagination_url(candidate, seen):
                    yield scrapy.Request(
                        candidate,
                        callback=self._parse_listing_page,
                        errback=self._errback,
                        meta={"referer": current},
                    )

    def _add_pagination_url(self, url: str, seen: set) -> bool:
        """Add url to seen + visited; return True if it's new."""
        if not url or url in seen or self._is_visited(url):
            return False
        if not same_domain(url, self._base_domain) or skip_url(url):
            return False
        seen.add(url)
        self._mark_visited(url)
        return True

    # ── Error callback ────────────────────────────────────────────────────────

    def _errback(self, failure):
        """Log request failures without crashing the spider."""
        logger.warning(
            f"Request failed [{failure.value.__class__.__name__}: {failure.value}]: "
            f"{getattr(failure.request, 'url', 'unknown')}"
        )

    # ── Type / brand helpers ──────────────────────────────────────────────────

    def _split_brand_model(self, title: str, known_brand: str | None = None) -> tuple:
        if known_brand:
            model = title
            if title.lower().startswith(known_brand.lower()):
                model = title[len(known_brand):].strip(" -:")
            return known_brand.strip().title(), (model.strip() or None)

        if not title:
            return None, None

        try:
            from app.services.normalization_service import BRAND_ALIASES
            known = set(BRAND_ALIASES.values())
            parts = title.split()
            for n in (3, 2, 1):
                candidate = " ".join(parts[:n]).title()
                if candidate.lower() in {b.lower() for b in known}:
                    return candidate, " ".join(parts[n:]).strip() or None
        except Exception:
            pass

        parts = title.split()
        if len(parts) >= 2:
            return parts[0].title(), " ".join(parts[1:])
        return None, title

    def _infer_machine_type(self, response: Response) -> str | None:
        crumbs = " ".join(response.css(
            ", ".join(BREADCRUMB_SELECTORS)
        ).getall()).lower()
        meta_kw   = response.css("meta[name='keywords']::attr(content)").get("").lower()
        meta_desc = response.css("meta[name='description']::attr(content)").get("").lower()
        h_text    = " ".join(response.css("h1::text,h2::text,h3::text").getall()).lower()
        combined  = f"{response.url.lower()} {crumbs} {meta_kw} {meta_desc} {h_text}"
        return self._infer_machine_type_from_text(combined)

    def _infer_machine_type_from_text(self, text: str) -> str | None:
        try:
            from app.services.normalization_service import TYPE_SYNONYMS
            text_lower = text.lower()
            for synonym, canonical in TYPE_SYNONYMS.items():
                if synonym in text_lower:
                    return canonical
        except Exception:
            pass
        return None

    def _looks_like_type(self, text: str) -> bool:
        """True if the text contains a machine-type keyword."""
        from zoogle_crawler.page_analyzer import MACHINE_WORDS
        return bool(set(re.split(r"\W+", text.lower())) & MACHINE_WORDS)

    # ── JSON API detection (for Next.js / headless sites) ─────────────────────

    def _probe_json_apis_from_homepage(self, response: Response):
        """
        Secondary JSON API probe — called from the homepage callback so it runs
        regardless of what start_requests generated.  Uses dont_filter=True so
        the requests always go through even if start_requests already probed
        the same URLs (duplicate items are safely deduplicated by the pipeline).
        Only fires on the root path (/) to avoid probing on every page.
        Probes BOTH the original base and any www-redirected base so that
        www ↔ non-www redirects never block API discovery.
        """
        parsed = urlparse(response.url)
        if parsed.path.strip("/"):
            return  # Not the homepage

        ct = (response.headers.get("Content-Type", b"") or b"").decode("utf-8", errors="ignore").lower()
        if "html" not in ct:
            return  # Only probe when homepage is HTML (i.e. an SPA / SSR site)

        # Collect the base URLs to probe: the response URL (canonical after redirects)
        # plus the original start URL base in case they differ (www ↔ non-www).
        bases_to_probe: set[str] = {f"{parsed.scheme}://{parsed.netloc}"}
        if self.start_urls:
            orig = urlparse(self.start_urls[0])
            bases_to_probe.add(f"{orig.scheme}://{orig.netloc}")
        if self._redirected_base:
            bases_to_probe.add(self._redirected_base)

        _json_api_paths = (
            "/api/subcategory/all", "/api/subcategories",
            "/api/categories", "/api/machine-types",
            "/api/products", "/api/machines",
            "/api/stock", "/api/inventory",
        )
        logger.info(f"[json_api] homepage probe — bases={bases_to_probe} paths={len(_json_api_paths)}")

        for base in bases_to_probe:
            for path in _json_api_paths:
                url = f"{base}{path}"
                yield scrapy.Request(
                    url, callback=self._parse_json_api,
                    errback=self._errback,
                    dont_filter=True,
                    meta={"referer": response.url, "json_api_probe": True},
                    headers={"Accept": "application/json"},
                )

    def _parse_json_api(self, response: Response):
        """
        Handle responses from probed JSON API endpoints.
        Handles two shapes:
          • List of subcategories  → schedule /api/product/{slug} per entry
          • List of machine objects → yield MachineItems directly
        """
        # Log every call so we can diagnose deployment / routing issues
        ct = (response.headers.get("Content-Type", b"") or b"").decode("utf-8", errors="ignore").lower()
        logger.info(
            f"[json_api] callback fired — url={response.url} "
            f"status={response.status} ct={ct[:60]!r}"
        )

        # Accept JSON content-type OR fallback: body starts with [ or { (some
        # Next.js routes return text/html content-type even for JSON payloads).
        body_start = response.text[:1].strip() if response.text else ""
        is_json_body = body_start in ("[", "{")
        is_json_ct   = any(t in ct for t in ("json", "javascript"))

        if response.status not in (200,) or (not is_json_ct and not is_json_body):
            logger.debug(f"[json_api] skip — not JSON (status={response.status} ct={ct[:40]!r} body_start={body_start!r})")
            return

        try:
            data = json.loads(response.text)
        except Exception as exc:
            logger.debug(f"[json_api] JSON parse failed at {response.url}: {exc}")
            return

        if not data:
            return

        logger.info(f"JSON API response at {response.url}: {type(data).__name__} len={len(data) if isinstance(data, (list, dict)) else '?'}")
        try:
            yield from self._extract_json_api_items(data, response)
        except Exception as exc:
            logger.error(f"JSON API extraction error at {response.url}: {exc}", exc_info=True)

    def _extract_json_api_items(self, data, response: Response):
        """
        Dispatch JSON API data to either subcategory chaining or direct machine
        item extraction.
        """
        base_url = "{0.scheme}://{0.netloc}".format(urlparse(response.url))
        page_url = response.url

        # Unwrap common envelope shapes: {"data": [...]} or {"results": [...]}
        if isinstance(data, dict):
            for key in ("data", "results", "items", "products", "machines", "records"):
                if isinstance(data.get(key), list):
                    data = data[key]
                    break
            else:
                # Single machine object?
                item = self._json_to_machine_item(data, page_url, base_url)
                if item:
                    yield item
                return

        if not isinstance(data, list) or not data:
            return

        first = data[0] if data else {}
        if not isinstance(first, dict):
            return

        # ── Detect subcategory list vs machine list ────────────────────────────
        # corelmachine /api/subcategory/all returns:
        #   {_id, title, url, image, order, description:"", status:true, category:{...}}
        # These objects have 'description' (empty) and 'status' (bool True) but
        # crucially do NOT have price/year/reference_no/capacity/product_status.
        # Use only the discriminating machine-specific fields (not 'description'
        # or 'brand'/'model' which can appear in both shapes).
        strict_machine_fields = {"price", "year", "reference_no", "capacity", "product_status"}
        is_subcategory_list = (
            not any(f in first for f in strict_machine_fields)
            and ("url" in first or "slug" in first)
            and len(data) <= 200
        )
        # Extra signal: corelmachine subcategories have a nested "category" dict
        if not is_subcategory_list and isinstance(first.get("category"), dict):
            if "url" in first and "price" not in first:
                is_subcategory_list = True

        if is_subcategory_list:
            # Schedule /api/product/{url} for each subcategory
            base_api = "{0.scheme}://{0.netloc}".format(urlparse(page_url))
            logger.info(f"Subcategory list detected at {page_url}: {len(data)} entries — scheduling product API calls")
            for entry in data:
                slug = entry.get("url") or entry.get("slug") or entry.get("id")
                if not slug:
                    continue
                api_url = f"{base_api}/api/product/{slug}"
                if not self._is_visited(api_url):
                    self._mark_visited(api_url)
                    logger.info(f"Scheduling product API: {api_url}")
                    yield scrapy.Request(
                        api_url,
                        callback=self._parse_json_api,
                        errback=self._errback,
                        headers={"Accept": "application/json"},
                        meta={"subcategory_slug": slug},
                    )
        else:
            # Machine list — yield items directly and also schedule detail pages
            # subcategory_slug is set when this list was fetched via /api/product/{slug}
            subcategory_slug = response.meta.get("subcategory_slug", "")
            for d in data:
                item = self._json_to_machine_item(d, page_url, base_url,
                                                  fallback_subcategory=subcategory_slug)
                if item:
                    yield item
                    # Schedule the HTML detail page too for richer extraction
                    detail = item.get("machine_url", "")
                    if detail and detail.startswith("http") and not self._is_visited(detail):
                        self._mark_visited(detail)
                        yield scrapy.Request(
                            detail,
                            callback=self._parse_detail_page,
                            errback=self._errback,
                            meta={"referer": page_url, "partial_item": dict(item)},
                        )

    def _json_to_machine_item(self, d: dict, page_url: str, base_url: str,
                               fallback_subcategory: str = "") -> "MachineItem | None":
        """
        Convert a raw JSON dict to a MachineItem.
        Handles corelmachine.com's product shape:
          {title, reference_no, capacity, product_status, url,
           sub_category: {url}, image, year}
        Also handles generic shapes with price/brand/model/location.
        fallback_subcategory: the subcategory slug from the API call
          (used when the product JSON doesn't embed sub_category).
        """
        if not isinstance(d, dict):
            return None

        # Skip sold / unavailable machines
        # d.get("status") can be a boolean (True/False) from JSON — cast to str first
        status_raw = d.get("product_status") or d.get("status") or ""
        status = str(status_raw).lower() if status_raw is not None else ""
        if status in ("sold", "unavailable", "inactive", "deleted"):
            return None

        # ── Title ──────────────────────────────────────────────────────────────
        title = (d.get("title") or d.get("name") or d.get("model") or "").strip()
        if not title:
            return None

        # ── Machine URL ────────────────────────────────────────────────────────
        # corelmachine pattern: /usedmachinestocklist/{sub_category.url}/{machine.url}
        raw_url = d.get("url") or d.get("slug") or d.get("link") or ""
        sub_cat = d.get("sub_category") or {}
        # Prefer the embedded sub_category, fall back to the API slug used to fetch this list
        sub_cat_url = (
            (sub_cat.get("url") if isinstance(sub_cat, dict) else None)
            or fallback_subcategory
            or None
        )

        if raw_url and not raw_url.startswith("http"):
            if sub_cat_url:
                # corelmachine detail URL pattern
                machine_url = f"{base_url}/usedmachinestocklist/{sub_cat_url}/{raw_url}"
            else:
                machine_url = urljoin(base_url + "/", raw_url.lstrip("/"))
        elif raw_url.startswith("http"):
            machine_url = raw_url
        else:
            machine_url = page_url

        # ── Brand / Model ──────────────────────────────────────────────────────
        brand = (d.get("brand") or d.get("make") or d.get("manufacturer") or "").strip() or None
        model = (d.get("model") or "").strip() or None
        if not brand and not model:
            brand, model = self._split_brand_model(title)

        # ── Price ──────────────────────────────────────────────────────────────
        raw_price = d.get("price") or d.get("asking_price") or d.get("sale_price")
        price = None
        if raw_price is not None:
            try:
                price = float(str(raw_price).replace(",", "").replace(" ", ""))
            except (ValueError, TypeError):
                pass

        # ── Location ──────────────────────────────────────────────────────────
        location = (d.get("location") or d.get("city") or d.get("country") or "").strip() or None

        # ── Description ───────────────────────────────────────────────────────
        desc_parts = []
        if d.get("capacity"):
            desc_parts.append(f"Capacity: {d['capacity']}")
        if d.get("year"):
            desc_parts.append(f"Year: {d['year']}")
        if d.get("reference_no"):
            desc_parts.append(f"Ref: {d['reference_no']}")
        if d.get("description"):
            desc_parts.append(str(d["description"]))
        description = " | ".join(desc_parts) or None

        # ── Image ──────────────────────────────────────────────────────────────
        img = d.get("image") or d.get("thumbnail") or d.get("photo") or ""
        if isinstance(img, list):
            img = img[0] if img else ""
        if img and not img.startswith("http"):
            img = urljoin(base_url + "/", img.lstrip("/"))
        thumbnail = img or None

        # ── Machine type ──────────────────────────────────────────────────────
        machine_type = (d.get("category") or d.get("machine_type") or d.get("type") or "").strip() or None
        if not machine_type:
            machine_type = self._infer_machine_type_from_text(title)

        item = MachineItem()
        item["website_id"]    = self.website_id
        item["crawl_log_id"]  = self.crawl_log_id
        item["machine_url"]   = machine_url
        item["machine_type"]  = machine_type
        item["brand"]         = brand
        item["model"]         = model
        item["price"]         = price
        item["currency"]      = "USD"
        item["location"]      = location
        item["description"]   = description
        item["thumbnail_url"] = thumbnail
        item["specs"]         = []
        item["images"]        = [thumbnail] if thumbnail else []
        return item

    # ── Supabase / SPA API detection ──────────────────────────────────────────

    def _try_extract_api_from_scripts(self, response: Response):
        """
        Detect embedded API credentials (Supabase, Firebase, etc.) in:
          1. Inline <script> tags
          2. The main JS bundle referenced in <script src="...">
        If found, yields API requests to fetch machine data directly.
        If JS bundle URLs are found, schedules them for credential scanning.
        """
        try:
            # 1. Check inline scripts for Supabase patterns
            for script_text in response.css("script:not([src])::text").getall():
                if "supabase" in script_text.lower() or "supabase.co" in script_text:
                    logger.info(f"Supabase credentials found in inline script at {response.url}")
                    yield from self._try_supabase_from_text(script_text, response.url)
                    return  # Only need to find one

            # 2. Look for main JS bundle URLs to fetch and scan
            bundle_patterns = [
                "script[src*='/assets/index']::attr(src)",
                "script[src*='/static/js/main']::attr(src)",
                "script[src*='/static/js/bundle']::attr(src)",
                "script[src*='app.']::attr(src)",
            ]
            for sel in bundle_patterns:
                src = response.css(sel).get()
                if src:
                    bundle_url = urljoin(response.url, src)
                    if not self._is_visited(bundle_url):
                        self._mark_visited(bundle_url)
                        logger.info(f"Scheduling JS bundle scan: {bundle_url}")
                        yield scrapy.Request(
                            bundle_url,
                            callback=self._parse_js_for_api_creds,
                            errback=self._errback,
                            meta={"referer": response.url},
                        )
                    return  # One bundle is enough

            # 3. Broader: any script with /assets/*.js or /static/js/*.js
            for src in response.css("script[src]::attr(src)").getall():
                if re.search(r"/assets/[^/]+\.js$|/static/js/[^/]+\.js$", src):
                    bundle_url = urljoin(response.url, src)
                    if not self._is_visited(bundle_url):
                        self._mark_visited(bundle_url)
                        logger.info(f"Scheduling JS bundle scan (fallback): {bundle_url}")
                        yield scrapy.Request(
                            bundle_url,
                            callback=self._parse_js_for_api_creds,
                            errback=self._errback,
                            meta={"referer": response.url},
                        )
                    return  # One bundle is enough

        except Exception as exc:
            logger.debug(f"API script detection error at {response.url}: {exc}")

    def _parse_js_for_api_creds(self, response: Response):
        """
        Search a downloaded JS bundle for embedded API credentials.
        Currently detects: Supabase project URL + anon JWT.
        """
        if response.status != 200:
            return

        text = response.text
        logger.info(f"Scanning JS bundle ({len(text)} chars) for API creds: {response.url}")

        referer = response.meta.get("referer", response.url)
        yield from self._try_supabase_from_text(text, referer)

    def _try_supabase_from_text(self, text: str, page_url: str):
        """
        Extract Supabase project URL + anon key from JS/HTML text.
        If found, schedules REST API requests for common public table names.
        """
        # Match Supabase project URL: https://<ref-id>.supabase.co
        # ref IDs are typically 20 chars but allow 10-30 to be safe
        url_match = re.search(r"https://([a-z0-9]{10,30})\.supabase\.co", text)
        # Match JWT anon key.
        # Structure: <header>.<payload>.<signature>
        # Real-world Supabase keys:
        #   header    = eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9  (36 chars, so 33 after "eyJ")
        #   payload   = eyJpc3MiOiJzdXBhYmFzZSIs...            (100+ chars)
        #   signature = GD_HVD-98oUUM9R...                     (40+ chars)
        # The original regex used {50,} for the header — WRONG, header is only ~33 chars.
        # Use optional surrounding quote characters (", ', or backtick from minified JS).
        key_match = re.search(
            r'["\x27`]?(eyJ[A-Za-z0-9_+/\-]{10,}\.[A-Za-z0-9_+/\-]{40,}\.[A-Za-z0-9_+/\-]{10,})["\x27`]?',
            text,
        )

        if not url_match or not key_match:
            return

        supabase_url = f"https://{url_match.group(1)}.supabase.co"
        anon_key = key_match.group(1)
        logger.info(f"Supabase detected! URL={supabase_url} (key length={len(anon_key)})")

        # Try common public table names used by industrial machine sites
        for table in ("machines_public", "machines", "products", "listings",
                      "stock", "inventory", "equipment"):
            api_url = f"{supabase_url}/rest/v1/{table}?select=*&limit=1000"
            if not self._is_visited(api_url):
                self._mark_visited(api_url)
                yield scrapy.Request(
                    api_url,
                    callback=self._parse_supabase_api,
                    errback=self._errback,
                    headers={
                        "apikey": anon_key,
                        "Authorization": f"Bearer {anon_key}",
                        "Accept": "application/json",
                    },
                    meta={"supabase_table": table, "referer": page_url},
                )

    def _parse_supabase_api(self, response: Response):
        """
        Handle a Supabase PostgREST API response.
        Expects a JSON array of machine objects (with RLS-filtered public view).
        """
        table = response.meta.get("supabase_table", "?")

        if response.status == 404 or response.status == 400:
            # Table doesn't exist — silent skip
            return

        if response.status != 200:
            logger.debug(f"Supabase table {table!r} returned {response.status}")
            return

        try:
            data = json.loads(response.text)
        except Exception:
            return

        if not isinstance(data, list):
            # PostgREST error envelope {"code": ..., "message": ...}
            if isinstance(data, dict) and data.get("code"):
                logger.debug(f"Supabase table {table!r} error: {data.get('message')}")
            return

        if not data:
            return

        logger.info(f"Supabase table {table!r}: {len(data)} rows")
        base_url = "{0.scheme}://{0.netloc}".format(urlparse(response.meta.get("referer", response.url)))

        for row in data:
            item = self._json_to_machine_item(row, response.url, base_url)
            if item:
                yield item

    # ═════════════════════════════════════════════════════════════════════════
    # SCALABLE RULE SYSTEM — added for 500-website architecture
    # ═════════════════════════════════════════════════════════════════════════

    # ── URL filtering ─────────────────────────────────────────────────────────

    def _should_skip_url(self, url: str) -> bool:
        """
        Return True if this URL should be skipped entirely.

        Checks:
          1. Built-in skip_url() heuristics (contact, login, assets, etc.)
          2. Site-specific skip_url_patterns from training rules (regex list).
        """
        from zoogle_crawler.page_analyzer import skip_url as _builtin_skip
        if _builtin_skip(url):
            return True
        for pat in self._skip_patterns:
            if pat.search(url):
                return True
        return False

    def _is_product_url(self, url: str) -> bool:
        """
        Return True if this URL looks like a machine/product detail page.

        If product_link_pattern is configured, ONLY URLs matching that regex
        are accepted as detail pages.  Otherwise falls back to the built-in
        is_detail_url() scorer from page_analyzer.
        """
        if self._product_link_re:
            return bool(self._product_link_re.search(url))
        from zoogle_crawler.page_analyzer import is_detail_url
        return is_detail_url(url)

    # ── Content validation ────────────────────────────────────────────────────

    def _validate_item(self, item: MachineItem) -> bool:
        """
        Drop items that are clearly not machines.

        A valid machine item must have:
          • machine_url
          • At least one of: title (model), brand
          • At least one of: image URL, spec dict with entries

        Items that fail this check are silently dropped (not yielded).
        """
        if not item.get("machine_url"):
            return False

        has_identity = bool(item.get("model") or item.get("brand"))
        if not has_identity:
            return False

        has_content = bool(
            (item.get("images") and len(item["images"]) > 0)
            or (item.get("specs") and len(item["specs"]) > 0)
            or item.get("description")
            or item.get("price")
        )
        if not has_content:
            return False

        return True

    # ── Field mapping for REST API responses ──────────────────────────────────

    def _resolve_nested(self, data: dict, dotpath: str):
        """
        Walk a dot-separated path in a nested dict.
        E.g. _resolve_nested(row, "brands.name") → row["brands"]["name"]
        Returns None if any key is missing.
        """
        parts = dotpath.split(".")
        cur = data
        for p in parts:
            if not isinstance(cur, dict):
                return None
            cur = cur.get(p)
        return cur

    def _apply_field_map(self, row: dict, site_base: str) -> MachineItem | None:
        """
        Convert a REST API row to MachineItem using self._field_map.

        field_map_json format (admin-configured):
          {"model_name": "model", "brands.name": "brand", "price_inr": "price", ...}

        Keys are dot-paths in the API row, values are MachineItem field names.
        Falls back to self._json_to_machine_item() for unmapped fields.
        """
        if not self._field_map:
            return self._json_to_machine_item(row, site_base, site_base)

        item = MachineItem()
        item["website_id"]     = self.website_id
        item["website_source"] = self._base_domain
        item["currency"]       = "USD"
        item["specs"]          = {}
        item["images"]         = []

        for api_key, machine_field in self._field_map.items():
            val = self._resolve_nested(row, api_key)
            if val is None:
                continue
            if machine_field in ("price",):
                try:
                    item["price"] = float(str(val).replace(",", "").strip())
                except (ValueError, TypeError):
                    pass
            elif machine_field == "images":
                if isinstance(val, list):
                    item["images"] = [str(v) for v in val if v]
                elif val:
                    item["images"] = [str(val)]
            elif machine_field == "specs":
                if isinstance(val, dict):
                    item["specs"] = val
            elif machine_field in MachineItem.fields:
                item[machine_field] = str(val).strip() if val else None

        # Build machine_url if not provided by map
        if not item.get("machine_url"):
            slug = row.get("slug") or row.get("sku") or row.get("id")
            if slug:
                item["machine_url"] = f"{site_base}/machine/{slug}"
            else:
                return None

        if not self._validate_item(item):
            return None
        return item

    # ── Direct REST API pagination ────────────────────────────────────────────

    def _build_direct_api_requests(self, api_url: str, referer: str):
        """
        Build the first paginated request(s) to a directly-configured REST API.
        Subsequent pages are requested by _parse_direct_api_response() itself.
        """
        import json as _json
        page_size = int(self._rules.get("api_page_size") or 100)
        param     = self._rules.get("api_pagination_param") or "page"
        first_url = f"{api_url}?{param}=1&limit={page_size}" if "?" not in api_url else api_url

        headers = {"Accept": "application/json"}
        _api_key = self._rules.get("api_key")
        if _api_key:
            headers["Authorization"] = f"Bearer {_api_key}"
        _extra_headers_raw = self._rules.get("api_headers_json")
        if _extra_headers_raw:
            try:
                headers.update(_json.loads(_extra_headers_raw))
            except Exception:
                pass

        yield scrapy.Request(
            first_url,
            callback=self._parse_direct_api_response,
            errback=self._errback,
            headers=headers,
            meta={
                "referer": referer,
                "api_page": 1,
                "api_page_size": page_size,
                "api_param": param,
                "api_base_url": api_url,
                "api_headers": headers,
            },
            dont_filter=True,
        )

    def _parse_direct_api_response(self, response):
        """
        Parse a direct REST API response (configured via api_url training rule).

        Handles:
          • JSON arrays at root level
          • Nested JSON at api_data_path (e.g. "data", "results.items")
          • Automatic pagination until empty page is returned
        """
        import json as _json

        if response.status not in (200, 201):
            logger.warning(f"Direct API returned {response.status}: {response.url}")
            return

        try:
            payload = _json.loads(response.text)
        except Exception as exc:
            logger.warning(f"Direct API JSON parse failed: {exc} url={response.url}")
            return

        # Extract results array using configured data path
        data_path = self._rules.get("api_data_path") or ""
        if data_path:
            rows = self._resolve_nested(payload, data_path) if isinstance(payload, dict) else None
        else:
            rows = payload if isinstance(payload, list) else (
                payload.get("results") or payload.get("data") or payload.get("items")
                or payload.get("machines") or payload.get("products")
                if isinstance(payload, dict) else None
            )

        if not rows or not isinstance(rows, list):
            logger.info(f"Direct API: no rows at path={data_path!r} url={response.url}")
            return

        logger.info(f"Direct API: {len(rows)} rows from {response.url}")

        site_base = "{0.scheme}://{0.netloc}".format(urlparse(response.url))

        for row in rows:
            if not isinstance(row, dict):
                continue
            item = self._apply_field_map(row, site_base)
            if item and self._validate_item(item):
                yield item

        # Paginate: if we got a full page, fetch the next one
        page_size = response.meta.get("api_page_size", 100)
        if len(rows) >= page_size:
            page      = response.meta.get("api_page", 1) + 1
            param     = response.meta.get("api_param", "page")
            api_base  = response.meta.get("api_base_url", "")
            headers   = response.meta.get("api_headers", {})

            next_url = f"{api_base}?{param}={page}&limit={page_size}"
            if not self._is_visited(next_url):
                self._mark_visited(next_url)
                yield scrapy.Request(
                    next_url,
                    callback=self._parse_direct_api_response,
                    errback=self._errback,
                    headers=headers,
                    meta={
                        **response.meta,
                        "api_page": page,
                    },
                    dont_filter=True,
                )
