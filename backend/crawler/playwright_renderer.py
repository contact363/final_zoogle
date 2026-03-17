"""
Playwright Page Renderer
========================
Renders JavaScript-heavy pages (React, Vue, Angular, Next.js, Nuxt, etc.)
to fully-resolved HTML that BeautifulSoup can parse.

Features:
  • Sync Playwright API — drop-in, no async required
  • Stealth fingerprint — realistic viewport, UA, locale, timezone
  • Lazy-load trigger — scrolls page to force all content into DOM
  • Network idle wait — waits until XHR/fetch traffic settles
  • Auto-retry — one retry on timeout or crash
  • Graceful fallback — if Playwright not installed, falls back to requests
  • JS-render detection — _needs_playwright(html) tells callers when to use it

Install:
    pip install playwright playwright-stealth
    playwright install chromium

Usage:
    from crawler.playwright_renderer import render_page, needs_playwright

    html = render_page("https://example.com/product/123")
    if not html:
        # fallback to requests
        ...
"""
from __future__ import annotations

import logging
import re
import time
from typing import Optional

logger = logging.getLogger(__name__)

# ── JS framework / SPA markers ────────────────────────────────────────────────

_JS_MARKERS_RE = re.compile(
    r"""
    __NEXT_DATA__        |   # Next.js
    __NUXT__             |   # Nuxt
    __INITIAL_STATE__    |   # many SPAs
    data-reactroot       |   # React
    ng-version           |   # Angular
    _app\.js             |   # Next.js bundle
    _next/static         |   # Next.js static
    nuxt\.js             |   # Nuxt bundle
    vue-app              |   # Vue
    window\.__data__     |   # generic SPA store
    <div\s+id="root"\s*>\s*</div>  |  # empty React root
    <div\s+id="app"\s*>\s*</div>      # empty Vue root
    """,
    re.IGNORECASE | re.VERBOSE,
)

_MIN_LINKS_THRESHOLD = 8   # fewer <a href> than this → likely SPA


def needs_playwright(html: str) -> bool:
    """
    Return True if the raw HTML looks like a JS-rendered SPA that needs
    Playwright to produce useful content.

    Criteria (any one is enough):
      • Contains known JS-framework markers
      • Has fewer than _MIN_LINKS_THRESHOLD <a href> anchors (empty shell)
    """
    if not html:
        return True
    if _JS_MARKERS_RE.search(html):
        return True
    link_count = html.lower().count("<a href")
    return link_count < _MIN_LINKS_THRESHOLD


# ── Playwright availability check ─────────────────────────────────────────────

_PLAYWRIGHT_AVAILABLE: Optional[bool] = None   # None = not checked yet


def _check_playwright() -> bool:
    global _PLAYWRIGHT_AVAILABLE
    if _PLAYWRIGHT_AVAILABLE is None:
        try:
            from playwright.sync_api import sync_playwright  # noqa: F401
            _PLAYWRIGHT_AVAILABLE = True
        except ImportError:
            _PLAYWRIGHT_AVAILABLE = False
            logger.warning(
                "Playwright not installed — JS pages will fall back to raw HTML. "
                "Install: pip install playwright && playwright install chromium"
            )
    return _PLAYWRIGHT_AVAILABLE


# ── Stealth launch args ───────────────────────────────────────────────────────
# These args make Chromium look like a real user browser to anti-bot systems.

_LAUNCH_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--disable-dev-shm-usage",
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-gpu",
    "--disable-infobars",
    "--window-size=1920,1080",
    "--disable-extensions",
    "--ignore-certificate-errors",
    "--lang=en-US",
]

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

_VIEWPORT = {"width": 1920, "height": 1080}

# Resource types to block — speeds up render by skipping unnecessary assets
_BLOCK_RESOURCES = {"font", "media"}   # keep: document, script, xhr, fetch, image, stylesheet


def _apply_stealth(page) -> None:
    """
    Inject JS to mask Playwright/Chromium automation signals.
    Covers: navigator.webdriver, chrome object, permissions API, plugins.
    """
    page.add_init_script("""
        // Hide webdriver
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});

        // Fake chrome object
        window.chrome = {
            runtime: {},
            loadTimes: function(){},
            csi: function(){},
            app: {}
        };

        // Fake plugins (real browsers have plugins)
        Object.defineProperty(navigator, 'plugins', {
            get: () => [1, 2, 3, 4, 5],
        });

        // Fake languages
        Object.defineProperty(navigator, 'languages', {
            get: () => ['en-US', 'en'],
        });

        // Permissions API spoof
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (params) =>
            params.name === 'notifications'
                ? Promise.resolve({ state: Notification.permission })
                : originalQuery(params);
    """)


def _scroll_to_bottom(page, pause: float = 0.4, max_scrolls: int = 12) -> None:
    """
    Scroll the page incrementally to trigger lazy-loaded content.
    Stops when no new height is added (page bottom reached).
    """
    last_height = page.evaluate("document.body.scrollHeight")
    for _ in range(max_scrolls):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(pause)
        new_height = page.evaluate("document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height
    # Scroll back to top so above-the-fold content is visible
    page.evaluate("window.scrollTo(0, 0)")


# ── Main render function ───────────────────────────────────────────────────────

def render_page(
    url: str,
    *,
    timeout_ms: int = 30_000,
    wait_until: str = "networkidle",
    scroll: bool = True,
    headless: bool = True,
    retries: int = 1,
) -> Optional[str]:
    """
    Render a URL with Playwright Chromium and return the fully-resolved HTML.

    Args:
        url         — page to render
        timeout_ms  — navigation timeout in milliseconds (default 30s)
        wait_until  — "networkidle" (default) | "domcontentloaded" | "load"
        scroll      — whether to scroll to trigger lazy content (default True)
        headless    — run browser headless (default True)
        retries     — number of automatic retries on timeout/crash (default 1)

    Returns:
        Rendered HTML string, or None if Playwright is unavailable / fails.
    """
    if not _check_playwright():
        return None

    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

    for attempt in range(retries + 1):
        try:
            with sync_playwright() as pw:
                browser = pw.chromium.launch(
                    headless=headless,
                    args=_LAUNCH_ARGS,
                )
                context = browser.new_context(
                    user_agent=_USER_AGENT,
                    viewport=_VIEWPORT,
                    locale="en-US",
                    timezone_id="America/New_York",
                    # Accept cookies / storage
                    accept_downloads=False,
                    ignore_https_errors=True,
                )

                # Block unnecessary resource types to speed up render
                def _route_handler(route):
                    if route.request.resource_type in _BLOCK_RESOURCES:
                        route.abort()
                    else:
                        route.continue_()

                context.route("**/*", _route_handler)
                page = context.new_page()
                _apply_stealth(page)

                # Extra realistic headers
                page.set_extra_http_headers({
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept": (
                        "text/html,application/xhtml+xml,"
                        "application/xml;q=0.9,image/webp,*/*;q=0.8"
                    ),
                })

                logger.info("[Playwright] Rendering %s (attempt %d)", url, attempt + 1)
                page.goto(url, timeout=timeout_ms, wait_until=wait_until)

                if scroll:
                    _scroll_to_bottom(page)

                html = page.content()
                browser.close()

                logger.info(
                    "[Playwright] Done — %d bytes of HTML from %s", len(html), url
                )
                return html

        except PWTimeout:
            logger.warning(
                "[Playwright] Timeout on %s (attempt %d/%d)", url, attempt + 1, retries + 1
            )
            if attempt < retries:
                # Retry with faster wait strategy
                wait_until = "domcontentloaded"
        except Exception as exc:
            logger.warning(
                "[Playwright] Error on %s (attempt %d/%d): %s",
                url, attempt + 1, retries + 1, exc,
            )
            if attempt >= retries:
                break

    logger.error("[Playwright] All attempts failed for %s", url)
    return None


def render_if_needed(
    url: str,
    static_html: str,
    *,
    force: bool = False,
    timeout_ms: int = 30_000,
) -> str:
    """
    Return Playwright-rendered HTML only when necessary.

    If `static_html` already has enough content (not a JS SPA), returns it
    as-is. Otherwise renders with Playwright.

    Args:
        url         — the page URL (for Playwright navigation)
        static_html — raw HTML already fetched via requests
        force       — skip detection, always use Playwright
        timeout_ms  — Playwright navigation timeout

    Returns:
        Best available HTML string (may be the original static_html if
        Playwright is unavailable or not needed).
    """
    if not force and not needs_playwright(static_html):
        return static_html

    logger.info("[Playwright] JS rendering needed for %s", url)
    rendered = render_page(url, timeout_ms=timeout_ms)
    if rendered:
        return rendered

    logger.warning(
        "[Playwright] Render failed — falling back to static HTML for %s", url
    )
    return static_html
