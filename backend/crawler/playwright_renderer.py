"""
Playwright Page Renderer + Network Interceptor
================================================
Two modes:

  render_page(url)
    → Returns fully-rendered HTML (React/Vue/Angular/Next.js)

  render_and_intercept(url)
    → Returns (html, captured_api_responses)
    → Captures ALL XHR/fetch JSON responses during page load
    → Works even when HTML shell is empty (most SPAs)
    → This is the key fix for sites like corelmachine.com

Install:
    pip install playwright
    playwright install chromium      ← REQUIRED after pip install
"""
from __future__ import annotations

import json
import logging
import re
import time
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ── JS framework / SPA markers ────────────────────────────────────────────────

_JS_MARKERS_RE = re.compile(
    r"__NEXT_DATA__|__NUXT__|__INITIAL_STATE__|data-reactroot|ng-version"
    r"|_app\.js|_next/static|nuxt\.js|vue-app|window\.__data__"
    r'|<div\s+id="root"\s*>\s*</div>|<div\s+id="app"\s*>\s*</div>',
    re.IGNORECASE,
)

_MIN_LINKS_THRESHOLD = 8


def needs_playwright(html: str) -> bool:
    # SSR sites (Next.js SSR, Nuxt SSR) have lots of links even though they
    # contain _next/static etc. Use link count as the real discriminator.
    if not html:
        return True
    return html.lower().count("<a href") < _MIN_LINKS_THRESHOLD


# ── Availability check ─────────────────────────────────────────────────────────

_PLAYWRIGHT_AVAILABLE: Optional[bool] = None


def _check_playwright() -> bool:
    global _PLAYWRIGHT_AVAILABLE
    if _PLAYWRIGHT_AVAILABLE is None:
        try:
            from playwright.sync_api import sync_playwright  # noqa: F401
            _PLAYWRIGHT_AVAILABLE = True
            logger.info("[Playwright] Library available")
        except ImportError:
            _PLAYWRIGHT_AVAILABLE = False
            logger.warning(
                "[Playwright] NOT installed. Run: pip install playwright && playwright install chromium"
            )
    return _PLAYWRIGHT_AVAILABLE


# ── Launch config ─────────────────────────────────────────────────────────────

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

# Skip these resource types to speed up render
_BLOCK_RESOURCES = {"font", "media"}

# API response content types to capture
_JSON_CONTENT_TYPES = ("application/json", "text/json", "application/ld+json")

# Skip these API URLs (analytics, CDN, etc.)
_SKIP_API_HOSTS = re.compile(
    r"google-analytics|googletagmanager|facebook\.net|hotjar|"
    r"sentry\.io|bugsnag|amplitude|segment\.io|mixpanel|"
    r"fonts\.googleapis|cloudflare|cdn\.",
    re.IGNORECASE,
)

# ── Last-error storage so callers can surface Playwright errors to UI ─────────
_last_playwright_error: str = ""


def get_last_error() -> str:
    return _last_playwright_error


def playwright_check() -> tuple:
    """
    Verify Playwright + Chromium are properly installed.
    Returns (ok: bool, message: str).
    Run this to diagnose why rendering fails.
    """
    if not _check_playwright():
        return False, "playwright Python package not installed. Run: pip install playwright"
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True, args=["--no-sandbox"])
            page = browser.new_page()
            page.goto("about:blank")
            title = page.title()
            browser.close()
        return True, f"Playwright OK — Chromium launched, got title={title!r}"
    except Exception as exc:
        msg = str(exc)
        if "Executable doesn't exist" in msg or "executable" in msg.lower():
            return False, (
                f"Chromium not downloaded: {msg}\n"
                "Fix: run  playwright install chromium"
            )
        return False, f"Playwright launch failed: {msg}"


def _stealth_js() -> str:
    return """
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        window.chrome = {runtime:{}, loadTimes:function(){}, csi:function(){}, app:{}};
        Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
        Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
        const _pq = window.navigator.permissions.query;
        window.navigator.permissions.query = (p) =>
            p.name === 'notifications'
                ? Promise.resolve({state: Notification.permission})
                : _pq(p);
    """


def _scroll_to_bottom(page, pause: float = 0.3, max_scrolls: int = 10) -> None:
    last = page.evaluate("document.body.scrollHeight")
    for _ in range(max_scrolls):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(pause)
        new = page.evaluate("document.body.scrollHeight")
        if new == last:
            break
        last = new
    page.evaluate("window.scrollTo(0, 0)")


# ── Core render function ───────────────────────────────────────────────────────

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
    Render URL with Playwright Chromium. Returns HTML or None on failure.
    Logs the actual error so you know exactly why it failed.
    """
    if not _check_playwright():
        return None

    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

    for attempt in range(retries + 1):
        try:
            with sync_playwright() as pw:
                browser = pw.chromium.launch(headless=headless, args=_LAUNCH_ARGS)
                ctx = browser.new_context(
                    user_agent=_USER_AGENT,
                    viewport=_VIEWPORT,
                    locale="en-US",
                    timezone_id="America/New_York",
                    ignore_https_errors=True,
                )
                ctx.route(
                    "**/*",
                    lambda r: r.abort() if r.request.resource_type in _BLOCK_RESOURCES
                    else r.continue_(),
                )
                page = ctx.new_page()
                page.add_init_script(_stealth_js())
                page.set_extra_http_headers({"Accept-Language": "en-US,en;q=0.9"})

                logger.info("[Playwright] Rendering %s (attempt %d, wait=%s)",
                            url, attempt + 1, wait_until)
                page.goto(url, timeout=timeout_ms, wait_until=wait_until)

                if scroll:
                    _scroll_to_bottom(page)

                html = page.content()
                browser.close()
                logger.info("[Playwright] Success — %d bytes from %s", len(html), url)
                return html

        except PWTimeout:
            logger.warning("[Playwright] Timeout on %s (attempt %d)", url, attempt + 1)
            wait_until = "domcontentloaded"
        except Exception as exc:
            global _last_playwright_error
            _last_playwright_error = str(exc)
            logger.error("[Playwright] Error on %s (attempt %d): %s", url, attempt + 1, exc)
            if "Executable doesn't exist" in str(exc) or "executable" in str(exc).lower():
                logger.error(
                    "[Playwright] Chromium not installed! Fix: playwright install chromium"
                )
            if attempt >= retries:
                break

    return None


# ── Network interception — the KEY fix for blank SPAs ─────────────────────────

def render_and_intercept(
    url: str,
    *,
    timeout_ms: int = 30_000,
    extra_paths: Optional[List[str]] = None,
) -> Tuple[str, List[Dict[str, Any]]]:
    """
    Render a page AND capture all XHR/fetch JSON responses.

    This is the core fix for sites like corelmachine.com:
    - The HTML shell may be completely empty (React root div only)
    - BUT the browser makes many API calls to load product data
    - We intercept those responses and extract machine data from them

    Returns:
        (html, api_responses)
        html          — fully rendered page HTML
        api_responses — list of {"url": ..., "data": ...} for each JSON response
    """
    if not _check_playwright():
        logger.warning("[Playwright] Not available — returning empty results")
        return "", []

    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

    captured: List[Dict[str, Any]] = []
    html = ""

    def _on_response(response) -> None:
        """Capture JSON responses from API calls."""
        try:
            if _SKIP_API_HOSTS.search(response.url):
                return
            ct = response.headers.get("content-type", "")
            if not any(jct in ct for jct in _JSON_CONTENT_TYPES):
                return
            if response.status < 200 or response.status >= 300:
                return
            body = response.json()
            if body and isinstance(body, (dict, list)):
                captured.append({"url": response.url, "data": body})
                logger.debug("[Playwright/XHR] Captured %s (%s)",
                             response.url, type(body).__name__)
        except Exception:
            pass   # ignore parse failures silently

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True, args=_LAUNCH_ARGS)
            ctx = browser.new_context(
                user_agent=_USER_AGENT,
                viewport=_VIEWPORT,
                locale="en-US",
                timezone_id="America/New_York",
                ignore_https_errors=True,
            )
            # Block only fonts/media; allow scripts, XHR, fetch
            ctx.route(
                "**/*",
                lambda r: r.abort() if r.request.resource_type in _BLOCK_RESOURCES
                else r.continue_(),
            )
            page = ctx.new_page()
            page.add_init_script(_stealth_js())
            page.set_extra_http_headers({"Accept-Language": "en-US,en;q=0.9"})

            # Attach response listener BEFORE navigation
            page.on("response", _on_response)

            logger.info("[Playwright/XHR] Loading %s", url)
            try:
                page.goto(url, timeout=timeout_ms, wait_until="networkidle")
            except PWTimeout:
                logger.warning("[Playwright/XHR] Timeout on %s — using domcontentloaded", url)
                try:
                    page.goto(url, timeout=timeout_ms, wait_until="domcontentloaded")
                    page.wait_for_timeout(3000)   # give JS 3s to fire XHR calls
                except Exception:
                    pass

            # Scroll to trigger lazy-loaded API calls
            _scroll_to_bottom(page)
            # Extra wait after scroll to let triggered XHR complete
            page.wait_for_timeout(2000)

            html = page.content()

            # Also visit extra paths to capture product-listing API calls
            if extra_paths:
                from urllib.parse import urlparse
                base = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
                for path in extra_paths[:5]:
                    try:
                        page.goto(base + path, timeout=15_000,
                                  wait_until="domcontentloaded")
                        page.wait_for_timeout(2000)
                        _scroll_to_bottom(page, max_scrolls=4)
                        page.wait_for_timeout(1500)
                    except Exception:
                        pass

            browser.close()

    except Exception as exc:
        logger.error("[Playwright/XHR] Fatal error on %s: %s", url, exc)

    logger.info(
        "[Playwright/XHR] Done — html=%d bytes, captured=%d API responses",
        len(html), len(captured),
    )
    return html, captured


def render_if_needed(
    url: str,
    static_html: str,
    *,
    force: bool = False,
    timeout_ms: int = 30_000,
) -> str:
    if not force and not needs_playwright(static_html):
        return static_html
    logger.info("[Playwright] JS rendering needed for %s", url)
    rendered = render_page(url, timeout_ms=timeout_ms)
    if rendered:
        return rendered
    logger.warning("[Playwright] Render failed — falling back to static HTML for %s", url)
    return static_html
