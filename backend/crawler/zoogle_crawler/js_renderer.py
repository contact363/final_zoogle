"""
js_renderer.py — Optional Playwright fallback for JS-heavy pages.

Design goals
────────────
• Memory-safe on Render's 512 MB free tier:
  - Launches one fresh subprocess per render → process exits + frees RAM.
  - Blocks images / fonts / media inside the browser.
  - Hard timeout (default 25 s) prevents zombie processes.

• Graceful degradation:
  - Returns None if Playwright is not installed (pip install playwright).
  - Returns None on any error so the caller can fall back to raw HTML.

• Detection heuristics:
  - is_js_page(html) decides whether a page needs rendering at all.

Usage in spider
───────────────
    from zoogle_crawler.js_renderer import JSRenderer, is_js_page

    if is_js_page(response.text):
        rendered = JSRenderer.render_sync(response.url)
        if rendered:
            fake_response = response.replace(body=rendered.encode())
            yield from self._parse_detail_css(fake_response)
"""

import re
import sys
import subprocess
import logging
import textwrap

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Heuristics to decide if a page needs JS rendering
# ─────────────────────────────────────────────────────────────────────────────

# SPA skeleton patterns — page was rendered on the client side only
_EMPTY_APP_PATTERNS = [
    r'<div\s+id=["\'](?:app|root|__next|__nuxt)["\']>\s*</div>',
    r'<div\s+id=["\'](?:app|root|__next)["\'][^>]*>\s*</div>',
]

# Signals that indicate JS is driving the content
_JS_FRAMEWORK_SIGNALS = [
    "window.__NUXT__",
    "__NEXT_DATA__",
    "ng-version=",             # Angular
    "data-reactroot",          # React
    "data-vue-app",            # Vue 3
    "_vue-app",
]

# Very common "loading" placeholders shown before JS runs
_LOADING_SIGNALS = [
    "Loading...",
    "Please wait",
    "Initializing",
]


def is_js_page(html: str) -> bool:
    """
    Return True when the page body looks like it was served without content
    (i.e., the browser must execute JavaScript to populate it).
    """
    if not html:
        return True

    # Strip tags and measure visible text length
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"\s+", " ", text).strip()

    # Very sparse: likely a SPA that hasn't rendered yet
    if len(text) < 300:
        return True

    # Empty SPA container pattern
    for pattern in _EMPTY_APP_PATTERNS:
        if re.search(pattern, html, re.IGNORECASE):
            return True

    # Contains a JS framework global that drives the content
    for sig in _JS_FRAMEWORK_SIGNALS:
        if sig in html:
            return True

    return False


# ─────────────────────────────────────────────────────────────────────────────
# Playwright renderer  (subprocess-isolated for memory safety)
# ─────────────────────────────────────────────────────────────────────────────

# The script we execute in a child process — kept as a string so we don't need
# a separate file and so it's easy to read.
_RENDER_SCRIPT = textwrap.dedent("""
import sys, asyncio

async def main(url, timeout_ms):
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        sys.exit(2)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--single-process",          # critical for 512 MB RAM
                "--no-first-run",
                "--no-zygote",
                "--disable-extensions",
                "--disable-background-networking",
                "--disable-sync",
                "--disable-translate",
                "--disable-notifications",
            ],
        )
        ctx = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
            # Block images / fonts / media to save bandwidth + RAM
            java_script_enabled=True,
        )
        # Block heavy resources
        async def block_heavy(route):
            if route.request.resource_type in ("image", "media", "font", "stylesheet"):
                await route.abort()
            else:
                await route.continue_()

        page = await ctx.new_page()
        await page.route("**/*", block_heavy)

        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=int(timeout_ms))
            # Allow lazy-loaded content a moment to settle
            await page.wait_for_timeout(1500)
            html = await page.content()
            # Write HTML to stdout for the parent process to read
            sys.stdout.buffer.write(html.encode("utf-8", errors="replace"))
        except Exception as exc:
            sys.stderr.write(f"playwright_render_error: {exc}\\n")
            sys.exit(1)
        finally:
            await page.close()
            await ctx.close()
            await browser.close()

url   = sys.argv[1]
toms  = int(sys.argv[2]) if len(sys.argv) > 2 else 20000
asyncio.run(main(url, toms))
""")


class JSRenderer:
    """
    Renders a URL with a headless Chromium browser (Playwright) in a
    subprocess and returns the fully-rendered HTML string.

    The subprocess approach isolates memory: once the render finishes the
    child process exits and the RAM is returned to the OS, keeping the main
    Scrapy process lean.
    """

    @staticmethod
    def render_sync(url: str, timeout_s: int = 25) -> str | None:
        """
        Synchronously render *url* and return the rendered HTML.

        Parameters
        ----------
        url : str
            The full URL to render.
        timeout_s : int
            Wall-clock timeout for the entire render subprocess (seconds).

        Returns
        -------
        str | None
            Rendered HTML, or None if Playwright is unavailable / errored.
        """
        try:
            result = subprocess.run(
                [sys.executable, "-c", _RENDER_SCRIPT, url, str(timeout_s * 800)],
                capture_output=True,
                timeout=timeout_s,
            )

            if result.returncode == 2:
                logger.debug("Playwright not installed — skipping JS render")
                return None

            if result.returncode != 0:
                err = result.stderr.decode("utf-8", errors="replace")[-500:]
                logger.warning(f"JS render failed for {url}: {err}")
                return None

            html = result.stdout.decode("utf-8", errors="replace")
            if len(html) < 200:
                return None

            logger.debug(f"JS render OK for {url} ({len(html):,} bytes)")
            return html

        except subprocess.TimeoutExpired:
            logger.warning(f"JS render timeout ({timeout_s}s) for {url}")
            return None
        except FileNotFoundError:
            logger.debug("Python interpreter not found for JS render subprocess")
            return None
        except Exception as exc:
            logger.error(f"JS render unexpected error for {url}: {exc}")
            return None

    @staticmethod
    def is_available() -> bool:
        """Returns True if Playwright is importable in the current environment."""
        try:
            import importlib
            return importlib.util.find_spec("playwright") is not None
        except Exception:
            return False
