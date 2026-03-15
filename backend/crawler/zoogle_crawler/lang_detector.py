"""
lang_detector.py — Language detection for multi-language industrial machine sites.

Problem
───────
Many machine dealer websites publish the same machine in several languages:
  English:  /used-machines/cnc-lathe-mazak-qt28
  German:   /gebrauchtmaschinen/cnc-drehmaschine-mazak-qt28
  Italian:  /macchine-usate/tornio-cnc-mazak-qt28

All three pages represent the SAME machine.  We want ONLY the English version.

Detection strategy (layered, fast to slow)
──────────────────────────────────────────
  1. URL path prefix  — /de/, /it/, /fr-ch/  (cheapest, no HTML needed)
  2. Subdomain        — de.example.com, it.example.com
  3. html[lang]       — <html lang="de"> (one CSS select)
  4. hreflang         — <link rel="alternate" hreflang="en" href="...">
                        If the page has an English hreflang, crawl that instead.
  5. meta content-language — <meta http-equiv="content-language" content="de">
  6. Default          — allow (assume English if no signal found)

Public API
──────────
  detect_language(url, response=None) → str   e.g. "de", "en", "it", "unknown"
  is_english_page(url, response=None) → bool
  get_english_alternate(response)     → str | None   (hreflang canonical URL)
"""

import re
from urllib.parse import urlparse
from typing import Optional

# ---------------------------------------------------------------------------
# Language code sets
# ---------------------------------------------------------------------------

# Non-English ISO 639-1 codes (and common subtags) seen on machine dealer sites
NON_ENGLISH_CODES: frozenset[str] = frozenset({
    # Germanic
    "de", "de-de", "de-at", "de-ch",
    # Romance
    "fr", "fr-fr", "fr-be", "fr-ch", "fr-ca",
    "it", "it-it", "it-ch",
    "es", "es-es", "es-mx", "es-ar",
    "pt", "pt-pt", "pt-br",
    "ro", "ro-ro",
    "ca",
    # Slavic
    "pl", "pl-pl",
    "cs", "cs-cz",
    "sk", "sk-sk",
    "hu", "hu-hu",
    "ru", "ru-ru",
    "uk", "uk-ua",
    "bg", "bg-bg",
    "hr", "sr", "sl",
    # Nordic
    "sv", "sv-se",
    "no", "nb", "nn", "nb-no",
    "da", "da-dk",
    "fi", "fi-fi",
    # Asian
    "zh", "zh-cn", "zh-tw", "zh-hk",
    "ja", "ja-jp",
    "ko", "ko-kr",
    # Middle East / other
    "tr", "tr-tr",
    "ar", "he", "fa",
    "nl", "nl-nl", "nl-be",
    "el", "el-gr",
    "lt", "lv", "et",
})

ENGLISH_CODES: frozenset[str] = frozenset({
    "en", "en-us", "en-gb", "en-au", "en-ca", "en-nz", "en-ie",
})

# ---------------------------------------------------------------------------
# URL-path language prefix detection
# ---------------------------------------------------------------------------

# Matches /de/, /de-DE/, /de_DE/, /fr/, /it/ etc. at the START of the path
# Also matches subpaths like /gebraucht/ (German for "used") as a hint
_LANG_PREFIX_RE = re.compile(
    r"^/([a-z]{2}(?:[_-][a-zA-Z]{2,4})?)/",
    re.IGNORECASE,
)

# Longer German-language path segments that clearly indicate a German page
_GERMAN_PATH_WORDS: frozenset[str] = frozenset({
    "gebrauchtmaschinen", "gebraucht", "maschinen", "angebote", "angebot",
    "produkte", "produkt", "haendler", "occasion", "maschinenpark",
    "drehmaschine", "fraesmaschine", "schleifmaschine", "bearbeitungszentrum",
    "abkantpresse", "stanzmaschine", "bandsaege", "kreissaege", "werkzeugmaschinen",
})

_ITALIAN_PATH_WORDS: frozenset[str] = frozenset({
    "macchine-usate", "macchine", "usato", "tornio", "fresatrice",
    "rettificatrice", "annunci", "offerte", "macchinari",
})

_FRENCH_PATH_WORDS: frozenset[str] = frozenset({
    "machines-occasion", "occasion", "fraiseuse", "tour-cnc", "annonces",
    "rectifieuse", "centre-dusinage",
})

_SPANISH_PATH_WORDS: frozenset[str] = frozenset({
    "maquinaria", "torno", "fresadora", "rectificadora", "ocasion",
    "maquinas-usadas", "equipos",
})

# Subdomain language codes (e.g. de.example.com, it.example.com)
_SUBDOMAIN_RE = re.compile(r"^([a-z]{2})(?:-[a-z]{2})?\.(?!com|net|org|io|co)", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Core detection
# ---------------------------------------------------------------------------

def detect_language(url: str, response=None) -> str:
    """
    Detect the primary language of a page.

    Returns an ISO 639-1 code (lower-cased, e.g. "de", "en", "it")
    or "unknown" when no signal is found.

    Checks are ordered from cheapest (URL) to most expensive (HTML parse).
    """
    url_lower = url.lower()
    parsed    = urlparse(url_lower)
    path      = parsed.path

    # ── 1. URL path prefix ────────────────────────────────────────────────────
    m = _LANG_PREFIX_RE.match(path)
    if m:
        code = m.group(1).lower().replace("_", "-")
        if code in NON_ENGLISH_CODES:
            return code.split("-")[0]          # normalise de-de → de
        if code in ENGLISH_CODES:
            return "en"

    # ── 2. Language path words (long German / Italian / French / Spanish) ─────
    path_parts = set(path.strip("/").lower().split("/"))
    if path_parts & _GERMAN_PATH_WORDS:
        return "de"
    if path_parts & _ITALIAN_PATH_WORDS:
        return "it"
    if path_parts & _FRENCH_PATH_WORDS:
        return "fr"
    if path_parts & _SPANISH_PATH_WORDS:
        return "es"

    # ── 3. Subdomain language code ────────────────────────────────────────────
    netloc = parsed.netloc.lstrip("www.").lower()
    sd_m = _SUBDOMAIN_RE.match(netloc)
    if sd_m:
        code = sd_m.group(1).lower()
        if code in NON_ENGLISH_CODES:
            return code
        if code in ENGLISH_CODES:
            return "en"

    if response is None:
        return "unknown"

    # ── 4. html[lang] attribute ───────────────────────────────────────────────
    html_lang = _get_html_lang(response)
    if html_lang:
        return html_lang

    # ── 5. meta http-equiv="content-language" ────────────────────────────────
    meta_lang = _get_meta_lang(response)
    if meta_lang:
        return meta_lang

    return "unknown"


def is_english_page(url: str, response=None) -> bool:
    """
    Return True if the page is English (or language-neutral).

    A page is treated as English when:
      • detect_language returns "en" or "unknown"
      • No non-English signal is found in URL or HTML

    Non-English pages are skipped by the LanguageFilterPipeline.
    """
    lang = detect_language(url, response)
    return lang in ("en", "unknown")


def get_english_alternate(response) -> Optional[str]:
    """
    If the page declares hreflang alternatives, return the English canonical URL.

    <link rel="alternate" hreflang="en"    href="https://example.com/used-machines/mazak-qt28">
    <link rel="alternate" hreflang="en-us" href="https://example.com/en/mazak-qt28">

    Returns None if no English alternate is declared.
    """
    if response is None:
        return None

    try:
        for sel in response.css('link[rel="alternate"][hreflang]'):
            hreflang = (sel.attrib.get("hreflang") or "").lower().strip()
            href     = (sel.attrib.get("href") or "").strip()
            if hreflang in ENGLISH_CODES and href.startswith("http"):
                return href
    except Exception:
        pass

    return None


def get_stock_number_from_url(url: str) -> Optional[str]:
    """
    Extract a stock/reference number from common URL patterns.

    Supports:
      /machines/A1234
      /listing/REF-5678
      /stock/ST-9012
      ?id=A1234
      ?stock=ST-9012
      ?sku=ABC-123
    """
    path = urlparse(url).path.rstrip("/")
    # Last path segment: if it looks like a stock number
    last = path.split("/")[-1] if "/" in path else ""
    if re.match(r"^[A-Z0-9][A-Z0-9_\-]{2,30}$", last, re.I):
        return last.upper()

    # Query params
    from urllib.parse import parse_qs
    qs = parse_qs(urlparse(url).query)
    for key in ("id", "stock", "sku", "ref", "nr", "no", "item"):
        vals = qs.get(key) or qs.get(key.upper())
        if vals and vals[0]:
            return vals[0].upper()

    return None


# ---------------------------------------------------------------------------
# HTML helpers (private)
# ---------------------------------------------------------------------------

def _get_html_lang(response) -> Optional[str]:
    """Extract lang from <html lang="...">."""
    try:
        lang = response.css("html::attr(lang)").get("")
        if not lang:
            lang = response.css("html::attr(xml:lang)").get("")
        if lang:
            code = lang.strip().lower().split("-")[0]
            if code in NON_ENGLISH_CODES:
                return code
            if code in ENGLISH_CODES or code == "en":
                return "en"
    except Exception:
        pass
    return None


def _get_meta_lang(response) -> Optional[str]:
    """Extract lang from <meta http-equiv='content-language' content='de'>."""
    try:
        content = response.css(
            "meta[http-equiv='content-language']::attr(content),"
            "meta[http-equiv='Content-Language']::attr(content)"
        ).get("")
        if not content:
            # <meta name="language" content="de">
            content = response.css(
                "meta[name='language']::attr(content),"
                "meta[name='Language']::attr(content)"
            ).get("")
        if content:
            code = content.strip().lower().split("-")[0]
            if code in NON_ENGLISH_CODES:
                return code
            if code in ENGLISH_CODES or code == "en":
                return "en"
    except Exception:
        pass
    return None
