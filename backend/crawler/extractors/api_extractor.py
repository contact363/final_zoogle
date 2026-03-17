"""
API Extractor — Phase 1 helper.

API-FIRST architecture:
  1. Scan page source for API/JSON endpoints (/api/, /rest/, /graphql, .json)
  2. Parse JS framework data stores (window.__NEXT_DATA__, __NUXT__, etc.)
  3. Try known API patterns (Supabase, Shopify, WooCommerce, REST)
  4. Full pagination with total-count validation

HTML scanning is only used when no API is found.
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests

logger = logging.getLogger(__name__)

# ── Data structures ───────────────────────────────────────────────────────────

@dataclass
class APIConfig:
    api_type: str            # supabase | rest | shopify | woocommerce | graphql | configured
    endpoint: str            # base API URL
    headers: Dict[str, str] = field(default_factory=dict)
    data_path: str = ""      # dot-notation path to items array
    pagination_param: str = "page"
    page_size: int = 100
    field_map: Dict[str, str] = field(default_factory=dict)
    # Cursor-based pagination
    cursor_field: str = ""       # response field containing next cursor
    # Total count extraction (for validation)
    total_count_path: str = ""   # dot-notation path to total count in response
    # URL building from API items
    url_slug_field: str = ""     # item field to use as URL slug
    url_id_field: str = ""       # item field to use as URL ID
    url_template: str = ""       # e.g. "/products/{slug}"
    # Base website URL (for building product URLs)
    base_url: str = ""


@dataclass
class APIDetectionResult:
    found: bool
    config: Optional[APIConfig] = None
    sample_count: int = 0
    error: Optional[str] = None


# ── Keyword patterns for machine detection in API responses ───────────────────

MACHINE_KEYWORDS = {
    "machine", "machinery", "equipment", "tool", "device",
    "maschine", "werkzeug", "geraet",
    "macchina", "attrezzatura",
    "maquina", "equipo",
    "machine", "équipement",
    "machine", "apparaat",
}

# ── Internal HTTP session ─────────────────────────────────────────────────────

def _session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
    s.headers["Accept"] = "application/json, text/html, */*"
    return s


# ── HTML source scanning for API endpoints ───────────────────────────────────

# Patterns that reveal API endpoints in page source / JS bundles
_API_SRC_PATTERNS = [
    # fetch/axios/xhr calls
    re.compile(r"""(?:fetch|axios\.get|axios\.post|\.get|\.post)\s*\(\s*["'`]([^"'`]+)["'`]"""),
    # url: "...", endpoint: "..."
    re.compile(r"""(?:url|endpoint|apiUrl|api_url)\s*[=:]\s*["'`]([^"'`]+)["'`]"""),
    # string literals containing /api/ or /rest/ or .json
    re.compile(r"""["'`]((?:https?://[^"'`]+)?(?:/api/|/rest/|/graphql)[^"'`\s]{3,})["'`]"""),
    re.compile(r"""["'`]([^"'`\s]+\.json(?:\?[^"'`\s]*)?)["'`]"""),
]

_SKIP_API_PATTERNS = re.compile(
    r"(?:\.css|\.js|\.png|\.jpg|\.svg|\.ico|/wp-json/wp/v2/(?:posts|pages|comments|users)|"
    r"schema\.org|cdnjs|googleapis|gstatic|jquery|bootstrap|fontawesome)",
    re.IGNORECASE,
)


def _detect_from_html_sources(
    base_url: str, html: str, session: requests.Session
) -> Optional[APIConfig]:
    """
    Scan page HTML and referenced JS bundles for XHR/fetch calls to API endpoints.
    Returns an APIConfig if a working JSON endpoint is found.
    """
    candidates: List[str] = []

    def _collect_from_text(text: str) -> None:
        for pat in _API_SRC_PATTERNS:
            for m in pat.finditer(text):
                url = m.group(1).strip()
                if _SKIP_API_PATTERNS.search(url):
                    continue
                abs_url = urljoin(base_url, url) if url.startswith("/") else url
                if abs_url.startswith("http") and abs_url not in candidates:
                    candidates.append(abs_url)

    _collect_from_text(html)

    # Also probe first few JS bundles
    js_srcs = re.findall(r'src=["\']([^"\']+\.js[^"\']*)["\']', html)
    for js_rel in js_srcs[:6]:
        try:
            resp = session.get(urljoin(base_url, js_rel), timeout=12)
            if resp.ok and len(resp.text) < 500_000:
                _collect_from_text(resp.text)
        except Exception:
            continue

    # Probe each candidate — accept first that returns a JSON list/object with items
    for url in candidates[:20]:
        try:
            r = session.get(url, timeout=8)
            if r.status_code != 200:
                continue
            ct = r.headers.get("content-type", "")
            if "json" not in ct and not url.endswith(".json"):
                continue
            data = r.json()
            items, data_path = _find_items_in_response(data)
            if items and len(items) > 0:
                logger.info("[API] detected via HTML source scan: %s", url)
                return APIConfig(
                    api_type="rest",
                    endpoint=url,
                    data_path=data_path,
                    pagination_param="page",
                    page_size=100,
                    base_url=base_url,
                )
        except Exception:
            continue

    return None


def _find_items_in_response(data: Any) -> Tuple[Optional[List], str]:
    """Return (items_list, data_path) from an API response, or (None, '')."""
    if isinstance(data, list) and len(data) > 0:
        return data, ""
    if isinstance(data, dict):
        for key in ("data", "results", "items", "products", "machines",
                    "listings", "equipment", "records", "entries"):
            val = data.get(key)
            if isinstance(val, list) and len(val) > 0:
                return val, key
    return None, ""


# ── GraphQL detection ─────────────────────────────────────────────────────────

_GQL_ENDPOINTS = ["/graphql", "/api/graphql", "/gql", "/query"]
_GQL_INTROSPECT = '{"query":"{ __typename }"}'
_GQL_PRODUCTS_QUERY = '{"query":"{ products(first:1) { edges { node { id title } } } }"}'


def _detect_graphql(base_url: str, session: requests.Session) -> Optional[APIConfig]:
    """Try common GraphQL endpoints. Returns APIConfig if one responds."""
    hdrs = {"Content-Type": "application/json", "Accept": "application/json"}
    for path in _GQL_ENDPOINTS:
        url = urljoin(base_url, path)
        try:
            r = session.post(url, data=_GQL_INTROSPECT, headers=hdrs, timeout=8)
            if r.status_code == 200 and "data" in (r.json() or {}):
                logger.info("[API] GraphQL endpoint detected: %s", url)
                return APIConfig(
                    api_type="graphql",
                    endpoint=url,
                    headers=hdrs,
                    data_path="data",
                    pagination_param="cursor",
                    page_size=100,
                    cursor_field="pageInfo.endCursor",
                    base_url=base_url,
                )
        except Exception:
            continue
    return None


# ── Total count extraction ────────────────────────────────────────────────────

_TOTAL_KEYS = [
    "total", "totalCount", "total_count", "count", "totalItems", "total_items",
    "totalResults", "total_results", "num_found", "numFound", "productCount",
    "machineCount", "pagination.total", "meta.total",
]


def _extract_total_count(data: Any) -> int:
    """Extract a total-item-count from an API response (top level or nested)."""
    if not isinstance(data, dict):
        return 0
    for key in _TOTAL_KEYS:
        # Support dot-notation like "pagination.total"
        parts = key.split(".")
        val = data
        for part in parts:
            if isinstance(val, dict):
                val = val.get(part)
            else:
                val = None
                break
        if isinstance(val, int) and val > 0:
            return val
    return 0


# ── Product URL building from API items ───────────────────────────────────────

_URL_FIELDS = ("url", "link", "href", "product_url", "machine_url", "permalink",
               "canonical_url", "page_url", "detail_url")
_SLUG_FIELDS = ("slug", "handle", "url_key", "url_slug", "path")
_ID_FIELDS   = ("id", "ID", "productId", "product_id", "machineId", "machine_id", "sku")


def _build_product_url(item: dict, base_url: str, config: APIConfig) -> str:
    """
    Build a product/machine page URL from an API item.
    Priority: direct URL field → slug field → ID field.
    """
    # 1. Direct URL in item
    for key in _URL_FIELDS:
        val = item.get(key)
        if val and isinstance(val, str):
            if val.startswith("http"):
                return val
            if val.startswith("/"):
                return urljoin(base_url or config.base_url, val)

    origin = _origin(base_url or config.base_url)

    # 2. URL template (e.g. "/products/{slug}")
    if config.url_template:
        try:
            return origin + config.url_template.format(**item)
        except (KeyError, ValueError):
            pass

    # 3. Slug field
    slug_field = config.url_slug_field or ""
    slug = (item.get(slug_field) if slug_field else None) or _first(item, _SLUG_FIELDS)
    if slug:
        for prefix in ("/products", "/machines", "/equipment", "/items", "/listings"):
            return f"{origin}{prefix}/{slug}"

    # 4. ID field
    id_field = config.url_id_field or ""
    item_id = (item.get(id_field) if id_field else None) or _first(item, _ID_FIELDS)
    if item_id:
        for prefix in ("/products", "/machines", "/equipment"):
            return f"{origin}{prefix}/{item_id}"

    return ""


def _origin(url: str) -> str:
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}"


def _first(d: dict, keys: tuple) -> Any:
    for k in keys:
        v = d.get(k)
        if v is not None:
            return v
    return None


# ── Supabase detection ────────────────────────────────────────────────────────

_SUPABASE_URL_RE  = re.compile(r'https://[a-z0-9]+\.supabase\.co', re.IGNORECASE)
_SUPABASE_KEY_RE  = re.compile(r'eyJ[A-Za-z0-9_\-]{50,}')   # JWT anon key
_SUPABASE_TABLE_CANDIDATES = [
    "machines", "products", "listings", "items",
    "equipment", "maschinen", "produits",
]


def _detect_supabase(base_url: str, html: str, session: requests.Session) -> Optional[APIConfig]:
    """Scan the page HTML (and referenced JS bundles) for Supabase credentials."""
    sb_urls  = _SUPABASE_URL_RE.findall(html)
    sb_keys  = _SUPABASE_KEY_RE.findall(html)

    # If not in inline HTML, probe JS bundles
    if not (sb_urls and sb_keys):
        js_urls = re.findall(r'src=["\']([^"\']+\.js[^"\']*)["\']', html)
        for js_rel in js_urls[:8]:  # limit to first 8 bundles
            js_url = urljoin(base_url, js_rel)
            try:
                resp = session.get(js_url, timeout=15)
                if resp.ok:
                    sb_urls  += _SUPABASE_URL_RE.findall(resp.text)
                    sb_keys  += _SUPABASE_KEY_RE.findall(resp.text)
            except Exception:
                continue

    if not sb_urls or not sb_keys:
        return None

    sb_project_url = sb_urls[0].rstrip("/")
    anon_key = sb_keys[0]
    headers = {
        "apikey": anon_key,
        "Authorization": f"Bearer {anon_key}",
        "Accept": "application/json",
    }

    # Probe tables to find the machine/product table
    for table in _SUPABASE_TABLE_CANDIDATES:
        probe_url = f"{sb_project_url}/rest/v1/{table}?select=*&limit=1"
        try:
            r = session.get(probe_url, headers=headers, timeout=10)
            if r.status_code == 200 and r.json():
                logger.info("Supabase table found: %s", table)
                return APIConfig(
                    api_type="supabase",
                    endpoint=f"{sb_project_url}/rest/v1/{table}",
                    headers=headers,
                    data_path="",
                    pagination_param="offset",
                    page_size=1000,
                )
        except Exception:
            continue

    return None


# ── Shopify detection ─────────────────────────────────────────────────────────

def _detect_shopify(base_url: str, session: requests.Session) -> Optional[APIConfig]:
    url = urljoin(base_url, "/products.json?limit=1")
    try:
        r = session.get(url, timeout=10)
        if r.status_code == 200:
            data = r.json()
            if "products" in data and isinstance(data["products"], list):
                return APIConfig(
                    api_type="shopify",
                    endpoint=urljoin(base_url, "/products.json"),
                    data_path="products",
                    pagination_param="page",
                    page_size=250,
                    base_url=base_url,
                )
    except Exception:
        pass
    return None


# ── WooCommerce detection ─────────────────────────────────────────────────────

def _detect_woocommerce(base_url: str, session: requests.Session) -> Optional[APIConfig]:
    url = urljoin(base_url, "/wp-json/wc/v3/products?per_page=1")
    try:
        r = session.get(url, timeout=10)
        if r.status_code == 200 and isinstance(r.json(), list):
            # WooCommerce puts total in X-WP-Total header
            return APIConfig(
                api_type="woocommerce",
                endpoint=urljoin(base_url, "/wp-json/wc/v3/products"),
                data_path="",
                pagination_param="page",
                page_size=100,
                base_url=base_url,
            )
    except Exception:
        pass
    return None


# ── Generic REST JSON detection ───────────────────────────────────────────────

_REST_CANDIDATES = [
    # Common product/machine paths
    "/api/products", "/api/machines", "/api/items", "/api/equipment",
    "/api/listings", "/api/inventory", "/api/catalog",
    "/api/v1/products", "/api/v1/machines", "/api/v1/items",
    "/api/v2/products", "/api/v2/machines",
    "/api/v3/products",
    # JSON endpoints
    "/products.json", "/machines.json", "/items.json", "/catalog.json",
    "/listings.json", "/equipment.json", "/inventory.json",
    # WP REST without auth
    "/wp-json/wp/v2/product", "/wp-json/wc/v3/products",
    # Next.js / Nuxt data routes
    "/_next/data/products", "/api/data/products",
    # Generic REST with filters
    "/api/search?type=product", "/api/search?category=machine",
]


def _detect_rest_json(base_url: str, session: requests.Session) -> Optional[APIConfig]:
    for path in _REST_CANDIDATES:
        url = urljoin(base_url, path)
        try:
            r = session.get(url, timeout=8)
            if r.status_code != 200:
                continue
            data = r.json()
            items, data_path = _find_items_in_response(data)
            if items and len(items) > 0:
                return APIConfig(
                    api_type="rest",
                    endpoint=url,
                    data_path=data_path,
                    pagination_param="page",
                    page_size=100,
                    base_url=base_url,
                )
        except Exception:
            continue
    return None


# ── Pre-configured API support (from training rules) ─────────────────────────

def build_config_from_rules(rules: dict) -> Optional[APIConfig]:
    """Convert WebsiteTrainingRules dict → APIConfig."""
    api_url = rules.get("api_url")
    if not api_url:
        return None
    headers = {}
    if rules.get("api_key"):
        headers["Authorization"] = f"Bearer {rules['api_key']}"
    try:
        extra = json.loads(rules.get("api_headers_json") or "{}")
        headers.update(extra)
    except json.JSONDecodeError:
        pass
    field_map = {}
    try:
        field_map = json.loads(rules.get("field_map_json") or "{}")
    except json.JSONDecodeError:
        pass
    return APIConfig(
        api_type="configured",
        endpoint=api_url,
        headers=headers,
        data_path=rules.get("api_data_path") or "",
        pagination_param=rules.get("api_pagination_param") or "page",
        page_size=int(rules.get("api_page_size") or 100),
        field_map=field_map,
        url_template=rules.get("url_template") or "",
        url_slug_field=rules.get("url_slug_field") or "",
        url_id_field=rules.get("url_id_field") or "",
    )


# ── Main detection entry point ────────────────────────────────────────────────

def detect_api(base_url: str, html: str) -> APIDetectionResult:
    """
    API-first detection. Order:
      1. Supabase (credentials in JS bundles)
      2. Shopify  (/products.json)
      3. WooCommerce (/wp-json/wc/v3/products)
      4. Generic REST JSON (known paths)
      5. HTML source scan (XHR/fetch calls in page + JS bundles)
      6. GraphQL probe

    Returns on the FIRST working API found.
    """
    session = _session()
    logger.info("[API] Starting detection for %s", base_url)

    detectors = [
        ("supabase",    lambda: _detect_supabase(base_url, html, session)),
        ("shopify",     lambda: _detect_shopify(base_url, session)),
        ("woocommerce", lambda: _detect_woocommerce(base_url, session)),
        ("rest",        lambda: _detect_rest_json(base_url, session)),
        ("html-scan",   lambda: _detect_from_html_sources(base_url, html, session)),
        ("graphql",     lambda: _detect_graphql(base_url, session)),
    ]

    for name, detect_fn in detectors:
        try:
            config = detect_fn()
            if config:
                items = list(_fetch_page(config, 0, session))
                if items:
                    logger.info("[API] detected (%s) endpoint=%s sample=%d",
                                name, config.endpoint, len(items))
                    return APIDetectionResult(
                        found=True, config=config, sample_count=len(items)
                    )
        except Exception as exc:
            logger.debug("[API] probe error (%s): %s", name, exc)

    logger.info("[API] no API detected for %s", base_url)
    return APIDetectionResult(found=False)


# ── Paginated fetching ────────────────────────────────────────────────────────

def _get_nested(data: Any, path: str) -> Any:
    """Resolve dot-notation path in nested dict/list."""
    if not path:
        return data
    for key in path.split("."):
        if isinstance(data, dict):
            data = data.get(key, [])
        elif isinstance(data, list) and key.isdigit():
            data = data[int(key)]
        else:
            return []
    return data


def _fetch_page(config: APIConfig, offset_or_page: int, session: requests.Session) -> List[dict]:
    """Fetch one page of items from the API."""
    params: Dict[str, Any] = {}

    if config.api_type == "supabase":
        params = {
            "select": "*",
            "limit":  config.page_size,
            "offset": offset_or_page,
        }
    elif config.pagination_param == "offset":
        params = {"limit": config.page_size, "offset": offset_or_page}
    else:
        params = {"page": offset_or_page, "per_page": config.page_size}

    r = session.get(config.endpoint, params=params, headers=config.headers, timeout=20)
    r.raise_for_status()
    data = r.json()
    items = _get_nested(data, config.data_path)
    return items if isinstance(items, list) else []


def _fetch_page_raw(
    config: APIConfig, offset_or_page: int, session: requests.Session
) -> Tuple[List[dict], Any]:
    """Like _fetch_page but also returns the raw response data (for total-count extraction)."""
    params: Dict[str, Any] = {}

    if config.api_type == "supabase":
        params = {"select": "*", "limit": config.page_size, "offset": offset_or_page}
    elif config.pagination_param == "offset":
        params = {"limit": config.page_size, "offset": offset_or_page}
    else:
        params = {"page": offset_or_page, "per_page": config.page_size}

    r = session.get(config.endpoint, params=params, headers=config.headers, timeout=20)
    r.raise_for_status()
    data = r.json()

    # WooCommerce stores total in header
    wc_total = 0
    if config.api_type == "woocommerce":
        try:
            wc_total = int(r.headers.get("X-WP-Total", 0))
        except (ValueError, TypeError):
            pass

    items = _get_nested(data, config.data_path)
    items = items if isinstance(items, list) else []
    return items, data, wc_total


def fetch_all_machines(config: APIConfig) -> Iterator[dict]:
    """
    Paginate through ALL API pages and yield raw item dicts with source_url injected.

    Supports:
      • offset + limit  (pagination_param == "offset")
      • page-based      (pagination_param == "page")
      • cursor-based    (cursor_field set in config)

    Validates total count: warns if collected count deviates > 20% from API total.
    Logs [API] total items / pages fetched / URLs collected.
    """
    session = _session()
    offset = 0
    page   = 1
    pages_fetched = 0
    total_collected = 0
    api_total = 0
    empty_streak = 0
    cursor: Optional[str] = None

    while True:
        try:
            if config.cursor_field and cursor is not None:
                # Cursor-based: pass cursor as query param
                params: Dict[str, Any] = {
                    "limit": config.page_size,
                    "after": cursor,
                }
                r = session.get(config.endpoint, params=params,
                                headers=config.headers, timeout=20)
                r.raise_for_status()
                raw_data = r.json()
                items = _get_nested(raw_data, config.data_path)
                items = items if isinstance(items, list) else []
                wc_total = 0
            else:
                cursor_val = offset if config.pagination_param == "offset" else page
                items, raw_data, wc_total = _fetch_page_raw(config, cursor_val, session)
        except Exception as exc:
            logger.error("[API] fetch error at page=%d offset=%d: %s", page, offset, exc)
            break

        # Extract total from first page
        if pages_fetched == 0:
            api_total = (
                wc_total
                or _extract_total_count(raw_data)
                or 0
            )
            if api_total:
                logger.info("[API] total items: %d", api_total)

        pages_fetched += 1

        if not items:
            empty_streak += 1
            if empty_streak >= 2:
                break
            offset += config.page_size
            page   += 1
            continue
        else:
            empty_streak = 0

        for item in items:
            # Inject source_url if not already present
            if "source_url" not in item and config.base_url:
                built = _build_product_url(item, config.base_url, config)
                if built:
                    item = {**item, "source_url": built}
            total_collected += 1
            yield item

        logger.debug("[API] pages fetched: %d  items so far: %d", pages_fetched, total_collected)

        # Cursor-based: advance cursor
        if config.cursor_field:
            next_cursor = _get_nested(raw_data, config.cursor_field)
            if not next_cursor:
                break
            cursor = next_cursor
        elif len(items) < config.page_size:
            # Last page reached
            break
        else:
            offset += config.page_size
            page   += 1

    logger.info("[API] pages fetched: %d", pages_fetched)
    logger.info("[API] URLs collected: %d", total_collected)

    # Validation
    if api_total > 0:
        deviation = abs(total_collected - api_total) / api_total
        if deviation > 0.20:
            logger.warning(
                "[API] VALIDATION WARNING: API reported %d items but collected %d "
                "(%.0f%% deviation) — pagination may be incomplete",
                api_total, total_collected, deviation * 100,
            )


def normalize_api_item(raw: dict, field_map: Dict[str, str], base_url: str = "") -> dict:
    """
    Map raw API fields to canonical MachineItem fields using field_map.
    Falls back to common field name guessing.
    """
    result: Dict[str, Any] = {}

    # Preserve source_url already injected by fetch_all_machines
    if raw.get("source_url"):
        result["source_url"] = raw["source_url"]

    def get_nested_value(obj: dict, dotted_key: str) -> Any:
        for part in dotted_key.split("."):
            if isinstance(obj, dict):
                obj = obj.get(part)
            else:
                return None
        return obj

    # Apply explicit field map
    for api_field, item_field in field_map.items():
        value = get_nested_value(raw, api_field)
        if value is not None:
            result[item_field] = value

    # Auto-detect common patterns if not already mapped
    _auto_map = {
        "machine_name": ["title", "name", "machine_name", "product_name", "label"],
        "brand":        ["brand", "make", "manufacturer", "hersteller", "marque"],
        "model":        ["model", "model_number", "modell", "modelo", "modele"],
        "machine_type": ["type", "category", "machine_type", "typ", "categorie"],
        "price":        ["price", "preis", "prix", "prezzo", "precio"],
        "description":  ["description", "desc", "details", "beschreibung"],
        "stock_number": ["stock_number", "ref", "sku", "reference", "stock_no"],
        "condition":    ["condition", "zustand", "etat", "stato"],
        "year":         ["year", "baujahr", "annee", "anno"],
    }
    for item_field, candidates in _auto_map.items():
        if item_field not in result:
            for cand in candidates:
                val = raw.get(cand)
                if val is not None:
                    result[item_field] = val
                    break

    # Images
    if "images" not in result:
        for key in ("images", "photos", "gallery", "bilder", "fotos"):
            val = raw.get(key)
            if isinstance(val, list):
                result["images"] = [
                    img.get("url") or img.get("src") or img
                    for img in val
                    if isinstance(img, (dict, str))
                ]
                break
            elif isinstance(val, str) and val.startswith("http"):
                result["images"] = [val]
                break

    # Specs / specifications
    if "specifications" not in result:
        for key in ("specs", "specifications", "attributes", "features"):
            val = raw.get(key)
            if isinstance(val, dict):
                result["specifications"] = val
                break
            elif isinstance(val, list):
                result["specifications"] = {
                    item.get("name", f"spec_{i}"): item.get("value", "")
                    for i, item in enumerate(val)
                    if isinstance(item, dict)
                }
                break

    return result
