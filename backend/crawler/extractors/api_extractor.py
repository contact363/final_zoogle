"""
API Extractor — Phase 1 helper.

Detects and fetches machines from known API patterns:
  • Supabase / PostgREST  (auto-discovers project URL + anon key from JS bundle)
  • Generic REST JSON     (configurable endpoint + field mapping)
  • Shopify               (/products.json)
  • WooCommerce           (/wp-json/wc/v3/products)
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional
from urllib.parse import urljoin, urlparse

import requests

logger = logging.getLogger(__name__)

# ── Data structures ───────────────────────────────────────────────────────────

@dataclass
class APIConfig:
    api_type: str            # supabase | rest | shopify | woocommerce
    endpoint: str            # base API URL
    headers: Dict[str, str] = field(default_factory=dict)
    data_path: str = ""      # dot-notation path to items array
    pagination_param: str = "page"
    page_size: int = 100
    field_map: Dict[str, str] = field(default_factory=dict)


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
            return APIConfig(
                api_type="woocommerce",
                endpoint=urljoin(base_url, "/wp-json/wc/v3/products"),
                data_path="",
                pagination_param="page",
                page_size=100,
            )
    except Exception:
        pass
    return None


# ── Generic REST JSON detection ───────────────────────────────────────────────

_REST_CANDIDATES = [
    "/api/products", "/api/machines", "/api/items",
    "/api/v1/products", "/api/v1/machines",
    "/api/v2/products", "/api/v2/machines",
    "/products.json", "/machines.json",
]


def _detect_rest_json(base_url: str, session: requests.Session) -> Optional[APIConfig]:
    for path in _REST_CANDIDATES:
        url = urljoin(base_url, path)
        try:
            r = session.get(url, timeout=8)
            if r.status_code != 200:
                continue
            data = r.json()
            # Must be a list or a dict with a list value
            items = data if isinstance(data, list) else None
            if items is None:
                for key in ("data", "results", "items", "products", "machines"):
                    if isinstance(data.get(key), list):
                        items = data[key]
                        break
            if items and len(items) > 0:
                return APIConfig(
                    api_type="rest",
                    endpoint=url,
                    data_path="" if isinstance(data, list) else list(data.keys())[0],
                    pagination_param="page",
                    page_size=100,
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
    )


# ── Main detection entry point ────────────────────────────────────────────────

def detect_api(base_url: str, html: str) -> APIDetectionResult:
    """
    Try to detect a machine/product API for the given website.
    Returns APIDetectionResult with found=True and config if successful.
    """
    session = _session()

    for detect_fn in [
        lambda: _detect_supabase(base_url, html, session),
        lambda: _detect_shopify(base_url, session),
        lambda: _detect_woocommerce(base_url, session),
        lambda: _detect_rest_json(base_url, session),
    ]:
        try:
            config = detect_fn()
            if config:
                # Quick sample to confirm real data
                items = list(_fetch_page(config, 0, session))
                return APIDetectionResult(
                    found=True, config=config, sample_count=len(items)
                )
        except Exception as exc:
            logger.debug("API probe error: %s", exc)

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


def fetch_all_machines(config: APIConfig) -> Iterator[dict]:
    """
    Paginate through all API pages and yield raw item dicts.
    Stops when a page returns fewer items than page_size.
    """
    session = _session()
    offset = 0
    page   = 1
    empty_streak = 0

    while True:
        try:
            cursor = offset if config.pagination_param == "offset" else page
            items = _fetch_page(config, cursor, session)
        except Exception as exc:
            logger.error("API fetch error at offset/page %s: %s", offset or page, exc)
            break

        if not items:
            empty_streak += 1
            if empty_streak >= 2:
                break
        else:
            empty_streak = 0
            for item in items:
                yield item

        if len(items) < config.page_size:
            break

        offset += config.page_size
        page   += 1


def normalize_api_item(raw: dict, field_map: Dict[str, str]) -> dict:
    """
    Map raw API fields to canonical MachineItem fields using field_map.
    Falls back to common field name guessing.
    """
    result: Dict[str, Any] = {}

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
