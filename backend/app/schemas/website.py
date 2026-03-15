from pydantic import BaseModel
from typing import Optional
from datetime import datetime
from decimal import Decimal


# ── Training Rules ─────────────────────────────────────────────────────────────

class TrainingRulesBase(BaseModel):
    # ── Crawl mode ────────────────────────────────────────────────────────────
    crawl_type:     Optional[str]  = "auto"   # auto|html|api|playwright
    use_playwright: Optional[bool] = False

    # ── HTML selectors ────────────────────────────────────────────────────────
    listing_selector:     Optional[str] = None
    title_selector:       Optional[str] = None
    url_selector:         Optional[str] = None
    description_selector: Optional[str] = None
    image_selector:       Optional[str] = None
    price_selector:       Optional[str] = None
    category_selector:    Optional[str] = None
    pagination_selector:  Optional[str] = None

    # ── REST / JSON API config ────────────────────────────────────────────────
    api_url:              Optional[str] = None
    api_key:              Optional[str] = None
    api_headers_json:     Optional[str] = None   # JSON string
    api_data_path:        Optional[str] = None   # "data" | "results.items"
    api_pagination_param: Optional[str] = None   # "offset" | "page"
    api_page_size:        Optional[int] = None

    # JSON string: API field → MachineItem field
    field_map_json:       Optional[str] = None

    # ── URL filtering ─────────────────────────────────────────────────────────
    product_link_pattern: Optional[str] = None   # regex
    skip_url_patterns:    Optional[str] = None   # JSON list of regex strings

    # ── Request control ───────────────────────────────────────────────────────
    request_delay: Optional[Decimal] = None      # seconds
    max_items:     Optional[int]     = None      # CLOSESPIDER_ITEMCOUNT override


class TrainingRulesCreate(TrainingRulesBase):
    pass


class TrainingRulesRead(TrainingRulesBase):
    id:         int
    website_id: int
    created_at: datetime
    updated_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


# ── Website ────────────────────────────────────────────────────────────────────

class WebsiteCreate(BaseModel):
    name: str
    url: str
    description: Optional[str] = None
    crawl_enabled: bool = True


class WebsiteUpdate(BaseModel):
    name:          Optional[str]  = None
    description:   Optional[str]  = None
    is_active:     Optional[bool] = None
    crawl_enabled: Optional[bool] = None


class WebsiteRead(BaseModel):
    id:              int
    name:            str
    url:             str
    description:     Optional[str]
    is_active:       bool
    crawl_enabled:   bool
    machine_count:   int
    last_crawled_at: Optional[datetime]
    crawl_status:    str
    created_at:      datetime

    model_config = {"from_attributes": True}
