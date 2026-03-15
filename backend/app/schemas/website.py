from pydantic import BaseModel, HttpUrl
from typing import Optional
from datetime import datetime


# ── Training Rules ─────────────────────────────────────────────────────────────

class TrainingRulesBase(BaseModel):
    listing_selector:     Optional[str] = None
    title_selector:       Optional[str] = None
    url_selector:         Optional[str] = None
    description_selector: Optional[str] = None
    image_selector:       Optional[str] = None
    price_selector:       Optional[str] = None
    category_selector:    Optional[str] = None
    pagination_selector:  Optional[str] = None


class TrainingRulesCreate(TrainingRulesBase):
    pass


class TrainingRulesRead(TrainingRulesBase):
    id:         int
    website_id: int
    created_at: datetime
    updated_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


class WebsiteCreate(BaseModel):
    name: str
    url: str
    description: Optional[str] = None
    crawl_enabled: bool = True


class WebsiteUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    is_active: Optional[bool] = None
    crawl_enabled: Optional[bool] = None


class WebsiteRead(BaseModel):
    id: int
    name: str
    url: str
    description: Optional[str]
    is_active: bool
    crawl_enabled: bool
    machine_count: int
    last_crawled_at: Optional[datetime]
    crawl_status: str
    created_at: datetime

    model_config = {"from_attributes": True}
