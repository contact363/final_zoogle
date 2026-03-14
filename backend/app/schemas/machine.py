from pydantic import BaseModel
from typing import Optional, List
from decimal import Decimal
from datetime import datetime


class MachineSpecRead(BaseModel):
    spec_key: str
    spec_value: Optional[str]
    spec_unit: Optional[str]

    model_config = {"from_attributes": True}


class MachineImageRead(BaseModel):
    id: int
    image_url: str
    local_path: Optional[str]
    is_primary: bool

    model_config = {"from_attributes": True}


class MachineRead(BaseModel):
    id: int
    machine_type: Optional[str]
    brand: Optional[str]
    model: Optional[str]
    price: Optional[Decimal]
    currency: str
    location: Optional[str]
    description: Optional[str]
    machine_url: str
    website_source: Optional[str]
    thumbnail_url: Optional[str]
    thumbnail_local: Optional[str]
    created_at: datetime
    updated_at: Optional[datetime]
    images: List[MachineImageRead] = []
    specs: List[MachineSpecRead] = []

    model_config = {"from_attributes": True}


class MachineUpdate(BaseModel):
    machine_type: Optional[str] = None
    brand: Optional[str] = None
    model: Optional[str] = None
    price: Optional[Decimal] = None
    location: Optional[str] = None
    description: Optional[str] = None
    is_active: Optional[bool] = None


# ── Search ──────────────────────────────────────────────────────────────────

class SearchRequest(BaseModel):
    query: str
    machine_type: Optional[str] = None
    brand: Optional[str] = None
    location: Optional[str] = None
    price_min: Optional[Decimal] = None
    price_max: Optional[Decimal] = None
    sort_by: Optional[str] = "relevance"   # relevance | price_asc | price_desc | newest
    page: int = 1
    limit: int = 20


class SearchResultItem(BaseModel):
    id: int
    machine_type: Optional[str]
    brand: Optional[str]
    model: Optional[str]
    price: Optional[Decimal]
    currency: str
    location: Optional[str]
    thumbnail_url: Optional[str]
    machine_url: str
    website_source: Optional[str]
    created_at: datetime

    model_config = {"from_attributes": True}


class SearchResponse(BaseModel):
    query: str
    total: int
    page: int
    limit: int
    pages: int
    results: List[SearchResultItem]
