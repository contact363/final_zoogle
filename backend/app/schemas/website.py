from pydantic import BaseModel, HttpUrl
from typing import Optional
from datetime import datetime


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
