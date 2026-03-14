from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.schemas.machine import SearchRequest, SearchResponse
from app.services.search_service import search_machines

router = APIRouter(prefix="/api/search", tags=["search"])


@router.post("", response_model=SearchResponse)
async def search(
    payload: SearchRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    ip = request.client.host if request.client else None
    return await search_machines(payload, db, ip=ip)


@router.get("", response_model=SearchResponse)
async def search_get(
    q: str = "",
    machine_type: str = None,
    brand: str = None,
    location: str = None,
    price_min: float = None,
    price_max: float = None,
    sort_by: str = "relevance",
    page: int = 1,
    limit: int = 20,
    request: Request = None,
    db: AsyncSession = Depends(get_db),
):
    payload = SearchRequest(
        query=q,
        machine_type=machine_type,
        brand=brand,
        location=location,
        price_min=price_min,
        price_max=price_max,
        sort_by=sort_by,
        page=page,
        limit=min(limit, 100),
    )
    ip = request.client.host if request.client else None
    return await search_machines(payload, db, ip=ip)
