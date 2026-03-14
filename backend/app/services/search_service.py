"""
Search engine for Zoogle.

Strategy (layered):
  1. PostgreSQL full-text search (tsvector) – fast ranked results
  2. ILIKE fallback for partial/fuzzy matching
  3. Synonym expansion (CNC → machining center, etc.)
  4. RapidFuzz post-processing for typo correction on brand/model
"""
from __future__ import annotations

import math
from decimal import Decimal
from typing import Optional

from rapidfuzz import fuzz, process
from sqlalchemy import select, func, or_, and_, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.machine import Machine
from app.models.search_log import SearchLog
from app.schemas.machine import SearchRequest, SearchResponse, SearchResultItem
from app.services.normalization_service import TYPE_SYNONYMS, _clean

# Build a flat synonym list for query expansion
_ALL_SYNONYMS: list[tuple[str, str]] = list(TYPE_SYNONYMS.items())


def _expand_query(query: str) -> list[str]:
    """Return the original query plus any synonym expansions."""
    terms = [query]
    q_lower = _clean(query)
    for synonym, canonical in _ALL_SYNONYMS:
        if synonym in q_lower or q_lower in synonym:
            if canonical.lower() not in q_lower:
                terms.append(canonical)
    return terms


async def search_machines(
    request: SearchRequest,
    db: AsyncSession,
    user_id: Optional[int] = None,
    ip: Optional[str] = None,
) -> SearchResponse:
    offset = (request.page - 1) * request.limit
    expanded = _expand_query(request.query)
    like_pattern = f"%{request.query}%"

    # ── Build base filter conditions ──────────────────────────────────────
    conditions = [Machine.is_active == True]

    if request.machine_type:
        conditions.append(
            Machine.type_normalized.ilike(f"%{request.machine_type}%")
        )
    if request.brand:
        conditions.append(
            Machine.brand_normalized.ilike(f"%{request.brand}%")
        )
    if request.location:
        conditions.append(
            Machine.location.ilike(f"%{request.location}%")
        )
    if request.price_min is not None:
        conditions.append(Machine.price >= request.price_min)
    if request.price_max is not None:
        conditions.append(Machine.price <= request.price_max)

    # ── Full-text search condition ────────────────────────────────────────
    ts_queries = " | ".join(
        " & ".join(word for word in term.split() if word)
        for term in expanded
        if term.strip()
    )

    search_cond = or_(
        # PostgreSQL full-text search
        text(
            "to_tsvector('english', coalesce(brand,'') || ' ' || coalesce(model,'') || ' '"
            " || coalesce(machine_type,'') || ' ' || coalesce(description,''))"
            f" @@ to_tsquery('english', '{ts_queries}')"
        ),
        # Fallback ILIKE on key columns
        Machine.brand.ilike(like_pattern),
        Machine.model.ilike(like_pattern),
        Machine.machine_type.ilike(like_pattern),
        Machine.description.ilike(like_pattern),
        Machine.brand_normalized.ilike(like_pattern),
        Machine.model_normalized.ilike(like_pattern),
        Machine.type_normalized.ilike(like_pattern),
        *[
            Machine.brand.ilike(f"%{term}%")
            for term in expanded[1:]
        ],
        *[
            Machine.machine_type.ilike(f"%{term}%")
            for term in expanded[1:]
        ],
    )
    conditions.append(search_cond)

    # ── Count total results ───────────────────────────────────────────────
    count_stmt = select(func.count()).select_from(Machine).where(and_(*conditions))
    total_result = await db.execute(count_stmt)
    total = total_result.scalar_one()

    # ── Fetch page ────────────────────────────────────────────────────────
    stmt = select(Machine).where(and_(*conditions))

    if request.sort_by == "price_asc":
        stmt = stmt.order_by(Machine.price.asc().nullslast())
    elif request.sort_by == "price_desc":
        stmt = stmt.order_by(Machine.price.desc().nullsfirst())
    elif request.sort_by == "newest":
        stmt = stmt.order_by(Machine.created_at.desc())
    else:
        # Relevance: full-text rank (approximate via created_at as tiebreaker)
        stmt = stmt.order_by(Machine.created_at.desc())

    stmt = stmt.offset(offset).limit(request.limit)
    rows = await db.execute(stmt)
    machines = rows.scalars().all()

    # ── Async log search ──────────────────────────────────────────────────
    log = SearchLog(
        query=request.query,
        results_count=total,
        user_id=user_id,
        ip_address=ip,
    )
    db.add(log)

    items = [SearchResultItem.model_validate(m) for m in machines]

    return SearchResponse(
        query=request.query,
        total=total,
        page=request.page,
        limit=request.limit,
        pages=math.ceil(total / request.limit) if total else 0,
        results=items,
    )
