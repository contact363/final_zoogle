"""CRUD and bulk upsert helpers for machines."""
from __future__ import annotations

import os
import hashlib
from typing import Optional

import httpx
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.machine import Machine, MachineImage, MachineSpec
from app.models.website import Website
from app.services.normalization_service import (
    normalize_brand,
    normalize_model,
    normalize_machine_type,
    build_content_hash,
)


async def upsert_machine(db: AsyncSession, data: dict) -> tuple[Machine, bool]:
    """
    Insert or update a machine record.
    Returns (machine, is_new).
    """
    brand_norm = normalize_brand(data.get("brand"))
    model_norm = normalize_model(data.get("model"))
    type_norm = normalize_machine_type(data.get("machine_type"))
    content_hash = build_content_hash(
        brand_norm, model_norm, data.get("machine_url", "")
    )

    result = await db.execute(
        select(Machine).where(Machine.content_hash == content_hash)
    )
    existing = result.scalar_one_or_none()

    if existing:
        # Update mutable fields
        existing.price = data.get("price")
        existing.description = data.get("description")
        existing.location = data.get("location")
        existing.brand_normalized = brand_norm
        existing.model_normalized = model_norm
        existing.type_normalized = type_norm
        await db.flush()
        return existing, False

    machine = Machine(
        website_id=data["website_id"],
        machine_type=data.get("machine_type"),
        brand=data.get("brand"),
        model=data.get("model"),
        price=data.get("price"),
        currency=data.get("currency", "USD"),
        location=data.get("location"),
        description=data.get("description"),
        machine_url=data["machine_url"],
        website_source=data.get("website_source"),
        brand_normalized=brand_norm,
        model_normalized=model_norm,
        type_normalized=type_norm,
        content_hash=content_hash,
    )
    db.add(machine)
    await db.flush()

    # Images
    for idx, img_url in enumerate(data.get("images", [])):
        img = MachineImage(
            machine_id=machine.id,
            image_url=img_url,
            is_primary=(idx == 0),
        )
        db.add(img)
        if idx == 0:
            machine.thumbnail_url = img_url

    # Specs
    for key, value in (data.get("specs") or {}).items():
        spec = MachineSpec(
            machine_id=machine.id,
            spec_key=key,
            spec_value=str(value),
        )
        db.add(spec)

    return machine, True


async def download_image(url: str, machine_id: int, idx: int) -> Optional[str]:
    """Download an image and return the local relative path."""
    ext = url.split(".")[-1].split("?")[0][:4] or "jpg"
    rel_path = f"{settings.MEDIA_DIR}/{machine_id}/{idx}.{ext}"
    abs_path = os.path.join(os.getcwd(), rel_path)
    os.makedirs(os.path.dirname(abs_path), exist_ok=True)

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(url, follow_redirects=True)
            if resp.status_code == 200:
                with open(abs_path, "wb") as f:
                    f.write(resp.content)
                return rel_path
    except Exception:
        pass
    return None


async def increment_website_machine_count(db: AsyncSession, website_id: int) -> None:
    await db.execute(
        update(Website)
        .where(Website.id == website_id)
        .values(machine_count=Website.machine_count + 1)
    )
