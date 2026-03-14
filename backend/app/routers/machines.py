from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional

from app.database import get_db
from app.models.machine import Machine
from app.schemas.machine import MachineRead, MachineUpdate
from app.utils.security import get_current_user, require_admin

router = APIRouter(prefix="/api/machines", tags=["machines"])


@router.get("/{machine_id}", response_model=MachineRead)
async def get_machine(machine_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Machine).where(Machine.id == machine_id))
    machine = result.scalar_one_or_none()
    if not machine:
        raise HTTPException(status_code=404, detail="Machine not found")
    return machine


@router.patch("/{machine_id}", response_model=MachineRead)
async def update_machine(
    machine_id: int,
    payload: MachineUpdate,
    db: AsyncSession = Depends(get_db),
    admin=Depends(require_admin),
):
    result = await db.execute(select(Machine).where(Machine.id == machine_id))
    machine = result.scalar_one_or_none()
    if not machine:
        raise HTTPException(status_code=404, detail="Machine not found")

    for field, value in payload.model_dump(exclude_unset=True).items():
        setattr(machine, field, value)
    await db.flush()
    return machine


@router.delete("/{machine_id}", status_code=204)
async def delete_machine(
    machine_id: int,
    db: AsyncSession = Depends(get_db),
    admin=Depends(require_admin),
):
    result = await db.execute(select(Machine).where(Machine.id == machine_id))
    machine = result.scalar_one_or_none()
    if not machine:
        raise HTTPException(status_code=404, detail="Machine not found")
    await db.delete(machine)
