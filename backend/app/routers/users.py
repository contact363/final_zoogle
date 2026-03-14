from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List

from app.database import get_db
from app.models.saved_machine import SavedMachine
from app.models.machine import Machine
from app.schemas.machine import MachineRead
from app.schemas.user import UserRead
from app.utils.security import get_current_user

router = APIRouter(prefix="/api/users", tags=["users"])


@router.get("/me", response_model=UserRead)
async def get_me(current_user=Depends(get_current_user)):
    return current_user


@router.get("/me/saved", response_model=List[MachineRead])
async def get_saved_machines(
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
):
    result = await db.execute(
        select(Machine)
        .join(SavedMachine, SavedMachine.machine_id == Machine.id)
        .where(SavedMachine.user_id == current_user.id)
        .order_by(SavedMachine.saved_at.desc())
    )
    return result.scalars().all()


@router.post("/me/saved/{machine_id}", status_code=201)
async def save_machine(
    machine_id: int,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
):
    # Check machine exists
    result = await db.execute(select(Machine).where(Machine.id == machine_id))
    if not result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Machine not found")

    # Check not already saved
    result = await db.execute(
        select(SavedMachine).where(
            SavedMachine.user_id == current_user.id,
            SavedMachine.machine_id == machine_id,
        )
    )
    if result.scalar_one_or_none():
        raise HTTPException(status_code=409, detail="Already saved")

    saved = SavedMachine(user_id=current_user.id, machine_id=machine_id)
    db.add(saved)
    return {"message": "Machine saved"}


@router.delete("/me/saved/{machine_id}", status_code=204)
async def unsave_machine(
    machine_id: int,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
):
    result = await db.execute(
        select(SavedMachine).where(
            SavedMachine.user_id == current_user.id,
            SavedMachine.machine_id == machine_id,
        )
    )
    saved = result.scalar_one_or_none()
    if not saved:
        raise HTTPException(status_code=404, detail="Not found in saved list")
    await db.delete(saved)
