from sqlalchemy import Column, Integer, ForeignKey, DateTime, UniqueConstraint, func
from sqlalchemy.orm import relationship
from app.database import Base


class SavedMachine(Base):
    __tablename__ = "saved_machines"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    machine_id = Column(Integer, ForeignKey("machines.id", ondelete="CASCADE"), nullable=False)
    saved_at = Column(DateTime(timezone=True), server_default=func.now())

    user = relationship("User", back_populates="saved_machines")
    machine = relationship("Machine", back_populates="saved_by")

    __table_args__ = (
        UniqueConstraint("user_id", "machine_id", name="uq_user_machine"),
    )
