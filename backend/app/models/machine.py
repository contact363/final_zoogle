from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, Text,
    Numeric, ForeignKey, Index, func
)
from sqlalchemy.orm import relationship
from app.database import Base


class Machine(Base):
    __tablename__ = "machines"

    id = Column(Integer, primary_key=True, index=True)
    website_id = Column(Integer, ForeignKey("websites.id", ondelete="CASCADE"), nullable=False)

    # Core fields
    machine_type = Column(String(100), nullable=True, index=True)
    brand = Column(String(100), nullable=True, index=True)
    model = Column(String(200), nullable=True, index=True)
    price = Column(Numeric(14, 2), nullable=True)
    currency = Column(String(10), default="USD")
    location = Column(String(255), nullable=True, index=True)
    description = Column(Text, nullable=True)

    # Source
    machine_url = Column(String(2048), nullable=False)
    website_source = Column(String(255), nullable=True)

    # Normalized/processed fields
    brand_normalized = Column(String(100), nullable=True, index=True)
    model_normalized = Column(String(200), nullable=True, index=True)
    type_normalized = Column(String(100), nullable=True, index=True)

    # Thumbnail (first image)
    thumbnail_url = Column(String(2048), nullable=True)
    thumbnail_local = Column(String(512), nullable=True)

    # Search vector (PostgreSQL tsvector stored as text, updated via trigger)
    search_vector = Column(Text, nullable=True)

    # Dedup hash
    content_hash = Column(String(64), unique=True, nullable=True, index=True)

    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    website = relationship("Website", back_populates="machines")
    images = relationship("MachineImage", back_populates="machine", cascade="all, delete-orphan")
    specs = relationship("MachineSpec", back_populates="machine", cascade="all, delete-orphan")
    saved_by = relationship("SavedMachine", back_populates="machine", lazy="dynamic")

    __table_args__ = (
        Index("ix_machines_brand_model", "brand_normalized", "model_normalized"),
        Index("ix_machines_type_brand", "type_normalized", "brand_normalized"),
    )


class MachineImage(Base):
    __tablename__ = "machine_images"

    id = Column(Integer, primary_key=True, index=True)
    machine_id = Column(Integer, ForeignKey("machines.id", ondelete="CASCADE"), nullable=False)
    image_url = Column(String(2048), nullable=False)
    local_path = Column(String(512), nullable=True)
    is_primary = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    machine = relationship("Machine", back_populates="images")


class MachineSpec(Base):
    __tablename__ = "machine_specs"

    id = Column(Integer, primary_key=True, index=True)
    machine_id = Column(Integer, ForeignKey("machines.id", ondelete="CASCADE"), nullable=False)
    spec_key = Column(String(100), nullable=False)
    spec_value = Column(Text, nullable=True)
    spec_unit = Column(String(50), nullable=True)

    machine = relationship("Machine", back_populates="specs")

    __table_args__ = (
        Index("ix_machine_specs_machine_key", "machine_id", "spec_key"),
    )
