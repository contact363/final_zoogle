from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text, func
from sqlalchemy.orm import relationship
from app.database import Base


class Website(Base):
    __tablename__ = "websites"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    url = Column(String(2048), unique=True, nullable=False, index=True)
    description = Column(Text, nullable=True)
    is_active = Column(Boolean, default=True)
    crawl_enabled = Column(Boolean, default=True)

    # Crawl stats
    machine_count = Column(Integer, default=0)
    last_crawled_at = Column(DateTime(timezone=True), nullable=True)
    crawl_status = Column(String(50), default="pending")  # pending, running, success, error

    # Discovery stats (Phase 1)
    discovered_count = Column(Integer, nullable=True)        # how many machines found on the site
    discovery_status = Column(String(50), default="pending") # pending, running, done, error

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    machines = relationship("Machine", back_populates="website", passive_deletes=True)
    crawl_logs = relationship("CrawlLog", back_populates="website", passive_deletes=True)
    training_rules = relationship(
        "WebsiteTrainingRules",
        back_populates="website",
        uselist=False,          # one-to-one
        passive_deletes=True,
        cascade="all, delete-orphan",
    )
