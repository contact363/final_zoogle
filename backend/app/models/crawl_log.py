from sqlalchemy import Column, Integer, String, DateTime, Text, ForeignKey, func
from sqlalchemy.orm import relationship
from app.database import Base


class CrawlLog(Base):
    __tablename__ = "crawl_logs"

    id = Column(Integer, primary_key=True, index=True)
    website_id = Column(Integer, ForeignKey("websites.id", ondelete="CASCADE"), nullable=True)
    task_id = Column(String(255), nullable=True, index=True)

    status = Column(String(50), nullable=False)  # started, running, success, error, stopped
    machines_found = Column(Integer, default=0)
    machines_new = Column(Integer, default=0)
    machines_updated = Column(Integer, default=0)
    machines_skipped = Column(Integer, default=0)
    errors_count = Column(Integer, default=0)

    error_details = Column(Text, nullable=True)
    log_output = Column(Text, nullable=True)

    started_at = Column(DateTime(timezone=True), server_default=func.now())
    finished_at = Column(DateTime(timezone=True), nullable=True)

    website = relationship("Website", back_populates="crawl_logs")
