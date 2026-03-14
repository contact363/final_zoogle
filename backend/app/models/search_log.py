from sqlalchemy import Column, Integer, String, DateTime, func
from app.database import Base


class SearchLog(Base):
    __tablename__ = "search_logs"

    id = Column(Integer, primary_key=True, index=True)
    query = Column(String(512), nullable=False)
    results_count = Column(Integer, default=0)
    user_id = Column(Integer, nullable=True)
    ip_address = Column(String(45), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
