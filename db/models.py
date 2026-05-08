from sqlalchemy import Column, String, Integer, Float, JSON
from db.database import Base


class TaskRecord(Base):
    __tablename__ = "tasks"

    id = Column(String, primary_key=True, index=True)
    display_id = Column(String, unique=True, index=True)

    type = Column(String)
    status = Column(String)

    retry_count = Column(Integer, default=0)
    max_retries = Column(Integer, default=3)

    result = Column(JSON, nullable=True)
    error = Column(String, nullable=True)

    duration_ms = Column(Float, nullable=True)

    created_at = Column(Float)
    completed_at = Column(Float, nullable=True)

    started_at = Column(Float, nullable=True)