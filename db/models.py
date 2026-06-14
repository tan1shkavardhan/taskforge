from sqlalchemy import Column, String, Integer, Float, JSON
from db.database import Base


class TaskRecord(Base):
    __tablename__ = "tasks"

    id = Column(String, primary_key=True, index=True,nullable=False)
    display_id = Column(
        String, 
        unique=True, 
        index=True,
        nullable=False)

    type = Column(String,nullable=False)
    status = Column(String,nullable=False)

    retry_count = Column(Integer, default=0)
    max_retries = Column(Integer, default=3)

    result = Column(JSON, nullable=True)
    payload= Column(JSON, nullable=True)
    
    error = Column(String, nullable=True)

    duration_ms = Column(Float, nullable=True)

    created_at = Column(Float,nullable=False)

    started_at = Column(Float, nullable=True)

    last_heartbeat = Column(Float,nullable=True)

    completed_at = Column(Float, nullable=True)