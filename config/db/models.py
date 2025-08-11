from datetime import datetime, timezone
from uuid import uuid4
from sqlalchemy import Column, Integer, String, Boolean, DateTime, JSON
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.ext.declarative import declarative_base

from .base import Base


class Task(Base):
    __tablename__ = "tasks"

    id = Column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    task_type = Column(String, nullable=False)
    parameters = Column(JSON, default={}, nullable=False)
    priority = Column(Integer, default=0, nullable=False)
    is_urgent = Column(Boolean, default=False, nullable=False)

    status = Column(String, default="queued", nullable=False)
    created_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc), nullable=False)
    updated_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc), nullable=False)
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    error_message = Column(String, nullable=True)
    retry_count = Column(Integer, default=0, nullable=False)
    result = Column(JSON, nullable=True)

    def __repr__(self):
        return (f"<Task(id={self.id}, type={self.task_type}, "
                f"status={self.status}, priority={self.priority})>")