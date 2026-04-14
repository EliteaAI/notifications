import uuid
from datetime import datetime

from tools import db_tools, db
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import Integer, DateTime, func, Boolean, String
from sqlalchemy.dialects.postgresql import JSONB


class Notification(db.Base):
    __tablename__ = 'notifications'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    uuid: Mapped[str] = mapped_column(UUID(as_uuid=True), unique=True, default=uuid.uuid4)
    is_seen: Mapped[bool] = mapped_column(Boolean, default=False)
    project_id: Mapped[int] = mapped_column(Integer, nullable=False)
    user_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    meta: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    event_type: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=True, onupdate=func.now())
