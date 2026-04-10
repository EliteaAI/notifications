from uuid import uuid4

from datetime import datetime
from typing import Optional

from pydantic.v1 import BaseModel, UUID4, Field


class NotificationBaseModel(BaseModel):
    id: int
    uuid: str | UUID4
    is_seen: bool
    project_id: int
    user_id: int
    meta: dict
    created_at: datetime
    updated_at: Optional[datetime] = None
    event_type: str

    class Config:
        orm_mode = True


class NotificationCreateModel(BaseModel):
    uuid: str | UUID4 | None = Field(default_factory=uuid4)
    project_id: int
    user_id: int
    meta: dict
    event_type: str


class NotificationBulkUpdateModel(BaseModel):
    ids: list[int]
    is_seen: bool


class NotificationBulkDeleteModel(BaseModel):
    ids: list[int]


class NotificationBulkUpdateResponseModel(BaseModel):
    updated: int


class NotificationBulkDeleteResponseModel(BaseModel):
    deleted: int
