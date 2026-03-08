from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, field_serializer


EventType = Literal["earnings", "dividend", "economic", "split"]


class EventOut(BaseModel):
    id: uuid.UUID
    symbol: str
    event_type: EventType
    event_date: date
    title: str
    details: dict[str, Any]
    created_at: datetime

    @field_serializer("event_date")
    def serialize_date(self, v: date) -> str:
        return v.isoformat()


class EventsListResponse(BaseModel):
    data: list[EventOut]
    total: int
    limit: int
    offset: int
    has_more: bool


class SyncRequest(BaseModel):
    symbols: list[str] = Field(..., min_length=1)
    force: bool = False


class SyncResponse(BaseModel):
    status: Literal["completed", "partial", "failed"]
    symbols_synced: list[str]
    symbols_skipped: list[str]
    events_created: int
    events_updated: int
    errors: list[dict[str, Any]]


class HealthComponentStatus(BaseModel):
    status: Literal["up", "down"]
    details: dict[str, Any] | None = None


class HealthResponse(BaseModel):
    status: Literal["ok", "degraded", "down"]
    database: HealthComponentStatus
    redis: HealthComponentStatus

