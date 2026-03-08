from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Any

from sqlalchemy import JSON, Date, DateTime, String, Text, UniqueConstraint, Uuid, func
from sqlalchemy.orm import Mapped, mapped_column

from .db import Base


class Event(Base):
    """Unified market event aggregated from external providers."""

    __tablename__ = "events"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    symbol: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    event_type: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    event_date: Mapped[date] = mapped_column(Date, index=True, nullable=False)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    details: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    dedup_key: Mapped[str] = mapped_column(String(512), unique=True, index=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        UniqueConstraint("symbol", "event_type", "event_date", "title", name="uq_event_natural_key"),
    )


class SymbolSyncState(Base):
    """Tracks when each symbol was last synced from providers."""

    __tablename__ = "symbol_sync_state"

    symbol: Mapped[str] = mapped_column(String(32), primary_key=True)
    last_synced_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class IngestionErrorLog(Base):
    """Records provider errors during sync."""

    __tablename__ = "ingestion_errors"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(64), nullable=False)
    provider: Mapped[str] = mapped_column(String(32), nullable=False)
    error_message: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
