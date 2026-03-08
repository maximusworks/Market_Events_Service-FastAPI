from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any

os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///./test_assessment.db")
os.environ.setdefault("REDIS_URL", "memory://")
os.environ.setdefault("PROVIDER_A_API_KEY", "test-key-a")
os.environ.setdefault("PROVIDER_B_API_KEY", "test-key-b")

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import AsyncSessionLocal, Base, engine
from app.main import app
from app.models import Event, IngestionErrorLog, SymbolSyncState
from app.services import NormalizedEvent, event_to_schema, upsert_events


@pytest.fixture(autouse=True, scope="module")
async def prepare_database() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)


@pytest.fixture(autouse=True)
async def clean_tables() -> None:
    async with AsyncSessionLocal() as session:
        for model in (IngestionErrorLog, SymbolSyncState, Event):
            await session.execute(delete(model))
        await session.commit()


@pytest.fixture
async def db_session() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        yield session


@pytest.mark.asyncio
async def test_upsert_events_creates_and_updates(db_session: AsyncSession) -> None:
    now = datetime.now(UTC)
    norm = NormalizedEvent(
        symbol="AAPL",
        event_type="earnings",
        event_date=now,
        title="Q1 Earnings",
        details={"foo": "bar"},
    )

    created, updated = await upsert_events(db_session, [norm])
    assert created == 1
    assert updated == 0

    norm2 = NormalizedEvent(
        symbol="AAPL",
        event_type="earnings",
        event_date=now,
        title="Q1 Earnings",
        details={"foo": "baz"},
    )
    created2, updated2 = await upsert_events(db_session, [norm2])
    assert created2 == 0
    assert updated2 == 1


@pytest.mark.asyncio
async def test_event_to_schema_roundtrip(db_session: AsyncSession) -> None:
    event = Event(
        symbol="MSFT",
        event_type="dividend",
        event_date=datetime.now(UTC).date(),
        title="Dividend Payment",
        details={"amount": 1.23},
        dedup_key="test-key",
    )
    db_session.add(event)
    await db_session.commit()
    await db_session.refresh(event)

    schema = event_to_schema(event)
    assert schema.symbol == "MSFT"
    assert schema.event_type == "dividend"
    assert schema.title == "Dividend Payment"


@pytest.mark.asyncio
async def test_health_endpoint() -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/api/v1/health")
    assert resp.status_code == 200
    body = resp.json()
    assert "status" in body
    assert body["database"]["status"] == "up"
    assert body["redis"]["status"] == "up"


@pytest.mark.asyncio
async def test_events_list_empty() -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/api/v1/events")
    assert resp.status_code == 200
    assert resp.headers.get("X-Cache") in {"HIT", "MISS"}
    data = resp.json()
    assert data["total"] == 0
    assert data["data"] == []


@pytest.mark.asyncio
async def test_sync_endpoint_uses_providers_and_returns_events(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_fetch_and_normalize_events(symbols: list[str], settings: Any):
        now = datetime.now(UTC)
        return ([
            NormalizedEvent(
                symbol=symbols[0],
                event_type="earnings",
                event_date=now,
                title=f"{symbols[0]} Q1 Earnings",
                details={"source": "test"},
            )
        ], [])

    monkeypatch.setattr("app.main.fetch_and_normalize_events", fake_fetch_and_normalize_events)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        sync_resp = await client.post("/api/v1/events/sync", json={"symbols": ["AAPL"], "force": True})
        list_resp = await client.get("/api/v1/events?symbols=AAPL")

    assert sync_resp.status_code == 200
    sync_body = sync_resp.json()
    assert sync_body["status"] == "completed"
    assert sync_body["events_created"] == 1

    assert list_resp.status_code == 200
    list_body = list_resp.json()
    assert list_body["total"] == 1
    assert list_body["data"][0]["symbol"] == "AAPL"
