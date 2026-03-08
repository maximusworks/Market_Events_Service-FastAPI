from __future__ import annotations

import logging
import uuid
from datetime import date
from typing import Annotated

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Response, status
from redis.asyncio import Redis, from_url as redis_from_url
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from .config import Settings, get_settings
from .db import get_db_session, init_db
from .schemas import EventOut, EventsListResponse, HealthComponentStatus, HealthResponse, SyncRequest, SyncResponse
from .services import (
    event_to_schema,
    fetch_and_normalize_events,
    get_cached_response,
    get_symbols_to_sync,
    invalidate_events_cache,
    list_events,
    record_errors,
    set_cached_response,
    update_symbol_sync_state,
    upsert_events,
)

logger = logging.getLogger("market-events-service")


class NullRedis:
    async def get(self, _key: str):
        return None

    async def set(self, _key: str, _value: str, ex: int | None = None):
        return None

    async def ping(self):
        return True

    async def scan_iter(self, match: str | None = None):
        if False:
            yield match

    async def delete(self, _key: str):
        return 0

    async def aclose(self):
        return None



def _create_redis(settings: Settings) -> Redis | NullRedis:
    if settings.redis_url.startswith("memory://"):
        return NullRedis()
    return redis_from_url(settings.redis_url, encoding="utf-8", decode_responses=True)


settings = get_settings()
redis_client = _create_redis(settings)

app = FastAPI(title="Market Events Service", version="1.0.0")


@app.on_event("startup")
async def on_startup() -> None:
    logging.basicConfig(level=settings.log_level)
    await init_db()
    try:
        await redis_client.ping()
    except Exception:  # noqa: BLE001
        logger.exception("Redis ping failed during startup")


@app.on_event("shutdown")
async def on_shutdown() -> None:
    await redis_client.aclose()


DbSessionDep = Annotated[AsyncSession, Depends(get_db_session)]
SettingsDep = Annotated[Settings, Depends(get_settings)]


def _parse_symbols(symbols: str | None) -> list[str] | None:
    if symbols is None:
        return None
    clean = [part.strip().upper() for part in symbols.split(",") if part.strip()]
    return clean or None


@app.get("/api/v1/health", response_model=HealthResponse)
async def health(db: DbSessionDep) -> HealthResponse:
    db_status = "down"
    redis_status = "down"

    try:
        await db.execute(text("SELECT 1"))
        db_status = "up"
    except Exception as exc:  # noqa: BLE001
        logger.exception("Database health check failed: %s", exc)

    try:
        await redis_client.ping()
        redis_status = "up"
    except Exception as exc:  # noqa: BLE001
        logger.exception("Redis health check failed: %s", exc)

    overall = "ok" if db_status == "up" and redis_status == "up" else "down" if db_status == "down" and redis_status == "down" else "degraded"
    return HealthResponse(
        status=overall,
        database=HealthComponentStatus(status=db_status, details=None),
        redis=HealthComponentStatus(status=redis_status, details=None),
    )


@app.get("/api/v1/events", response_model=EventsListResponse)
async def get_events(
    response: Response,
    db: DbSessionDep,
    settings_dep: SettingsDep,
    symbols: str | None = Query(default=None, description="Comma-separated symbols, e.g. AAPL,MSFT"),
    event_type: str | None = Query(default=None, pattern="^(earnings|dividend|economic|split)$"),
    from_date: date | None = Query(default=None, description="Start date (YYYY-MM-DD)"),
    to_date: date | None = Query(default=None, description="End date (YYYY-MM-DD)"),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> EventsListResponse:
    if from_date and to_date and from_date > to_date:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="from_date must be <= to_date")

    symbols_list = _parse_symbols(symbols)
    cache_key = "events?" + "&".join([
        f"symbols={','.join(sorted(symbols_list))}" if symbols_list else "symbols=",
        f"event_type={event_type or ''}",
        f"from_date={from_date.isoformat() if from_date else ''}",
        f"to_date={to_date.isoformat() if to_date else ''}",
        f"limit={limit}",
        f"offset={offset}",
    ])

    cached = await get_cached_response(redis_client, cache_key)
    if cached is not None:
        response.headers["X-Cache"] = "HIT"
        return EventsListResponse.model_validate(cached)

    events, total = await list_events(
        db,
        symbols=symbols_list,
        event_type=event_type,
        from_date=from_date,
        to_date=to_date,
        limit=limit,
        offset=offset,
    )
    payload = EventsListResponse(
        data=[event_to_schema(event) for event in events],
        total=total,
        limit=limit,
        offset=offset,
        has_more=offset + limit < total,
    )
    await set_cached_response(redis_client, cache_key, payload.model_dump(mode="json"), settings_dep.cache_ttl_seconds)
    response.headers["X-Cache"] = "MISS"
    return payload


@app.get("/api/v1/events/{event_id}", response_model=EventOut)
async def get_event_by_id(event_id: uuid.UUID, db: DbSessionDep) -> EventOut:
    from .models import Event

    event = await db.get(Event, event_id)
    if event is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Event not found")
    return event_to_schema(event)


@app.post("/api/v1/events/sync", response_model=SyncResponse)
async def sync_events(
    payload: SyncRequest,
    db: DbSessionDep,
    settings_dep: SettingsDep,
    x_request_id: str | None = Header(default=None, alias="X-Request-ID"),
) -> SyncResponse:
    symbols_to_sync, symbols_skipped = await get_symbols_to_sync(db, symbols=payload.symbols, force=payload.force)
    if not symbols_to_sync:
        return SyncResponse(
            status="completed",
            symbols_synced=[],
            symbols_skipped=symbols_skipped,
            events_created=0,
            events_updated=0,
            errors=[],
        )

    normalized_events, fetch_errors = await fetch_and_normalize_events(symbols_to_sync, settings_dep)
    created, updated = await upsert_events(db, normalized_events)
    await update_symbol_sync_state(db, symbols_to_sync)
    await record_errors(db, fetch_errors)
    await invalidate_events_cache(redis_client)

    if x_request_id:
        for err in fetch_errors:
            err["request_id"] = x_request_id

    status_value = "failed" if fetch_errors and created == 0 and updated == 0 else "partial" if fetch_errors else "completed"
    return SyncResponse(
        status=status_value,
        symbols_synced=symbols_to_sync,
        symbols_skipped=symbols_skipped,
        events_created=created,
        events_updated=updated,
        errors=fetch_errors,
    )
