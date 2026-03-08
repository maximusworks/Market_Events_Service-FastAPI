from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

from redis.asyncio import Redis
from sqlalchemy import Select, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from providers import ProviderA, ProviderB

from .config import Settings
from .models import Event, IngestionErrorLog, SymbolSyncState
from .schemas import EventOut, EventType

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class NormalizedEvent:
    symbol: str
    event_type: EventType
    event_date: datetime
    title: str
    details: dict[str, Any]

    def dedup_key(self) -> str:
        return "|".join([
            self.symbol.upper().strip(),
            self.event_type,
            self.event_date.date().isoformat(),
            self.title.strip().lower(),
        ])


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


async def _fetch_provider_a_events(symbols: list[str], settings: Settings) -> list[dict[str, Any]]:
    async with ProviderA(api_key=settings.provider_a_api_key) as client:
        return await client.fetch_events(symbols=symbols)


async def _fetch_provider_b_events(symbols: list[str], settings: Settings) -> list[dict[str, Any]]:
    async with ProviderB(api_key=settings.provider_b_api_key) as client:
        all_events: list[dict[str, Any]] = []
        seen_cursors: set[str | None] = set()
        cursor: str | None = None

        while cursor not in seen_cursors:
            seen_cursors.add(cursor)
            result = await client.fetch_events(symbols=symbols, cursor=cursor)
            all_events.extend(result.get("events", []))
            pagination = result.get("pagination", {})
            next_cursor = pagination.get("next_cursor")
            if not pagination.get("has_next") or not next_cursor:
                break
            cursor = next_cursor

        return all_events


def _normalize_from_provider_a(event: dict[str, Any]) -> NormalizedEvent:
    event_date = _ensure_utc(datetime.fromisoformat(event["date"]))
    return NormalizedEvent(
        symbol=event["ticker"].upper(),
        event_type=event["type"],
        event_date=event_date,
        title=event["title"],
        details={
            "source": "provider_a",
            "provider_event_id": event.get("event_id"),
            "raw_metadata": event.get("metadata") or {},
            "raw_details": event.get("details") or {},
            "time": event.get("time"),
        },
    )


def _map_provider_b_category(category: str) -> EventType | None:
    mapping: dict[str, EventType] = {
        "earnings_release": "earnings",
        "dividend_payment": "dividend",
        "stock_split": "split",
        "economic_indicator": "economic",
    }
    return mapping.get(category)


def _normalize_from_provider_b(event: dict[str, Any]) -> NormalizedEvent | None:
    event_payload = event.get("event") or {}
    category = event_payload.get("category") or ""
    event_type = _map_provider_b_category(category)
    if event_type is None:
        return None

    scheduled_at = event_payload.get("scheduled_at")
    parsed = datetime.fromisoformat(scheduled_at.replace("Z", "+00:00")) if scheduled_at else datetime.now(UTC)
    instrument = event.get("instrument") or {}

    return NormalizedEvent(
        symbol=str(instrument.get("symbol", "UNKNOWN")).upper(),
        event_type=event_type,
        event_date=_ensure_utc(parsed),
        title=event_payload.get("title") or "",
        details={
            "source": "provider_b",
            "provider_event_id": event.get("id"),
            "instrument": instrument,
            "provider_metadata": event.get("provider_metadata") or {},
            "raw_event": event_payload,
        },
    )


async def fetch_and_normalize_events(symbols: list[str], settings: Settings) -> tuple[list[NormalizedEvent], list[dict[str, Any]]]:
    errors: list[dict[str, Any]] = []
    normalized: list[NormalizedEvent] = []
    provider_names = ["provider_a", "provider_b"]
    tasks = [_fetch_provider_a_events(symbols, settings), _fetch_provider_b_events(symbols, settings)]

    for provider_name, result in zip(provider_names, await asyncio.gather(*tasks, return_exceptions=True), strict=True):
        if isinstance(result, Exception):
            errors.append({"provider": provider_name, "symbols": symbols, "error": str(result)})
            continue

        for raw in result:
            try:
                item = _normalize_from_provider_a(raw) if provider_name == "provider_a" else _normalize_from_provider_b(raw)
                if item is not None:
                    normalized.append(item)
            except Exception as exc:  # noqa: BLE001
                symbol = raw.get("ticker") or (raw.get("instrument") or {}).get("symbol") or "UNKNOWN"
                errors.append({
                    "provider": provider_name,
                    "symbols": [symbol],
                    "error": f"normalization failed: {exc}",
                })

    unique_by_key: dict[str, NormalizedEvent] = {}
    for event in normalized:
        unique_by_key[event.dedup_key()] = event

    return list(unique_by_key.values()), errors


async def upsert_events(session: AsyncSession, events: Iterable[NormalizedEvent]) -> tuple[int, int]:
    events_list = list(events)
    if not events_list:
        return 0, 0

    dedup_keys = [event.dedup_key() for event in events_list]
    result = await session.execute(select(Event).where(Event.dedup_key.in_(dedup_keys)))
    existing_by_key = {event.dedup_key: event for event in result.scalars()}

    created = 0
    updated = 0
    for norm in events_list:
        key = norm.dedup_key()
        existing = existing_by_key.get(key)
        if existing is None:
            session.add(Event(
                symbol=norm.symbol.upper(),
                event_type=norm.event_type,
                event_date=norm.event_date.date(),
                title=norm.title,
                details=norm.details,
                dedup_key=key,
            ))
            created += 1
            continue

        changed = False
        if existing.title != norm.title:
            existing.title = norm.title
            changed = True
        if existing.details != norm.details:
            existing.details = norm.details
            changed = True
        if changed:
            updated += 1

    await session.commit()
    return created, updated


async def record_errors(session: AsyncSession, errors: Iterable[dict[str, Any]]) -> None:
    error_list = list(errors)
    for err in error_list:
        session.add(IngestionErrorLog(
            symbol=",".join(err.get("symbols") or []) or "UNKNOWN",
            provider=str(err.get("provider", "unknown")),
            error_message=str(err.get("error", "")),
        ))
    if error_list:
        await session.commit()


async def update_symbol_sync_state(session: AsyncSession, symbols: Iterable[str]) -> None:
    now = datetime.now(UTC)
    for symbol in {symbol.upper() for symbol in symbols}:
        state = await session.get(SymbolSyncState, symbol)
        if state is None:
            session.add(SymbolSyncState(symbol=symbol, last_synced_at=now))
        else:
            state.last_synced_at = now
    await session.commit()


async def get_symbols_to_sync(
    session: AsyncSession,
    symbols: list[str],
    force: bool,
    freshness_window: timedelta = timedelta(hours=1),
) -> tuple[list[str], list[str]]:
    unique_symbols = sorted({symbol.upper() for symbol in symbols if symbol.strip()})
    if force:
        return unique_symbols, []

    now = datetime.now(UTC)
    to_sync: list[str] = []
    skipped: list[str] = []

    for symbol in unique_symbols:
        state = await session.get(SymbolSyncState, symbol)
        if state is None:
            to_sync.append(symbol)
            continue
        last_synced_at = _ensure_utc(state.last_synced_at)
        if now - last_synced_at > freshness_window:
            to_sync.append(symbol)
        else:
            skipped.append(symbol)

    return to_sync, skipped


def _apply_event_filters(
    stmt: Select[Any],
    *,
    symbols: list[str] | None,
    event_type: EventType | None,
    from_date: date | None,
    to_date: date | None,
) -> Select[Any]:
    if symbols:
        stmt = stmt.where(Event.symbol.in_([symbol.upper() for symbol in symbols]))
    if event_type:
        stmt = stmt.where(Event.event_type == event_type)
    if from_date:
        stmt = stmt.where(Event.event_date >= from_date)
    if to_date:
        stmt = stmt.where(Event.event_date <= to_date)
    return stmt


async def list_events(
    session: AsyncSession,
    *,
    symbols: list[str] | None,
    event_type: EventType | None,
    from_date: date | None,
    to_date: date | None,
    limit: int,
    offset: int,
) -> tuple[list[Event], int]:
    stmt: Select[Any] = select(Event)
    stmt = _apply_event_filters(stmt, symbols=symbols, event_type=event_type, from_date=from_date, to_date=to_date)
    stmt = stmt.order_by(Event.event_date.asc(), Event.symbol.asc(), Event.created_at.asc()).offset(offset).limit(limit)
    events = list((await session.execute(stmt)).scalars())

    count_stmt: Select[Any] = select(func.count()).select_from(Event)
    count_stmt = _apply_event_filters(count_stmt, symbols=symbols, event_type=event_type, from_date=from_date, to_date=to_date)
    total = int((await session.execute(count_stmt)).scalar_one())
    return events, total


def event_to_schema(event: Event) -> EventOut:
    created_at = _ensure_utc(event.created_at)
    return EventOut(
        id=event.id,
        symbol=event.symbol,
        event_type=event.event_type,
        event_date=event.event_date,
        title=event.title,
        details=event.details,
        created_at=created_at,
    )


async def get_cached_response(redis: Redis, cache_key: str) -> dict[str, Any] | None:
    try:
        raw = await redis.get(cache_key)
    except Exception:  # noqa: BLE001
        logger.exception("Redis read failed for key=%s", cache_key)
        return None

    if raw is None:
        return None

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


async def set_cached_response(redis: Redis, cache_key: str, payload: dict[str, Any], ttl_seconds: int) -> None:
    try:
        await redis.set(cache_key, json.dumps(payload, default=str), ex=ttl_seconds)
    except Exception:  # noqa: BLE001
        logger.exception("Redis write failed for key=%s", cache_key)


async def invalidate_events_cache(redis: Redis) -> None:
    try:
        async for key in redis.scan_iter(match="events?*"):
            await redis.delete(key)
    except Exception:  # noqa: BLE001
        logger.exception("Redis cache invalidation failed")
