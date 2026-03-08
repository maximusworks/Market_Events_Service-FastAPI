"""
Provider B - Financial events API (simulated).

Characteristics:
- Slower but more reliable
- Paginated (max 20/page)
- Nested data format
- Rate limited: 5 requests per 30 seconds
- Includes economic events
"""

import asyncio
import random
import time
import uuid
from datetime import datetime, timedelta
from typing import Any


class ProviderBError(Exception):
    """Base exception for Provider B errors."""
    pass


class RateLimitError(ProviderBError):
    """Raised when rate limit is exceeded."""
    def __init__(self, retry_after: int = 30):
        self.retry_after = retry_after
        super().__init__(f"Rate limit exceeded. Retry after {retry_after} seconds.")


class ProviderTimeoutError(ProviderBError):
    """Raised when request times out."""
    pass


# Module-level rate limit tracking
_request_timestamps: list[float] = []
_RATE_LIMIT = 5
_RATE_WINDOW = 30


def _reset_rate_limit():
    """Reset rate limit state (useful for testing)."""
    global _request_timestamps
    _request_timestamps = []


class ProviderB:
    """
    Async client for Provider B's events API.

    Note: This provider returns paginated results. You must handle pagination
    to retrieve all events.

    Usage:
        async with ProviderB(api_key="your-key") as provider:
            result = await provider.fetch_events(["AAPL"], days_ahead=30)
            events = result["events"]

            while result["pagination"]["has_next"]:
                result = await provider.fetch_events(
                    ["AAPL"],
                    days_ahead=30,
                    cursor=result["pagination"]["next_cursor"]
                )
                events.extend(result["events"])
    """

    def __init__(self, api_key: str = "test-key"):
        self.api_key = api_key
        self._cursor_cache: dict[str, list[dict]] = {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._cursor_cache.clear()

    async def fetch_events(
        self,
        symbols: list[str],
        days_ahead: int = 30,
        cursor: str | None = None,
        page_size: int = 20,
    ) -> dict[str, Any]:
        """
        Fetch upcoming events for the given symbols.

        Args:
            symbols: List of ticker symbols
            days_ahead: Number of days to look ahead
            cursor: Pagination cursor from previous response
            page_size: Number of items per page (max 20)

        Returns:
            Dict with 'events', 'pagination', and 'meta' keys

        Raises:
            RateLimitError: If rate limit exceeded
            ProviderTimeoutError: If request times out
        """
        global _request_timestamps

        # Simulate slower but more consistent latency
        await asyncio.sleep(random.uniform(0.2, 1.2))

        # Check rate limit
        now = time.monotonic()
        _request_timestamps = [ts for ts in _request_timestamps if now - ts < _RATE_WINDOW]

        if len(_request_timestamps) >= _RATE_LIMIT:
            raise RateLimitError(retry_after=_RATE_WINDOW)

        _request_timestamps.append(now)

        # Simulate occasional timeout (3% chance)
        if random.random() < 0.03:
            await asyncio.sleep(5)
            raise ProviderTimeoutError("Request timed out")

        page_size = min(page_size, 20)

        # Get or generate events
        if cursor and cursor in self._cursor_cache:
            all_events = self._cursor_cache[cursor]
        else:
            all_events = self._generate_events(symbols, days_ahead)
            if len(all_events) > page_size:
                cursor_key = str(uuid.uuid4())
                self._cursor_cache[cursor_key] = all_events

        # Sometimes pagination gets "stuck" (returns same page)
        if cursor and random.random() < 0.1:
            pass  # Don't advance cursor position

        # Calculate pagination
        start_idx = 0
        if cursor:
            for i, event in enumerate(all_events):
                if event.get("_cursor_marker") == cursor:
                    start_idx = i + 1
                    break

        page_events = all_events[start_idx:start_idx + page_size]
        has_next = start_idx + page_size < len(all_events)

        # Generate next cursor
        next_cursor = None
        if has_next and page_events:
            next_cursor = str(uuid.uuid4())
            page_events[-1]["_cursor_marker"] = next_cursor
            self._cursor_cache[next_cursor] = all_events

        # Clean internal markers before returning
        clean_events = []
        for event in page_events:
            clean_event = {k: v for k, v in event.items() if not k.startswith("_")}
            clean_events.append(clean_event)

        return {
            "events": clean_events,
            "pagination": {
                "total": len(all_events),
                "page_size": page_size,
                "has_next": has_next,
                "next_cursor": next_cursor,
            },
            "meta": {
                "request_id": str(uuid.uuid4()),
                "timestamp": datetime.utcnow().isoformat() + "Z",
            },
        }

    def _generate_events(self, symbols: list[str], days_ahead: int) -> list[dict[str, Any]]:
        """Generate mock events with Provider B's nested format."""
        events = []
        base_date = datetime.now()

        for symbol in symbols:
            num_events = random.randint(3, 7)

            for _ in range(num_events):
                event_date = base_date + timedelta(days=random.randint(1, days_ahead))

                # Provider B uses different event type names and has economic events
                event_type = random.choice([
                    "earnings_release",
                    "dividend_payment",
                    "stock_split",
                    "economic_indicator",
                ])

                event_id = f"pb_{symbol}_{event_type}_{event_date.strftime('%Y%m%d')}_{random.randint(1000, 9999)}"

                event = {
                    "id": event_id,
                    "instrument": {
                        "symbol": symbol,
                        "exchange": random.choice(["NASDAQ", "NYSE"]),
                    },
                    "event": {
                        "category": event_type,
                        "scheduled_at": f"{event_date.strftime('%Y-%m-%d')}T{random.randint(8, 16):02d}:00:00Z",
                        "title": f"{symbol} - {event_type.replace('_', ' ').title()}",
                        "description": f"Upcoming {event_type.replace('_', ' ')} event for {symbol}",
                    },
                    "provider_metadata": {
                        "source": "provider_b",
                        "quality_score": random.randint(70, 100),
                        "last_updated": datetime.utcnow().isoformat() + "Z",
                    },
                }

                # Add type-specific nested details
                if event_type == "earnings_release":
                    event["event"]["earnings_data"] = {
                        "eps_consensus": round(random.uniform(1.0, 5.0), 2),
                        "revenue_consensus": random.randint(10, 100) * 1e9,
                        "period": f"FY{event_date.year}Q{random.randint(1, 4)}",
                    }
                elif event_type == "dividend_payment":
                    event["event"]["dividend_data"] = {
                        "amount_per_share": round(random.uniform(0.1, 2.0), 4),
                        "currency": "USD",
                        "ex_dividend_date": (event_date - timedelta(days=14)).strftime("%Y-%m-%d"),
                    }
                elif event_type == "economic_indicator":
                    event["event"]["economic_data"] = {
                        "indicator_name": random.choice([
                            "CPI", "GDP", "Unemployment Rate", "Fed Interest Rate",
                            "Retail Sales", "Housing Starts", "PMI"
                        ]),
                        "previous_value": round(random.uniform(-2, 5), 2),
                        "forecast_value": round(random.uniform(-2, 5), 2),
                    }

                events.append(event)

        # Sort by date for consistent pagination
        events.sort(key=lambda e: e["event"]["scheduled_at"])

        return events

    async def get_event(self, event_id: str) -> dict[str, Any] | None:
        """Fetch a single event by ID."""
        await asyncio.sleep(random.uniform(0.1, 0.3))

        if random.random() < 0.05:
            raise ProviderTimeoutError("Request timed out")

        if random.random() < 0.1:
            return None

        return {
            "id": event_id,
            "instrument": {"symbol": "AAPL", "exchange": "NASDAQ"},
            "event": {
                "category": "earnings_release",
                "scheduled_at": (datetime.now() + timedelta(days=10)).isoformat() + "Z",
                "title": "Mock event",
            },
            "provider_metadata": {"source": "provider_b"},
        }
