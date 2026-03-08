"""
Provider A - Financial events API (simulated).

Characteristics:
- Fast but unreliable
- Rate limited: 10 requests per minute
- Returns flat list of events
- ~5% chance of 5xx errors
"""

import asyncio
import random
import time
from datetime import datetime, timedelta
from typing import Any


class ProviderAError(Exception):
    """Base exception for Provider A errors."""
    pass


class RateLimitError(ProviderAError):
    """Raised when rate limit is exceeded."""
    def __init__(self, retry_after: int = 60):
        self.retry_after = retry_after
        super().__init__(f"Rate limit exceeded. Retry after {retry_after} seconds.")


class ProviderUnavailableError(ProviderAError):
    """Raised when provider returns 5xx error."""
    pass


# Module-level rate limit tracking
_request_timestamps: list[float] = []
_RATE_LIMIT = 10
_RATE_WINDOW = 60


def _reset_rate_limit():
    """Reset rate limit state (useful for testing)."""
    global _request_timestamps
    _request_timestamps = []


class ProviderA:
    """
    Async client for Provider A's events API.

    Usage:
        async with ProviderA(api_key="your-key") as provider:
            events = await provider.fetch_events(["AAPL", "MSFT"], days_ahead=30)
    """

    def __init__(self, api_key: str = "test-key"):
        self.api_key = api_key

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    async def fetch_events(
        self,
        symbols: list[str],
        days_ahead: int = 30,
    ) -> list[dict[str, Any]]:
        """
        Fetch upcoming events for the given symbols.

        Args:
            symbols: List of ticker symbols
            days_ahead: Number of days to look ahead

        Returns:
            List of event dictionaries

        Raises:
            RateLimitError: If rate limit exceeded
            ProviderUnavailableError: If provider is down
        """
        global _request_timestamps

        # Simulate network latency (100ms - 800ms, occasionally 2s+)
        latency = random.uniform(0.1, 0.8)
        if random.random() < 0.1:
            latency = random.uniform(2.0, 4.0)
        await asyncio.sleep(latency)

        # Check rate limit
        now = time.monotonic()
        _request_timestamps = [ts for ts in _request_timestamps if now - ts < _RATE_WINDOW]

        if len(_request_timestamps) >= _RATE_LIMIT:
            raise RateLimitError(retry_after=_RATE_WINDOW)

        _request_timestamps.append(now)

        # Simulate occasional 5xx errors (5% chance)
        if random.random() < 0.05:
            raise ProviderUnavailableError("Internal server error")

        # Generate mock events
        events = []
        base_date = datetime.now()

        for symbol in symbols:
            num_events = random.randint(2, 5)

            for _ in range(num_events):
                event_date = base_date + timedelta(days=random.randint(1, days_ahead))
                event_type = random.choice(["earnings", "dividend", "split"])

                # Event ID based on symbol, type, and month
                event_id = f"pa-{symbol}-{event_type}-{event_date.strftime('%Y%m')}"

                event = {
                    "event_id": event_id,
                    "ticker": symbol,
                    "type": event_type,
                    "date": event_date.strftime("%Y-%m-%d"),
                    "time": event_date.strftime("%H:%M:%S") if event_type == "earnings" else None,
                    "title": f"{symbol} {event_type.title()} - {event_date.strftime('%b %Y')}",
                    "metadata": {
                        "source": "provider_a",
                        "fetched_at": datetime.utcnow().isoformat(),
                        "confidence": round(random.uniform(0.7, 1.0), 2),
                    },
                }

                # Add type-specific details
                if event_type == "earnings":
                    event["details"] = {
                        "eps_estimate": round(random.uniform(1.0, 5.0), 2),
                        "revenue_estimate": random.randint(10, 100) * 1_000_000_000,
                        "fiscal_quarter": f"Q{random.randint(1, 4)}",
                        "fiscal_year": event_date.year,
                    }
                elif event_type == "dividend":
                    event["details"] = {
                        "amount": round(random.uniform(0.1, 2.0), 2),
                        "ex_date": (event_date - timedelta(days=14)).strftime("%Y-%m-%d"),
                        "record_date": (event_date - timedelta(days=12)).strftime("%Y-%m-%d"),
                        "pay_date": event_date.strftime("%Y-%m-%d"),
                    }
                elif event_type == "split":
                    ratio = random.choice([(2, 1), (3, 1), (4, 1), (5, 1), (10, 1)])
                    event["details"] = {
                        "ratio_from": ratio[0],
                        "ratio_to": ratio[1],
                    }

                events.append(event)

        # Sometimes return duplicate events in the same response (different timestamps)
        if events and random.random() < 0.15:
            duplicate = events[0].copy()
            duplicate["metadata"] = duplicate["metadata"].copy()
            duplicate["metadata"]["fetched_at"] = datetime.utcnow().isoformat()
            events.append(duplicate)

        return events

    async def get_event(self, event_id: str) -> dict[str, Any] | None:
        """Fetch a single event by ID."""
        await asyncio.sleep(random.uniform(0.05, 0.2))

        if random.random() < 0.02:
            raise ProviderUnavailableError("Internal server error")

        if random.random() < 0.1:
            return None

        return {
            "event_id": event_id,
            "ticker": "AAPL",
            "type": "earnings",
            "date": (datetime.now() + timedelta(days=10)).strftime("%Y-%m-%d"),
            "title": "Mock event",
            "metadata": {"source": "provider_a"},
        }
