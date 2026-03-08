# Market Events Service

Build a FastAPI service that aggregates financial market events (earnings, dividends, splits) from multiple providers and exposes a unified API.

## Setup


```bash
docker-compose up -d postgres redis
cp .env.example .env
poetry install
poetry run uvicorn app.main:app --reload --port 8000
```


The API will be available at `http://localhost:8000`.


## Architecture notes

### Event deduplication
Both providers can return the same real-world event in different shapes and with different provider IDs. The service normalizes each item and computes:

```text
{SYMBOL}|{EVENT_TYPE}|{EVENT_DATE}|{normalized_title}
```

That key is stored as `dedup_key` and used to upsert records.

### Storage model
- `events`: canonical normalized events
- `symbol_sync_state`: tracks last successful sync timestamp per symbol
- `ingestion_errors`: stores provider/normalization failures for observability

### Caching
`GET /api/v1/events` is cached in Redis using the full query string dimensions:
- symbols
- event_type
- from_date
- to_date
- limit
- offset

After `POST /api/v1/events/sync`, all list cache entries are invalidated.

### Provider handling
- Provider A: flat payload, fast but unreliable
- Provider B: nested payload, paginated
- Provider B pagination is protected against cursor loops by tracking seen cursors
- Provider-level failures are returned in the sync response and persisted to `ingestion_errors`


## API examples

### Sync symbols

```bash
curl -X POST http://localhost:8000/api/v1/events/sync \
  -H 'Content-Type: application/json' \
  -H 'X-Request-ID: req-123' \
  -d '{"symbols": ["AAPL", "MSFT"], "force": true}'
```

### List events

```bash
curl 'http://localhost:8000/api/v1/events?symbols=AAPL,MSFT&event_type=earnings&limit=20&offset=0'
```

### Get event by id

```bash
curl http://localhost:8000/api/v1/events/<event-id>
```

### Health

```bash
curl http://localhost:8000/api/v1/health
```

## Testing

```bash
poetry install
poetry run pytest
```