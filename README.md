# Multi-Tenant API Rate Limiter

A FastAPI-based HTTP middleware that enforces **per-tenant, per-endpoint rate limiting** using the **Sliding Window Log** algorithm with pluggable Redis and in-memory storage backends.

---

## Architecture

```
                    ┌────────────────────────────────────────┐
                    │           FastAPI Middleware            │
                    │  ┌──────────────────────────────────┐  │
  HTTP Request ────▶│  │  Extract X-Tenant-ID + Endpoint  │  │
                    │  └──────────────┬───────────────────┘  │
                    │                 │                       │
                    │  ┌──────────────▼───────────────────┐  │
                    │  │   RateLimiter (Abstract Base)     │  │
                    │  │   ┌────────────┬────────────┐    │  │
                    │  │   │ RedisLimiter│InMemory   │    │  │
                    │  │   │ (ZSET +    ││Limiter    │    │  │
                    │  │   │  Pipeline) ││(deque +   │    │  │
                    │  │   │            ││  Lock)    │    │  │
                    │  │   └────────────┴────────────┘    │  │
                    │  └──────────────┬───────────────────┘  │
                    │                 │                       │
                    │       ┌─────────▼─────────┐            │
                    │       │  429 + Retry-After │            │
                    │       │   OR pass through  │            │
                    │       └───────────────────┘            │
                    └────────────────────────────────────────┘
```

---

## How to Run

### Option A: Docker (Recommended)
```bash
# Start the web server and Redis instance
docker-compose up --build -d

# The API will be available at http://localhost:8000
```

### Option B: Local Development
```bash
# Start Redis separately
docker run -d -p 6379:6379 redis:alpine

# Install dependencies
pip install -r requirements.txt

# Run with Redis backend
STORAGE_BACKEND=redis REDIS_URL=redis://localhost:6379/0 uvicorn app.main:app --reload

# Or run with in-memory backend (no Redis needed)
STORAGE_BACKEND=memory uvicorn app.main:app --reload
```

---

## Testing the API

Include the `X-Tenant-ID` header in your requests:

```bash
# Standard endpoint (100 req/min)
curl -H "X-Tenant-ID: tenant-a" http://localhost:8000/

# Upload endpoint (10 req/min — stricter limit)
curl -X POST -H "X-Tenant-ID: tenant-a" http://localhost:8000/upload

# Check metrics (not rate limited)
curl http://localhost:8000/metrics

# Trigger rate limit (send 11 rapid requests to /upload)
for i in $(seq 1 11); do
  curl -s -o /dev/null -w "Request $i: HTTP %{http_code}\n" \
    -X POST -H "X-Tenant-ID: tenant-a" http://localhost:8000/upload
done
```

---

## Algorithm Selection

### Why Sliding Window Log?

| Algorithm | Accuracy | Memory | Chosen? | Reason |
|---|---|---|---|---|
| **Fixed Window Counter** | ❌ Approximate | Very Low | No | Thundering herd problem at window boundaries |
| **Token/Leaky Bucket** | ✅ Good for bursts | Low | No | Doesn't align with strict 60s window requirement |
| **Sliding Window Counter** | ⚠️ Approximate | Low | No | Weighted average is imprecise at low limits |
| **Sliding Window Log** | ✅ 100% Accurate | Moderate | ✅ **Yes** | Perfect accuracy; feasible at 100 req/min scale |

**How it works:** Every request's timestamp is logged. To check if a new request is allowed, we evict all timestamps older than 60 seconds and count what remains. If count < limit, the request is allowed.

**Why it's feasible:** At 100 requests/minute, we store at most 100 timestamps per tenant per endpoint — negligible memory.

---

## Key Design Decisions

### 1. Concurrency & Atomicity (Critical)

Rate limiting is highly susceptible to race conditions. If two requests from the same tenant arrive at the same millisecond, a naive "read → check → write" pattern will fail.

**Redis Backend:** All operations are executed in a **single atomic pipeline**:

```python
pipe = self.redis.pipeline()
pipe.zremrangebyscore(key, 0, window_start_ms)           # Evict expired
pipe.zadd(key, {str(current_time_ms): current_time_ms})  # Optimistically add
pipe.zcard(key)                                           # Count total
pipe.zrange(key, 0, 0)                                   # Get oldest
pipe.expire(key, window_sec + 5)                          # TTL with buffer
results = pipe.execute()
```

If the count exceeds the limit, the optimistically added entry is removed. This eliminates the check-then-insert race condition window.

**In-Memory Backend:** Uses `threading.Lock()` around all dictionary mutations.

### 2. Pluggable Storage (Dependency Injection)

The middleware interacts only with the `RateLimiter` abstract base class — it never knows whether it's talking to Redis or a dictionary. At startup, the correct implementation is injected based on the `STORAGE_BACKEND` environment variable.

```python
# The middleware only sees this interface:
class RateLimiter(ABC):
    def is_allowed(self, tenant_id, endpoint, limit, window_sec) -> tuple[bool, int]: ...
    def get_metrics(self) -> dict: ...
```

### 3. Retry-After Calculation

When a request is blocked, `Retry-After` is calculated as the time until the **oldest** request in the window expires. Once that timestamp falls out of the 60s window, a slot opens up. `math.ceil()` is used to avoid sub-second precision issues, and the value is clamped to `max(1, ...)` to prevent zero or negative values that could cause client retry spam.

### 4. Redis Key Design

Keys follow the versioned format `rate:v1:{tenant}:{endpoint}` to:
- Avoid key collisions with other Redis data
- Enable key versioning for future schema migrations
- Support efficient `SCAN` pattern matching for metrics

### 5. TTL Buffer

Key expiry is set to `window_sec + 5` seconds instead of exactly `window_sec`. This prevents premature key eviction when traffic pauses briefly, which could cause phantom "slot available" signals.

---

## Configuration

Endpoint-specific limits are defined in `config.json`:

```json
{
  "window_seconds": 60,
  "default_limit": 100,
  "endpoints": {
    "/upload": 10
  }
}
```

---

## Assumptions

- Tenants are identified by the `X-Tenant-ID` header. Requests lacking this header are rejected with `400 Bad Request`.
- Endpoint limits use **exact string matching** (e.g., `/upload`). Dynamic path parameters (e.g., `/users/{id}`) would require regex route matching, which was omitted to keep scope focused.
- The `/metrics` endpoint is excluded from rate limiting (operational endpoint).

---

## Edge Cases Handled

| Edge Case | How It's Handled |
|---|---|
| Burst concurrency (same ms) | Atomic pipeline / thread lock |
| Retry-After ≤ 0 | Clamped to `max(1, ...)` |
| Redis key premature eviction | TTL buffer of +5 seconds |
| Memory growth (many tenants) | TTL auto-cleanup in Redis; deque eviction in-memory |
| Missing tenant header | 400 Bad Request |

## Edge Cases Acknowledged (Not Implemented)

| Edge Case | Notes |
|---|---|
| Clock drift (distributed) | Use Redis server time (`TIME`) for multi-instance deployments |
| Dynamic path matching | `/user/{id}` → requires regex normalization |
| Metrics at massive scale | Replace SCAN with Prometheus push or background aggregation |

---

## What I'd Do With More Time

- **Lua Scripting:** Move the Redis pipeline into a Lua script for true server-side atomicity and reduced network round-trips.
- **Wildcard Route Matching:** Support patterns like `/api/v1/*` in config.
- **Prometheus Integration:** Export metrics via `/metrics` in Prometheus format instead of JSON.
- **Distributed Clock Sync:** Use Redis `TIME` command instead of local `time.time()` for multi-instance consistency.
- **Rate Limit Headers:** Add `X-RateLimit-Limit`, `X-RateLimit-Remaining`, and `X-RateLimit-Reset` headers to all responses.
