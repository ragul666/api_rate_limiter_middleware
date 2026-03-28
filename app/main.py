"""
FastAPI Application — Multi-Tenant API Rate Limiter

Wires up:
  1. Configuration loading from config.json
  2. Pluggable storage backend injection (Redis or In-Memory)
  3. Rate limiting middleware with 429 response + Retry-After header
  4. Application endpoints and metrics route
"""

import os
import json

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.limiter import RedisLimiter, InMemoryLimiter

app = FastAPI(
    title="Multi-Tenant API Rate Limiter",
    description="Per-tenant, per-endpoint rate limiting using the Sliding Window Log algorithm.",
    version="1.0.0",
)

# ── 1. Load Configuration at Startup ────────────────────────────────
with open("config.json", "r") as f:
    CONFIG = json.load(f)

# ── 2. Dependency Injection for Storage Backend ─────────────────────
# The middleware never knows whether it's talking to Redis or a dict.
# At startup, we inject the correct implementation based on environment.
STORAGE_BACKEND = os.getenv("STORAGE_BACKEND", "memory")

if STORAGE_BACKEND == "redis":
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    limiter = RedisLimiter(redis_url)
else:
    limiter = InMemoryLimiter()


# ── 3. Rate Limiter Middleware ──────────────────────────────────────
@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    """Intercept every request and enforce per-tenant rate limits.

    Flow:
      1. Skip rate limiting for /metrics (operational endpoint).
      2. Extract X-Tenant-ID header — reject if missing.
      3. Look up endpoint-specific limit from config, fallback to default.
      4. Call storage backend's is_allowed() method.
      5. If blocked, return 429 with JSON body and Retry-After header.
    """
    # Skip metrics endpoint from being rate limited
    if request.url.path == "/metrics":
        return await call_next(request)

    # Extract tenant identity from request header
    tenant_id = request.headers.get("X-Tenant-ID")
    if not tenant_id:
        return JSONResponse(
            status_code=400,
            content={"error": "Missing X-Tenant-ID header"},
        )

    endpoint = request.url.path
    window_sec = CONFIG.get("window_seconds", 60)

    # Check config for endpoint-specific limits, fallback to default
    limit = CONFIG.get("endpoints", {}).get(
        endpoint, CONFIG.get("default_limit", 100)
    )

    is_allowed, retry_after = limiter.is_allowed(
        tenant_id, endpoint, limit, window_sec
    )

    if not is_allowed:
        return JSONResponse(
            status_code=429,
            content={
                "error": "rate_limited",
                "retry_after": retry_after,
            },
            headers={"Retry-After": str(retry_after)},
        )

    response = await call_next(request)
    return response


# ── 4. Application Endpoints ───────────────────────────────────────
@app.get("/")
async def root():
    """Default endpoint — rate limited at 100 req/min per tenant."""
    return {"message": "Success! You are within your rate limit."}


@app.post("/upload")
async def upload():
    """Upload endpoint — rate limited at 10 req/min per tenant (stricter)."""
    return {"message": "Upload successful! (Stricter limit applied)"}


@app.get("/metrics")
async def get_metrics():
    """Operational metrics — not rate limited.

    Returns current request counts per tenant across all endpoints.
    For production scale, these should be pushed to Prometheus/Grafana
    rather than computed on-demand via SCAN.
    """
    return limiter.get_metrics()
