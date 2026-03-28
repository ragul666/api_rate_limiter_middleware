"""
Rate Limiter Module — Sliding Window Log Algorithm

Implements a pluggable storage architecture with two backends:
  - RedisLimiter:    Uses Redis Sorted Sets (ZSET) with atomic pipelines.
  - InMemoryLimiter: Uses deques protected by threading locks.

The Sliding Window Log was chosen over:
  - Fixed Window Counter: Suffers from "thundering herd" at window boundaries.
  - Token/Leaky Bucket: Doesn't align with strict 60s sliding window requirement.
  - Sliding Window Counter: Approximate; unnecessary when limit is low (100/min).

Key Design Decisions:
  1. Single atomic pipeline (no check-then-insert gap) to prevent race conditions.
  2. Key versioning (rate:v1:...) for future-proofing.
  3. TTL buffer (+5s) to avoid premature key eviction.
  4. Deque for in-memory backend — O(1) popleft vs O(n) list rebuild.
"""

import math
import time
import threading
from abc import ABC, abstractmethod
from collections import defaultdict, deque

import redis


class RateLimiter(ABC):
    """Abstract base class defining the rate limiter interface.

    All storage backends must implement these methods. The middleware
    interacts exclusively through this interface — it never knows
    whether it's talking to Redis or a dictionary.
    """

    @abstractmethod
    def is_allowed(
        self, tenant_id: str, endpoint: str, limit: int, window_sec: int
    ) -> tuple[bool, int]:
        """Check if a request is allowed under the rate limit.

        Args:
            tenant_id:  Unique identifier for the tenant.
            endpoint:   The request path (e.g., "/upload").
            limit:      Maximum requests allowed in the window.
            window_sec: Size of the sliding window in seconds.

        Returns:
            A tuple of (is_allowed, retry_after_seconds).
            retry_after_seconds is 0 if request is allowed, otherwise
            the number of seconds until a slot opens up.
        """
        pass

    @abstractmethod
    def get_metrics(self) -> dict:
        """Return current request counts per tenant.

        Returns:
            Dict mapping tenant_id -> total active request count
            across all endpoints within the current window.
        """
        pass


class InMemoryLimiter(RateLimiter):
    """Thread-safe in-memory rate limiter using deques.

    Uses collections.deque for efficient O(1) eviction of expired
    timestamps from the left side, rather than rebuilding a new list
    on every request (which would be O(n)).

    Thread safety is ensured via threading.Lock around all mutations.
    """

    def __init__(self):
        self.logs: dict[str, deque] = defaultdict(deque)
        self.lock = threading.Lock()

    def is_allowed(
        self, tenant_id: str, endpoint: str, limit: int, window_sec: int
    ) -> tuple[bool, int]:
        key = f"rate:v1:{tenant_id}:{endpoint}"
        current_time = time.time()
        window_start = current_time - window_sec

        with self.lock:
            logs = self.logs[key]

            # Evict expired timestamps from the left (O(1) per pop)
            while logs and logs[0] <= window_start:
                logs.popleft()

            if len(logs) < limit:
                logs.append(current_time)
                return True, 0
            else:
                # Retry-After = time until the oldest request expires
                oldest_ts = logs[0]
                retry_after = math.ceil((oldest_ts + window_sec) - current_time)
                return False, max(1, retry_after)

    def get_metrics(self) -> dict:
        metrics: dict[str, int] = defaultdict(int)
        current_time = time.time()

        with self.lock:
            for key, timestamps in self.logs.items():
                # Key format: rate:v1:{tenant}:{endpoint}
                parts = key.split(":")
                if len(parts) >= 3:
                    tenant = parts[2]
                else:
                    tenant = "unknown"

                # Count only non-expired timestamps
                count = sum(1 for ts in timestamps if ts > current_time - 60)
                metrics[tenant] += count

        return dict(metrics)


class RedisLimiter(RateLimiter):
    """Redis-backed rate limiter using Sorted Sets (ZSET).

    Each tenant+endpoint combination maps to a ZSET where:
      - Score = timestamp in milliseconds
      - Member = stringified timestamp (unique per ms)

    Atomicity is achieved by performing ALL operations (cleanup,
    insert, count, TTL) in a SINGLE pipeline execution. This
    eliminates the race condition where a second request slips in
    between the "check" and "insert" steps.

    The TTL is set to window_sec + 5 seconds as a buffer to prevent
    premature key eviction when traffic pauses briefly.
    """

    def __init__(self, redis_url: str):
        self.redis = redis.from_url(redis_url, decode_responses=True)

    def is_allowed(
        self, tenant_id: str, endpoint: str, limit: int, window_sec: int
    ) -> tuple[bool, int]:
        key = f"rate:v1:{tenant_id}:{endpoint}"
        current_time_ms = int(time.time() * 1000)
        window_start_ms = current_time_ms - (window_sec * 1000)

        # ── Single atomic pipeline ──────────────────────────────────
        # This is the critical fix: we do cleanup + insert + count
        # in ONE pipeline execution. No gap between check and insert.
        pipe = self.redis.pipeline()
        pipe.zremrangebyscore(key, 0, window_start_ms)           # 0: Evict expired
        pipe.zadd(key, {str(current_time_ms): current_time_ms})  # 1: Optimistically add
        pipe.zcard(key)                                           # 2: Count total
        pipe.zrange(key, 0, 0)                                   # 3: Get oldest element
        pipe.expire(key, window_sec + 5)                          # 4: TTL with buffer
        results = pipe.execute()

        current_count = results[2]  # ZCARD result

        if current_count <= limit:
            # Request fits within the limit — we already added it
            return True, 0
        else:
            # Over limit — remove the optimistically added timestamp
            self.redis.zrem(key, str(current_time_ms))

            # Calculate Retry-After from the oldest request in the window
            oldest_elements = results[3]  # ZRANGE result
            if oldest_elements:
                oldest_ts_ms = int(oldest_elements[0])
                retry_after_ms = (oldest_ts_ms + (window_sec * 1000)) - current_time_ms
                retry_after = math.ceil(retry_after_ms / 1000)
            else:
                retry_after = 1

            return False, max(1, retry_after)

    def get_metrics(self) -> dict:
        """Aggregate request counts per tenant using SCAN.

        Note: SCAN is used instead of KEYS to avoid blocking Redis.
        For production at massive scale, metrics should be pre-aggregated
        via a background worker or pushed to a monitoring system like
        Prometheus / Grafana.
        """
        metrics: dict[str, int] = defaultdict(int)
        cursor = "0"
        while True:
            cursor, keys = self.redis.scan(
                cursor=cursor, match="rate:v1:*", count=100
            )
            for key in keys:
                # Key format: rate:v1:{tenant}:{endpoint}
                parts = key.split(":")
                if len(parts) >= 3:
                    tenant = parts[2]
                else:
                    tenant = "unknown"
                count = self.redis.zcard(key)
                metrics[tenant] += count

            if cursor == 0 or cursor == "0":
                break

        return dict(metrics)
