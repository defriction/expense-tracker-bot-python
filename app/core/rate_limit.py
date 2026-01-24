from __future__ import annotations

import time
from collections import defaultdict, deque
import os
from threading import Lock
from typing import Deque, Dict

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover - optional dependency at runtime
    redis = None


class RateLimiter:
    def __init__(self) -> None:
        self._events: Dict[str, Deque[float]] = defaultdict(deque)
        self._lock = Lock()
        self._redis = None
        self._redis_available = None

    def _get_redis(self):
        if self._redis_available is False:
            return None
        if self._redis is not None:
            return self._redis
        if redis is None:
            self._redis_available = False
            return None
        url = os.getenv("REDIS_URL", "").strip()
        if not url:
            self._redis_available = False
            return None
        try:
            client = redis.Redis.from_url(url, decode_responses=True)
            client.ping()
            self._redis = client
            self._redis_available = True
            return client
        except Exception:
            self._redis_available = False
            return None

    def allow(self, key: str, limit: int, window_seconds: int) -> bool:
        if limit <= 0:
            return True
        client = self._get_redis()
        if client is not None:
            window = int(time.time() // window_seconds)
            bucket_key = f"rl:{key}:{window}"
            try:
                count = client.incr(bucket_key)
                if count == 1:
                    client.expire(bucket_key, window_seconds)
                return count <= limit
            except Exception:
                # Fall back to in-memory limiter on Redis errors.
                pass

        now = time.monotonic()
        cutoff = now - window_seconds
        with self._lock:
            bucket = self._events[key]
            while bucket and bucket[0] < cutoff:
                bucket.popleft()
            if len(bucket) >= limit:
                return False
            bucket.append(now)
            return True


rate_limiter = RateLimiter()
