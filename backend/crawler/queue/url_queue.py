"""
Redis-backed URL queue with in-memory fallback.

Keys used per website (Redis):
  zoogle:queue:{website_id}  — Redis list  (FIFO queue of pending URLs)
  zoogle:seen:{website_id}   — Redis set   (all URLs ever pushed)

If Redis is unavailable, the queue falls back to an in-memory list.
A warning is logged but the system continues running.
"""
from __future__ import annotations

import logging
import os
from collections import deque
from typing import List, Optional

logger = logging.getLogger(__name__)

# ── In-memory fallback ────────────────────────────────────────────────────────

class _InMemoryQueue:
    """Thread-unsafe in-memory queue used when Redis is unavailable."""

    def __init__(self):
        self._queue: deque = deque()
        self._seen: set = set()

    def sadd(self, key, *values):
        added = 0
        for v in values:
            if v not in self._seen:
                self._seen.add(v)
                added += 1
        return added

    def rpush(self, key, *values):
        for v in values:
            self._queue.append(v)

    def lpop(self, key):
        try:
            return self._queue.popleft()
        except IndexError:
            return None

    def llen(self, key):
        return len(self._queue)

    def scard(self, key):
        return len(self._seen)

    def lrange(self, key, start, end):
        lst = list(self._queue)
        if end == -1:
            return lst[start:]
        return lst[start:end + 1]

    def delete(self, *keys):
        self._queue.clear()
        self._seen.clear()

    def expire(self, key, ttl):
        pass  # no-op for in-memory

    def pipeline(self):
        return _InMemoryPipeline(self)


class _InMemoryPipeline:
    def __init__(self, backend: _InMemoryQueue):
        self._backend = backend
        self._cmds: list = []

    def sadd(self, key, *values):
        self._cmds.append(("sadd", key, values))
        return self

    def lpop(self, key):
        self._cmds.append(("lpop", key))
        return self

    def execute(self):
        results = []
        for cmd, key, *args in self._cmds:
            if cmd == "sadd":
                results.append(self._backend.sadd(key, *args[0]))
            elif cmd == "lpop":
                results.append(self._backend.lpop(key))
        return results


# ── Redis connection helper ───────────────────────────────────────────────────

def _make_redis(redis_url: str):
    """
    Connect to Redis using the provided URL.
    Returns (redis_client, is_real_redis).
    Falls back to in-memory queue if Redis is unavailable.
    """
    url = redis_url or os.getenv("REDIS_URL", "redis://localhost:6379/0")

    try:
        import redis as _redis
        client = _redis.from_url(url, decode_responses=True, socket_connect_timeout=3)
        client.ping()   # confirm connection works
        logger.info("URLQueue: Redis connected at %s", url)
        return client, True
    except Exception as exc:
        logger.warning(
            "URLQueue: Redis unavailable (%s) — using in-memory fallback. "
            "URL collection will work but won't persist across restarts.",
            exc,
        )
        return _InMemoryQueue(), False


# ── Public queue class ────────────────────────────────────────────────────────

class URLQueue:
    def __init__(self, redis_url: str, website_id: int, ttl: int = 86400):
        self._r, self._is_redis = _make_redis(redis_url)
        self.website_id = website_id
        self._queue_key = f"zoogle:queue:{website_id}"
        self._seen_key  = f"zoogle:seen:{website_id}"
        self._ttl = ttl

    # ── Write ──────────────────────────────────────────────────────────────────

    def push(self, url: str) -> bool:
        """Push a URL onto the queue. Returns True if new, False if duplicate."""
        if self._r.sadd(self._seen_key, url):
            self._r.rpush(self._queue_key, url)
            self._r.expire(self._queue_key, self._ttl)
            self._r.expire(self._seen_key,  self._ttl)
            return True
        return False

    def push_many(self, urls: List[str]) -> int:
        """Push multiple URLs. Returns count of newly enqueued URLs."""
        if not urls:
            return 0
        pipe = self._r.pipeline()
        for url in urls:
            pipe.sadd(self._seen_key, url)
        results = pipe.execute()

        new_urls = [url for url, added in zip(urls, results) if added]
        if new_urls:
            self._r.rpush(self._queue_key, *new_urls)
            self._r.expire(self._queue_key, self._ttl)
            self._r.expire(self._seen_key,  self._ttl)
        return len(new_urls)

    # ── Read ───────────────────────────────────────────────────────────────────

    def pop(self) -> Optional[str]:
        return self._r.lpop(self._queue_key)

    def pop_many(self, count: int = 100) -> List[str]:
        pipe = self._r.pipeline()
        for _ in range(count):
            pipe.lpop(self._queue_key)
        return [url for url in pipe.execute() if url is not None]

    # ── Info ───────────────────────────────────────────────────────────────────

    def size(self) -> int:
        return self._r.llen(self._queue_key)

    def seen_count(self) -> int:
        return self._r.scard(self._seen_key)

    def is_empty(self) -> bool:
        return self.size() == 0

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    def clear(self) -> None:
        self._r.delete(self._queue_key, self._seen_key)

    def get_all_urls(self) -> List[str]:
        return self._r.lrange(self._queue_key, 0, -1)
