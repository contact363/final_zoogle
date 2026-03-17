"""
Redis-backed URL queue with built-in deduplication.

Keys used per website:
  zoogle:queue:{website_id}  — Redis list  (FIFO queue of pending URLs)
  zoogle:seen:{website_id}   — Redis set   (all URLs ever pushed)
"""
from __future__ import annotations

import redis
from typing import List, Optional


class URLQueue:
    def __init__(self, redis_url: str, website_id: int, ttl: int = 86400):
        self._r = redis.from_url(redis_url, decode_responses=True)
        self.website_id = website_id
        self._queue_key = f"zoogle:queue:{website_id}"
        self._seen_key  = f"zoogle:seen:{website_id}"
        self._ttl = ttl

    # ── Write ──────────────────────────────────────────────────────────────────

    def push(self, url: str) -> bool:
        """Push a URL onto the queue.  Returns True if new, False if duplicate."""
        if self._r.sadd(self._seen_key, url):
            self._r.rpush(self._queue_key, url)
            self._r.expire(self._queue_key, self._ttl)
            self._r.expire(self._seen_key,  self._ttl)
            return True
        return False

    def push_many(self, urls: List[str]) -> int:
        """Push multiple URLs.  Returns count of newly enqueued URLs."""
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
        """Pop the next URL from the queue (blocking-safe, non-blocking call)."""
        return self._r.lpop(self._queue_key)

    def pop_many(self, count: int = 100) -> List[str]:
        """Pop up to `count` URLs at once."""
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
        """Delete all queue and seen-set data for this website."""
        self._r.delete(self._queue_key, self._seen_key)

    def get_all_urls(self) -> List[str]:
        """Return all queued URLs without removing them (for inspection)."""
        return self._r.lrange(self._queue_key, 0, -1)
