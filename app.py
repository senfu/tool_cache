from __future__ import annotations
from fastapi import FastAPI, HTTPException, Query, status
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel
import asyncio
import time
from collections import OrderedDict
from typing import Optional
from datetime import datetime

# -----------------------------
# Core: High-concurrency in-memory cache with TTL + LRU
# -----------------------------
class CacheEntry:
    __slots__ = ("value", "expire_at")
    def __init__(self, value: str, expire_at: Optional[float]):
        self.value = value
        self.expire_at = expire_at  # None means no expiration; otherwise epoch seconds

class AsyncTTL_LRUCache:
    """
    - O(1) (amortized) get/set
    - TTL expiration check
    - LRU eviction when capacity is reached
    - Async safe: a single lock for writes/evictions/maintenance; fast lock-free reads where possible
    - Periodic sweeping of expired keys to reduce overhead on hot read paths
    """
    def __init__(self, max_size: int = 200_000, sweep_interval: float = 5.0):
        self._store: dict[str, CacheEntry] = {}
        self._lru = OrderedDict()  # key -> None (order only)
        self._max_size = max_size
        self._lock = asyncio.Lock()
        self._sweep_interval = sweep_interval
        self._stop = asyncio.Event()
        self._janitor_task: Optional[asyncio.Task] = None

        # metrics
        self.hits = 0
        self.misses = 0

    def _is_expired_unlocked(self, e: CacheEntry) -> bool:
        return e.expire_at is not None and e.expire_at <= time.time()

    async def start(self):
        """Launch the background sweeper."""
        if self._janitor_task is None:
            self._stop.clear()
            self._janitor_task = asyncio.create_task(self._janitor())

    async def close(self):
        """Stop the background sweeper."""
        if self._janitor_task:
            self._stop.set()
            await self._janitor_task
            self._janitor_task = None

    async def _janitor(self):
        """Periodically remove expired keys to keep read paths lightweight."""
        while not self._stop.is_set():
            await asyncio.sleep(self._sweep_interval)
            now = time.time()
            # Bulk cleanup uses the lock
            async with self._lock:
                to_delete = []
                for k, e in self._store.items():
                    if e.expire_at is not None and e.expire_at <= now:
                        to_delete.append(k)
                for k in to_delete:
                    self._store.pop(k, None)
                    self._lru.pop(k, None)

    async def get(self, key: str) -> Optional[str]:
        """
        Fast read path:
        - Try a lock-free read of the dict (safe for CPython single op).
        - If expired, perform a locked cleanup of that key.
        - If hit, schedule an async LRU 'touch' to avoid blocking.
        """
        e = self._store.get(key)
        if not e:
            self.misses += 1
            return None

        if self._is_expired_unlocked(e):
            # Expired: remove under lock
            async with self._lock:
                e2 = self._store.get(key)
                if e2 and self._is_expired_unlocked(e2):
                    self._store.pop(key, None)
                    self._lru.pop(key, None)
            self.misses += 1
            return None

        # Hit: update LRU order asynchronously to reduce lock contention on hot keys
        self.hits += 1
        print(f"{datetime.now():%Y-%m-%d %H:%M:%S} [Cache] Hit key: {key} | Total hits: {self.hits}, misses: {self.misses} Hit rate: {self.hits / (self.hits + self.misses):.2%}")
        asyncio.create_task(self._touch(key))
        return e.value

    async def _touch(self, key: str):
        """Move the key to the end (most recently used)."""
        async with self._lock:
            if key in self._lru:
                self._lru.move_to_end(key, last=True)

    async def set(self, key: str, value: str, ttl_seconds: Optional[float] = None):
        """
        Insert or update a key:
        - If ttl_seconds is provided, store an expiration timestamp.
        - Maintain LRU and evict the least recently used key when capacity is exceeded.
        """
        expire_at = (time.time() + ttl_seconds) if ttl_seconds else None
        async with self._lock:
            if key in self._store:
                # Overwrite existing
                self._store[key].value = value
                self._store[key].expire_at = expire_at
                self._lru.move_to_end(key, last=True)
            else:
                self._store[key] = CacheEntry(value, expire_at)
                self._lru[key] = None
                if len(self._store) > self._max_size:
                    # Evict least recently used
                    old_key, _ = self._lru.popitem(last=False)
                    self._store.pop(old_key, None)

    async def delete(self, key: str):
        """Remove a key if it exists."""
        async with self._lock:
            self._store.pop(key, None)
            self._lru.pop(key, None)

    def size(self) -> int:
        """Current number of keys in the cache (not counting expired ones not yet swept)."""
        return len(self._store)

# -----------------------------
# HTTP API
# -----------------------------
app = FastAPI(title="KV Cache Service", version="1.0.0")
cache = AsyncTTL_LRUCache(max_size=200_000, sweep_interval=5.0)

class SetItem(BaseModel):
    key: str
    value: str
    ttl_seconds: Optional[float] = None  # None = never expires

@app.on_event("startup")
async def on_startup():
    await cache.start()

@app.on_event("shutdown")
async def on_shutdown():
    await cache.close()

# Read endpoint: supports /kv?key=xxx or /kv/{key}
@app.get("/kv", response_class=PlainTextResponse, summary="Get value by key")
async def get_kv(key: str = Query(..., min_length=1, max_length=4096)):
    val = await cache.get(key)
    if val is None:
        # Cache miss -> 404
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="NOT_FOUND")
    # Cache hit -> return value as plain text
    return val

@app.get("/kv/{key}", response_class=PlainTextResponse, include_in_schema=False)
async def get_kv_path(key: str):
    val = await cache.get(key)
    if val is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="NOT_FOUND")
    return val

# Optional write endpoint (remove this if you want a read-only cache)
@app.post("/kv", summary="Set key with value (optional admin)")
async def set_kv(item: SetItem):
    await cache.set(item.key, item.value, item.ttl_seconds)
    current_size = cache.size()
    print(f"{datetime.now():%Y-%m-%d %H:%M:%S} [Cache] Added key: {item.key} | Total: {current_size}")
    return {"ok": True}

# Health check & minimal metrics
@app.get("/healthz")
async def health():
    return {
        "status": "ok",
        "cache_size": cache.size(),
        "hits": cache.hits,
        "misses": cache.misses,
    }
