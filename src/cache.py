import hashlib
import time
from threading import Lock


class QueryCache:
    """Simple in-memory TTL cache for SQL query results."""

    def __init__(self, ttl: int = 300):
        self._store: dict[str, tuple[float, object]] = {}
        self._ttl = ttl
        self._lock = Lock()
        self._hits = 0
        self._misses = 0

    def _key(self, question: str, schema_version: str) -> str:
        raw = f"{question}||{schema_version}"
        return hashlib.sha256(raw.encode()).hexdigest()

    def get(self, question: str, schema_version: str) -> object | None:
        key = self._key(question, schema_version)
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                self._misses += 1
                return None
            expires_at, value = entry
            if time.monotonic() > expires_at:
                del self._store[key]
                self._misses += 1
                return None
            self._hits += 1
            return value

    def set(self, question: str, schema_version: str, value: object) -> None:
        key = self._key(question, schema_version)
        with self._lock:
            self._store[key] = (time.monotonic() + self._ttl, value)

    def clear(self) -> None:
        with self._lock:
            self._store.clear()

    def stats(self) -> dict:
        with self._lock:
            return {
                "size": len(self._store),
                "hits": self._hits,
                "misses": self._misses,
                "ttl": self._ttl,
            }
