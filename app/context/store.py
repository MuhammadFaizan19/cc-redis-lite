import time
import collections

class Store:
    def __init__(self) -> None:
        self.store = collections.defaultdict(lambda: (None, None))
    
    def save(self, key: str, value: str, ttl: int = None):
        self.store[key] = (value, ttl)
    
    def get(self, key: str) -> str | None:
        return self.store[key][0] if not self.is_expired(key) else None

    def delete(self, key: str) -> None:
        self.store.pop(key, None)
    
    def is_expired(self, key: str) -> bool:
        ttl = self.store[key][1]
        if ttl and ttl < time.time() * 1000:
            self.delete(key)
            return True
        return False

    def exists(self, key: str) -> bool:
        return self.get(key) is not None
    
    def keys(self) -> list[str]:
        return list(self.store.keys())
    
    def flush(self) -> None:
        self.store.clear()
