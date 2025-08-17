import requests
import hashlib
from typing import Optional
import random

BASE_URL = "http://localhost:8000"  # Change to your service address

def hash_key(key: str) -> str:
    """Hash the original key into a fixed 64-char SHA-256 hex string."""
    return hashlib.sha256(key.encode("utf-8")).hexdigest()

def kv_request(key: str, value: Optional[str] = None, ttl_seconds: Optional[float] = None, n_entry: Optional[int] = 5) -> Optional[str]:
    """
    If value is None -> GET from cache.
    If value is provided -> SET to cache (optional TTL).
    
    Returns:
        - On GET: the value as string, or None if not found.
        - On SET: None.
    Raises:
        - requests.HTTPError on HTTP errors other than 404 for GET.
    """
    assert isinstance(key, str) and isinstance(value, (str, type(None)))
    rn = random.randint(0, n_entry - 1)
    key = f"{key}::entry{rn}"
    hashed_key = hash_key(key)

    if value is None:
        # GET request
        r = requests.get(f"{BASE_URL}/kv", params={"key": hashed_key}, timeout=3)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.text
    else:
        # POST request
        payload = {"key": hashed_key, "value": value, "ttl_seconds": ttl_seconds}
        r = requests.post(f"{BASE_URL}/kv", json=payload, timeout=3)
        r.raise_for_status()
        return None

# Example usage
if __name__ == "__main__":
    kv_request("my_long_original_key", "world", ttl_seconds=60)  # SET
    print(kv_request("my_long_original_key"))  # GET -> world
    print(kv_request("not_exist_key"))  # GET -> None
