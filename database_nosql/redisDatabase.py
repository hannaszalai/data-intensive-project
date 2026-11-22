import json
import uuid

import redis
import datetime

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
REDIS_HOST = "localhost"
REDIS_PORT = 6379

# ---------------------------------------------------------------------------
# Module-level connection handles (lazy singletons)
# ---------------------------------------------------------------------------

_redis_client = None

def _get_redis():
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            decode_responses=True
        )
    return _redis_client

def init_redis(r: redis.Redis, data: dict) -> None:
    print("  Flushing Redis database...")
    r.flushdb()

    # --- Pre-warmed cached menus ---
    restaurants = data.get("restaurants", [])
    print("  Pre-warming region-aware menu cache from JSON...")
    for restaurant in restaurants:
        key = f"cache:menu:{restaurant['RegionID']}:{restaurant['RestaurantID']}"
        doc = {k: v for k, v in restaurant.items() if k != "_id"}
        r.set(key, json.dumps(doc), ex=300)
    print(f"  Cached menus for {len(restaurants)} restaurants (TTL 300s).")

    # --- Shopping carts ---
    carts = data.get("shopping_carts", [])
    print(f"  Loading {len(carts)} customer shopping carts from JSON...")
    for cart in carts:
        key = f"cart:{cart['CustomerID']}"
        r.set(key, json.dumps(cart))
    print(f"  Loaded {len(carts)} shopping carts (e.g. cart:CUST-201 matches Payment UI).")

    # --- Session tokens ---
    sessions = data.get("session_tokens", [])
    print(f"  Loading {len(sessions)} session tokens from JSON...")
    for session in sessions:
        key = f"session:{session['UserID']}"
        r.set(key, json.dumps(session), ex=3600)
    print(f"  Loaded {len(sessions)} session tokens (TTL 3600s).")

    print("  Redis initialization complete.")

# ---------------------------------------------------------------------------
# Redis - Shopping cart functions
# ---------------------------------------------------------------------------

def get_cart(customer_id: str) -> dict:
    r = _get_redis()
    raw = r.get(f"cart:{customer_id}")
    if raw is None:
        return {}
    return json.loads(raw)


def update_cart(customer_id: str, items: list) -> dict:
    total = sum(item["Price"] * item["Quantity"] for item in items)
    cart = {
        "CustomerID": customer_id,
        "Items": items,
        "TotalPrice": round(total, 2),
        "UpdatedAt": datetime.datetime.utcnow().isoformat()
    }
    r = _get_redis()
    r.setex(f"cart:{customer_id}", 3600, json.dumps(cart))
    return cart

# ---------------------------------------------------------------------------
# Redis - Distributed locking
# ---------------------------------------------------------------------------

def acquire_distributed_lock(lock_key: str, timeout_ms: int = 5000) -> bool:
    r = _get_redis()
    token = str(uuid.uuid4())
    acquired = r.set(f"lock:{lock_key}", token, nx=True, px=timeout_ms)
    return acquired is True


def release_distributed_lock(lock_key: str) -> bool:
    r = _get_redis()
    deleted = r.delete(f"lock:{lock_key}")
    return deleted > 0