import json
import uuid
import datetime

from pymongo import MongoClient, ReadPreference
import redis
from kafka import KafkaProducer, KafkaConsumer


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MONGO_PRIMARY_URI = "mongodb://localhost:27017/?directConnection=true"
MONGO_SECONDARY_URI = "mongodb://localhost:27018/?directConnection=true&readPreference=secondaryPreferred"
REDIS_HOST = "localhost"
REDIS_PORT = 6379
KAFKA_BOOTSTRAP = "localhost:9092"
DB_NAME = "foodgo_nosql"
CACHE_TTL = 300  # seconds


# ---------------------------------------------------------------------------
# Module-level connection handles (lazy singletons)
# ---------------------------------------------------------------------------

_mongo_primary = None
_mongo_secondary = None
_redis_client = None
_kafka_producer = None


def _get_mongo_client(use_replica: bool = False):
    global _mongo_primary, _mongo_secondary
    if use_replica:
        if _mongo_secondary is None:
            _mongo_secondary = MongoClient(MONGO_SECONDARY_URI)
        return _mongo_secondary
    else:
        if _mongo_primary is None:
            _mongo_primary = MongoClient(MONGO_PRIMARY_URI)
        return _mongo_primary


def _get_redis():
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            decode_responses=True
        )
    return _redis_client


def _get_producer():
    global _kafka_producer
    if _kafka_producer is None:
        _kafka_producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BOOTSTRAP],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None
        )
    return _kafka_producer


# ---------------------------------------------------------------------------
# MongoDB - Restaurant and Menu functions
# ---------------------------------------------------------------------------

def get_restaurants_by_region(region_id: str, use_replica: bool = True) -> list:
    r = _get_redis()
    cache_key = f"cache:region:{region_id}"

    cached = r.get(cache_key)
    if cached:
        return json.loads(cached)

    # Cache miss - query MongoDB
    client = _get_mongo_client(use_replica=use_replica)
    db = client[DB_NAME]

    cursor = db.restaurants.find({"RegionID": region_id}, {"_id": 0})
    results = []
    for doc in cursor:
        # Convert any ObjectId or datetime fields that are not JSON-serializable
        results.append(_make_serializable(doc))

    r.setex(cache_key, CACHE_TTL, json.dumps(results))
    return results


def get_restaurant_menu(restaurant_id: str, region_id: str) -> dict:
    r = _get_redis()
    cache_key = f"cache:menu:{region_id}:{restaurant_id}"

    cached = r.get(cache_key)
    if cached:
        return json.loads(cached)

    client = _get_mongo_client()
    db = client[DB_NAME]
    doc = db.restaurants.find_one(
        {"RestaurantID": restaurant_id, "RegionID": region_id},
        {"_id": 0}
    )
    if doc is None:
        return {}

    result = _make_serializable(doc)
    r.setex(cache_key, CACHE_TTL, json.dumps(result))
    return result


def update_menu_item_optimistic(
    restaurant_id: str,
    region_id: str,
    item_id: str,
    updates: dict,
    expected_version: int
) -> bool:
    client = _get_mongo_client()
    db = client[DB_NAME]

    # Build the $set targeting only the matching element in the Menus array
    set_fields = {f"Menus.$[elem].{k}": v for k, v in updates.items()}
    set_fields["version"] = {"$add": ["$version", 1]}  # placeholder, handled by $inc below

    result = db.restaurants.update_one(
        {
            "RestaurantID": restaurant_id,
            "RegionID": region_id,
            "version": expected_version
        },
        {
            "$set": {f"Menus.$[elem].{k}": v for k, v in updates.items()},
            "$inc": {"version": 1}
        },
        array_filters=[{"elem.ItemID": item_id}]
    )

    if result.matched_count == 0:
        # Version mismatch - another process already updated this document
        return False

    # Invalidate cache
    r = _get_redis()
    r.delete(f"cache:menu:{region_id}:{restaurant_id}")
    r.delete(f"cache:region:{region_id}")
    return True


def add_menu_item(restaurant_id: str, region_id: str, new_item: dict) -> bool:
    client = _get_mongo_client()
    db = client[DB_NAME]

    result = db.restaurants.update_one(
        {"RestaurantID": restaurant_id, "RegionID": region_id},
        {
            "$push": {"Menus": new_item},
            "$inc": {"version": 1}
        }
    )

    if result.matched_count == 0:
        return False

    # Invalidate cache
    r = _get_redis()
    r.delete(f"cache:menu:{region_id}:{restaurant_id}")
    r.delete(f"cache:region:{region_id}")
    return True


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


# ---------------------------------------------------------------------------
# Kafka - Publishing events
# ---------------------------------------------------------------------------

def publish_driver_location(
    driver_id: str,
    latitude: float,
    longitude: float,
    order_id: str = None
) -> bool:
    event = {
        "DriverID": driver_id,
        "OrderID": order_id,
        "Latitude": latitude,
        "Longitude": longitude,
        "Timestamp": datetime.datetime.utcnow().isoformat(),
        "EventType": "GPS_UPDATE"
    }
    try:
        producer = _get_producer()
        producer.send("driver-locations", key=driver_id, value=event)
        producer.flush()
        return True
    except Exception as exc:
        print(f"publish_driver_location failed: {exc}")
        return False


def publish_order_status_event(order_id: str, region_id: str, new_status: str) -> bool:
    event = {
        "OrderID": order_id,
        "RegionID": region_id,
        "Status": new_status,
        "Timestamp": datetime.datetime.utcnow().isoformat(),
        "EventType": "ORDER_STATUS"
    }
    try:
        producer = _get_producer()
        producer.send("order-status-events", key=order_id, value=event)
        producer.flush()
        return True
    except Exception as exc:
        print(f"publish_order_status_event failed: {exc}")
        return False


# ---------------------------------------------------------------------------
# Kafka - Consuming events
# ---------------------------------------------------------------------------

def consume_recent_events(topic: str, max_messages: int = 10) -> list:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=3000,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id=None
    )

    # Seek all partitions to the very beginning
    consumer.seek_to_beginning()

    messages = []
    for msg in consumer:
        messages.append(msg.value)
        if len(messages) >= max_messages:
            break

    consumer.close()
    return messages


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _make_serializable(doc: dict) -> dict:
    result = {}
    for key, value in doc.items():
        if isinstance(value, dict):
            result[key] = _make_serializable(value)
        elif isinstance(value, list):
            result[key] = [
                _make_serializable(item) if isinstance(item, dict) else str(item)
                if not isinstance(item, (str, int, float, bool, type(None))) else item
                for item in value
            ]
        elif isinstance(value, (str, int, float, bool, type(None))):
            result[key] = value
        else:
            # Covers ObjectId, datetime, Decimal128, etc.
            result[key] = str(value)
    return result
