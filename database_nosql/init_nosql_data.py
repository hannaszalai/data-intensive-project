import json
import os
import sys
import time
from pathlib import Path

from pymongo import MongoClient
import redis
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic


def load_mock_data(file_path: str = None) -> dict:
    if file_path is None:
        file_path = Path(__file__).resolve().parent / "mock_data.json"
    else:
        file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"Mock data file not found at: {file_path}")

    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    return data


def clear_and_init_mongodb(client: MongoClient, data: dict) -> None:
    print("  Dropping existing foodgo_nosql database...")
    client.drop_database("foodgo_nosql")
    db = client["foodgo_nosql"]

    # --- Restaurants collection ---
    restaurants = data.get("restaurants", [])
    restaurants_col = db["restaurants"]
    print(f"  Inserting {len(restaurants)} restaurant documents from JSON...")
    # Use shallow copies to prevent insert_many from attaching _id in-place
    restaurants_col.insert_many([dict(r) for r in restaurants])

    restaurants_col.create_index("RegionID")
    restaurants_col.create_index("RestaurantID", unique=True)
    print("  Indexes created: RegionID, RestaurantID (unique).")

    total_items = sum(len(r.get("Menus", [])) for r in restaurants)
    total_reviews = sum(len(r.get("Reviews", [])) for r in restaurants)
    print(f"  Total menu items: {total_items}, Total reviews: {total_reviews}")

    # --- Driver locations history collection ---
    locations = data.get("driver_locations_history", [])
    locations_col = db["driver_locations_history"]
    print(f"  Inserting {len(locations)} driver location documents from JSON...")
    locations_col.insert_many([dict(loc) for loc in locations])

    print("  MongoDB initialization complete.")


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


def init_kafka(data: dict) -> None:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    topic_names = ["driver-locations", "order-status-events"]

    # --- Create partitioned topics ---
    admin = KafkaAdminClient(bootstrap_servers=[bootstrap])

    existing = admin.list_topics()
    topics_to_delete = [t for t in topic_names if t in existing]
    if topics_to_delete:
        print(f"  Deleting existing topics: {topics_to_delete}")
        admin.delete_topics(topics_to_delete)
        time.sleep(2)

    new_topics = [
        NewTopic(name="driver-locations",    num_partitions=3, replication_factor=1),
        NewTopic(name="order-status-events", num_partitions=3, replication_factor=1),
    ]
    admin.create_topics(new_topics=new_topics)
    admin.close()
    print("  Kafka topics created: driver-locations (3p), order-status-events (3p).")

    time.sleep(1)

    producer = KafkaProducer(
        bootstrap_servers=[bootstrap],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )

    # --- GPS location events ---
    gps_events = data.get("gps_events", [])
    print(f"  Publishing {len(gps_events)} GPS events to driver-locations from JSON...")
    for event in gps_events:
        producer.send("driver-locations", key=event["DriverID"], value=event)

    # --- Order status events ---
    status_events = data.get("order_status_events", [])
    print(f"  Publishing {len(status_events)} order status events to order-status-events from JSON...")
    for event in status_events:
        producer.send("order-status-events", key=event["OrderID"], value=event)

    producer.flush()
    producer.close()
    print("  Kafka initialization complete.")


def main() -> None:
    print("Initializing FoodGo NoSQL databases using mock_data.json...")

    # Load JSON data
    data = load_mock_data()
    print(f"Loaded mock_data.json successfully ({len(data)} entity groups).")

    # MongoDB
    print("\n[1/3] MongoDB (primary node direct connection)...")
    mongo_uri = os.getenv("MONGO_PRIMARY_URI", "mongodb://localhost:27017/?directConnection=true")
    mongo_client = MongoClient(mongo_uri)
    clear_and_init_mongodb(mongo_client, data)
    mongo_client.close()

    # Redis
    print("\n[2/3] Redis...")
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", 6379))
    r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
    init_redis(r, data)

    # Kafka
    print("\n[3/3] Kafka...")
    init_kafka(data)

    print("\nDone. All NoSQL mock data initialized successfully from JSON.")


if __name__ == "__main__":
    main()


if __name__ == "__main__":
    main()
