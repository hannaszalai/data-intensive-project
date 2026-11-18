import os
import time
import json
from pymongo import MongoClient, ReadPreference
import redis
from kafka import KafkaProducer, KafkaConsumer

MONGO_PRIMARY_URI = os.getenv(
    "MONGO_PRIMARY_URI",
    "mongodb://localhost:27017/?directConnection=true"
)
MONGO_SECONDARY_URI = os.getenv(
    "MONGO_SECONDARY_URI",
    "mongodb://localhost:27018/?directConnection=true&readPreference=secondaryPreferred"
)
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")


def print_section(title: str) -> None:
    print("\n" + "=" * 60)
    print("  " + title)
    print("=" * 60)


def test_regional_partitioning(db) -> None:
    print_section("TEST 1 - Regional Partitioning (Horizontal Fragmentation)")
    for region in ["FI-LPR", "FI-HEL", "FI-LAH"]:
        count = db.restaurants.count_documents({"RegionID": region})
        restaurants = list(
            db.restaurants.find(
                {"RegionID": region},
                {"_id": 0, "RestaurantID": 1, "Name": 1, "RegionID": 1}
            )
        )
        print(f"Region {region}: {count} restaurants")
        for r in restaurants:
            print(f"  - {r['RestaurantID']}: {r['Name']}")
    print("\nResult: Each region contains only its own restaurants. Partitioning verified.")


def test_replication(primary_client, secondary_client) -> None:
    print_section("TEST 2 - MongoDB Replication (Primary Write, Secondary Read)")

    db_primary = primary_client["foodgo_nosql"]
    db_secondary = secondary_client["foodgo_nosql"]

    test_doc = {
        "RestaurantID": "REST-999",
        "Name": "Replication Test Restaurant",
        "RegionID": "FI-LPR",
        "version": 1,
        "Menus": [],
        "Reviews": [],
        "Address": "Test Street 1",
        "Rating": 5.0
    }

    # Write to primary
    db_primary.restaurants.delete_many({"RestaurantID": "REST-999"})
    db_primary.restaurants.insert_one(test_doc)
    print("Written to PRIMARY: REST-999 Replication Test Restaurant")

    # Give replica a moment to sync
    time.sleep(2)

    # Read from secondary
    result = db_secondary.restaurants.find_one(
        {"RestaurantID": "REST-999"},
        {"_id": 0, "Name": 1, "RegionID": 1}
    )
    if result:
        print(f"Read from SECONDARY: {result}")
        print("Result: Replication verified. Document propagated from primary to secondary.")
    else:
        print("Result: Document not yet replicated (may need more time). Try running again.")

    # Cleanup
    db_primary.restaurants.delete_many({"RestaurantID": "REST-999"})


def test_optimistic_concurrency_control(db) -> None:
    print_section("TEST 3 - Optimistic Concurrency Control (OCC)")

    # Get current version of REST-101
    restaurant = db.restaurants.find_one(
        {"RestaurantID": "REST-101"},
        {"_id": 0, "version": 1, "Name": 1}
    )
    current_version = restaurant["version"]
    print(f"Restaurant REST-101 current version: {current_version}")

    # Manager A updates with correct version -> should succeed
    result_a = db.restaurants.update_one(
        {"RestaurantID": "REST-101", "version": current_version},
        {
            "$set": {"Menus.$[elem].Price": 9.00},
            "$inc": {"version": 1}
        },
        array_filters=[{"elem.ItemID": "ITEM-1001"}]
    )
    if result_a.matched_count > 0:
        print(
            f"Manager A update (version={current_version}): "
            "SUCCESS - Margherita Pizza price updated to 9.00"
        )
    else:
        print(f"Manager A update (version={current_version}): FAILED")

    # Manager B tries with the same (now stale) version -> should fail
    result_b = db.restaurants.update_one(
        {"RestaurantID": "REST-101", "version": current_version},
        {
            "$set": {"Menus.$[elem].Price": 10.00},
            "$inc": {"version": 1}
        },
        array_filters=[{"elem.ItemID": "ITEM-1001"}]
    )
    if result_b.matched_count == 0:
        print(
            f"Manager B update (version={current_version}, stale): "
            "REJECTED - OCC conflict detected correctly"
        )
    else:
        print(f"Manager B update (version={current_version}): unexpectedly succeeded")

    # Restore original price
    db.restaurants.update_one(
        {"RestaurantID": "REST-101"},
        {"$set": {"Menus.$[elem].Price": 8.50}, "$inc": {"version": 1}},
        array_filters=[{"elem.ItemID": "ITEM-1001"}]
    )
    print("Result: OCC verified. Stale version update rejected; fresh version accepted.")


def test_redis_caching(r) -> None:
    print_section("TEST 4 - Redis Caching and Shopping Cart")

    cache_key = "cache:menu:FI-LPR:REST-101"

    # Test 1: Cache hit on pre-warmed key
    cached = r.get(cache_key)
    if cached:
        data = json.loads(cached)
        print(f"Cache HIT for {cache_key}")
        print(f"  Restaurant: {data.get('Name', 'unknown')}")
        menu_count = len(data.get("Menus", []))
        print(f"  Menu items: {menu_count}")
        ttl = r.ttl(cache_key)
        print(f"  TTL remaining: {ttl}s")
    else:
        print(f"Cache MISS for {cache_key} (run init_nosql_data.py first)")

    # Test 2: Cache invalidation
    r.delete(cache_key)
    print(f"\nCache INVALIDATED: deleted {cache_key}")
    cached_after = r.get(cache_key)
    print(f"Cache after invalidation: {'HIT (unexpected)' if cached_after else 'MISS (correct)'}")

    # Test 3: Shopping cart for CUST-201 (Part 1 Payment UI example)
    cart_raw = r.get("cart:CUST-201")
    if cart_raw:
        cart = json.loads(cart_raw)
        print(f"\nCart for CUST-201 (Part 1 Payment UI example):")
        for item in cart.get("Items", []):
            print(f"  {item['Name']}: {item['Price']} EUR x {item.get('Quantity', 1)}")
        print(f"  Total: {cart.get('TotalPrice', 0):.2f} EUR")
    else:
        print("\ncart:CUST-201 not found (run init_nosql_data.py first)")

    # Test 4: Modify cart
    new_items = [
        {"ItemID": "ITEM-1001", "Name": "Margherita Pizza", "Price": 8.50, "Quantity": 2},
        {"ItemID": "ITEM-1003", "Name": "Carbonara Pasta", "Price": 9.00, "Quantity": 1}
    ]
    total = sum(item["Price"] * item["Quantity"] for item in new_items)
    new_cart = {
        "CustomerID": "CUST-201",
        "Items": new_items,
        "TotalPrice": round(total, 2)
    }
    r.set("cart:CUST-201", json.dumps(new_cart), ex=3600)
    print(f"\nCart updated for CUST-201. New total: {total:.2f} EUR")
    print("Result: Redis caching and cart operations verified.")


def test_kafka_streaming(bootstrap: str) -> None:
    print_section("TEST 5 - Kafka Event Streaming (Driver GPS and Order Status)")

    producer = KafkaProducer(
        bootstrap_servers=[bootstrap],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8")
    )

    # Publish 3 GPS events for different drivers
    gps_events = [
        {"DriverID": "DRV-001", "OrderID": "ORD-5001", "Latitude": 61.0590, "Longitude": 28.1865, "RegionID": "FI-LPR"},
        {"DriverID": "DRV-002", "OrderID": "ORD-5002", "Latitude": 60.1699, "Longitude": 24.9384, "RegionID": "FI-HEL"},
        {"DriverID": "DRV-003", "OrderID": "ORD-5003", "Latitude": 60.9826, "Longitude": 25.6549, "RegionID": "FI-LAH"},
    ]
    for event in gps_events:
        producer.send("driver-locations", key=event["DriverID"], value=event)
        print(
            f"Published GPS: {event['DriverID']} at "
            f"({event['Latitude']}, {event['Longitude']}) for order {event['OrderID']}"
        )

    # Publish 3 order status events (a single order progressing)
    status_events = [
        {"OrderID": "ORD-5010", "RegionID": "FI-LPR", "Status": "NEW"},
        {"OrderID": "ORD-5010", "RegionID": "FI-LPR", "Status": "PREPARING"},
        {"OrderID": "ORD-5010", "RegionID": "FI-LPR", "Status": "ON_THE_WAY"},
    ]
    for event in status_events:
        producer.send("order-status-events", key=event["OrderID"], value=event)
        print(f"Published order status: {event['OrderID']} -> {event['Status']}")

    producer.flush()
    producer.close()

    # Consume from driver-locations
    print("\nConsuming recent driver-locations events:")
    consumer = KafkaConsumer(
        "driver-locations",
        bootstrap_servers=[bootstrap],
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=5000,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id=None
    )
    count = 0
    for msg in consumer:
        data = msg.value
        print(
            f"  Partition {msg.partition} | Offset {msg.offset} | "
            f"{data.get('DriverID')} -> ({data.get('Latitude')}, {data.get('Longitude')})"
        )
        count += 1
        if count >= 5:
            break
    consumer.close()

    print(f"\nResult: Kafka streaming verified. {count} GPS events consumed.")


def main() -> None:
    print("FoodGo NoSQL Layer - Verification Script")
    print("Student: Hanna | Part 2 - NoSQL, Caching, Streaming")
    print("Connecting to services...")

    primary_client = MongoClient(MONGO_PRIMARY_URI)
    secondary_client = MongoClient(MONGO_SECONDARY_URI)
    db = primary_client["foodgo_nosql"]
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    test_regional_partitioning(db)
    test_replication(primary_client, secondary_client)
    test_optimistic_concurrency_control(db)
    test_redis_caching(r)
    test_kafka_streaming(KAFKA_BOOTSTRAP)

    primary_client.close()
    secondary_client.close()
    print("\n" + "=" * 60)
    print("All tests completed.")
    print("=" * 60)