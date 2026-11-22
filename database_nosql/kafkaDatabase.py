import json
import os
import time
import datetime

from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP = "localhost:9092"
DB_NAME = "foodgo_nosql"
CACHE_TTL = 300  # seconds


# ---------------------------------------------------------------------------
# Module-level connection handles (lazy singletons)
# ---------------------------------------------------------------------------

_kafka_producer = None



def _get_producer():
    global _kafka_producer
    if _kafka_producer is None:
        _kafka_producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BOOTSTRAP],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None
        )
    return _kafka_producer

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
