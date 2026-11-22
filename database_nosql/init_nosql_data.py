import json
import os
from pathlib import Path

import redis
from pymongo import MongoClient

import mongoDatabase
import redisDatabase
import kafkaDatabase


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

def init_nosql_data():
    print("Initializing FoodGo NoSQL databases using mock_data.json...")

    # Load JSON data
    data = load_mock_data()
    print(f"Loaded mock_data.json successfully ({len(data)} entity groups).")

    # MongoDB
    print("\n[1/3] MongoDB (primary node direct connection)...")
    mongo_uri = os.getenv("MONGO_PRIMARY_URI", "mongodb://localhost:27017/?directConnection=true")
    mongo_client = MongoClient(mongo_uri)
    mongoDatabase.clear_and_init_mongodb(mongo_client, data)
    mongo_client.close()

    # Redis
    print("\n[2/3] Redis...")
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", 6379))
    r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
    redisDatabase.init_redis(r, data)

    # Kafka
    print("\n[3/3] Kafka...")
    kafkaDatabase.init_kafka(data)

    print("\nDone. All NoSQL mock data initialized successfully from JSON.")

def main() -> None:
    init_nosql_data()


if __name__ == "__main__":
    main()


if __name__ == "__main__":
    main()
