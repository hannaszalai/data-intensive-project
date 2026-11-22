from pymongo import MongoClient
import datetime
import json

import redis
import serialization

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MONGO_PRIMARY_URI = "mongodb://localhost:27017/?directConnection=true"
MONGO_SECONDARY_URI = "mongodb://localhost:27018/?directConnection=true&readPreference=secondaryPreferred"
DB_NAME = "foodgo_nosql"
CACHE_TTL = 300  # seconds

# ---------------------------------------------------------------------------
# Module-level connection handles (lazy singletons)
# ---------------------------------------------------------------------------

_mongo_primary = None
_mongo_secondary = None

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

# ---------------------------------------------------------------------------
# MongoDB - Restaurant and Menu functions
# ---------------------------------------------------------------------------

def get_restaurants_by_region(region_id: str, use_replica: bool = True) -> list:
    r = redis._get_redis()
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
        results.append(serialization._make_serializable(doc))

    r.setex(cache_key, CACHE_TTL, json.dumps(results))
    return results


def get_restaurant_menu(restaurant_id: str, region_id: str) -> dict:
    r = redis._get_redis()
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

    result = serialization._make_serializable(doc)
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
    r = redis._get_redis()
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
    r = redis._get_redis()
    r.delete(f"cache:menu:{region_id}:{restaurant_id}")
    r.delete(f"cache:region:{region_id}")
    return True
