# FoodGo

## Overview

This directory contains the implementation of the FoodGo data layer for MongoDB (replica set), Redis (caching + distributed locks), and Apache Kafka (real-time event streaming).

## Architecture

The NoSQL layer is built around three systems, each serving a distinct role:

- **MongoDB replica set (rs0):** `mongo-primary` (27017) handles all writes; `mongo-secondary` (27018) serves reads, demonstrating replication for restaurant and menu data.
- **Redis (6379):** provides region-aware menu caching, shopping cart storage, and distributed locks using `SET NX PX`.
- **Kafka (KRaft, 9092):** two topics - `driver-locations` and `order-status-events` - each with 3 partitions, keyed by `DriverID` and `OrderID` respectively.
- **Horizontal partitioning by RegionID:** `FI-LPR` (Lappeenranta), `FI-HEL` (Helsinki), `FI-LAH` (Lahti).

## Prerequisites

- Docker Desktop installed and running
- Python 3.10+
- pip packages: `pymongo`, `redis`, `kafka-python-ng` (see `requirements.txt`)

## Setup and Running

### Step 1 - Start Docker containers

```bash
cd <project-root>
docker compose up -d --build
```

Wait about 15-20 seconds for MongoDB replica set initialization to complete (the `mongo-init` container handles this automatically).

Check containers are running:

```bash
docker compose ps
```

### Step 2 - Install Python dependencies

```bash
pip install -r requirements.txt
```

### Step 3 - Initialize mock data

```bash
python -m databases.nosql.init_nosql_data
```

This script loads all entities directly from `database_nosql/mock_data.json` and:

- Inserts 15 restaurants (103 menu items, 37 reviews) across 3 regions in MongoDB
- Inserts 50 driver location history records
- Creates indexes on `RegionID` and `RestaurantID`
- Pre-warms Redis with 15 cached menus, 10 shopping carts, 10 session tokens
- Creates Kafka topics (`driver-locations`, `order-status-events`) and publishes 45 initial events

### Step 4 - Run the verification script

```bash
python databases/nosql/test/verify_hanna_part.py
```

This runs 5 tests and prints clear output demonstrating:

- **TEST 1:** Regional partitioning (restaurants filtered by `RegionID`)
- **TEST 2:** MongoDB replication (write to primary, read from secondary)
- **TEST 3:** Optimistic Concurrency Control (stale version rejected)
- **TEST 4:** Redis caching (cache miss -> hit -> invalidation) and cart operations
- **TEST 5:** Kafka streaming (GPS events and order status events)

## Stopping Containers

```bash
docker compose down
# To also remove volumes (delete all data):
docker compose down -v
```

### Mock Data Summary Table

| Entity / Collection | Count | Target Database | Description / Key Attributes |
|---|---|---|---|
| Regions | 3 | All | `FI-LPR`, `FI-HEL`, `FI-LAH` |
| Users | 30 | PostgreSQL | 10 customers, 10 drivers, 5 restaurant owners, 5 admins |
| Customers | 10 | PostgreSQL | Delivery address, preferred region (`CUST-201` to `CUST-210`) |
| Drivers | 10 | PostgreSQL | Vehicle type, license plate, home region (`DRV-001` to `DRV-010`) |
| Restaurants | 15 | MongoDB | 5 per region, rating, OCC version, nested menus & reviews |
| Menu Items | 103 | MongoDB | Pizzas, pasta, salads, sushi, bowls, burgers, desserts |
| Customer Reviews | 37 | MongoDB | Customer ID, rating (1-5), timestamp, comment |
| Orders | 50 | PostgreSQL | Regional order routing, status, delivery address |
| Order Items | 127 | PostgreSQL | Historical unit price, quantity, item reference |
| Payments | 50 | PostgreSQL | 1:1 order relation, card/Apple Pay/Google Pay |
| Driver Location History | 50 | MongoDB | Historical GPS traces with realistic Finnish coordinates |
| Shopping Carts | 10 | Redis | Pre-warmed carts (`CUST-201` = Margherita, 4-Cheese, Salad, Coke = 23.00€) |
| Session Tokens | 10 | Redis | User auth sessions with 3600s TTL |
| GPS Events | 30 | Kafka | Partitioned by `DriverID` |
| Order Status Events | 15 | Kafka | Partitioned by `OrderID` (lifecycle: NEW -> PREPARING -> READY -> ON_THE_WAY -> DELIVERED) |
