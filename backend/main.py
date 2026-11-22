import os
import time
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from databases.nosql import mongo_database
from databases.nosql import init_nosql_data

# Create fastAPI application
app = FastAPI(title="FoodGo API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:8080",
        "http://127.0.0.1:8080",
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# TODO remove retry loop
@app.on_event("startup")
def startup_event():
    if os.getenv("INIT_DB_ON_STARTUP") == "True":
        max_retries = 10
        for attempt in range(1, max_retries + 1):
            try:
                print(f"Try {attempt}/{max_retries} for initialization of NoSQL data...")
                init_nosql_data.init_nosql_data()
                print("NoSQL data initialization completed successfully.")
                break
            except Exception as e:
                print(f"Database is not ready yet ({e}). Waiting 5 seconds...")
                time.sleep(5)
        else:
            print("Error: Failed to initialize NoSQL data after 10 attempts.")
            raise Exception("NoSQL data initialization failed.")

@app.get("/")
@app.get("/test")
def test_endpoint():
    return {
        "message": "Hi, FastAPI backend is running successfully.",
        "status": "ok",
    }

@app.get("/restaurants/{region_id}")
def get_restaurants(region_id: str, use_replica: bool = True):
    try:
        restaurants = mongo_database.get_restaurants_by_region(
            region_id=region_id,
            use_replica=use_replica
        )
        return {
            "region_id": region_id,
            "count": len(restaurants),
            "restaurants": restaurants,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))