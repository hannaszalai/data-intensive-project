# data-intensive-project

## Setup and Running

```bash
cd <project-root>
docker compose up -d --build
```

Wait about 15-20 seconds for MongoDB replica set initialization to complete.

Check containers are running:

```bash
docker compose ps
```

To verify that the server is running and the mock data is loaded, visit http://localhost:8000/restaurants/FI-LPR to view a sample of the local data.