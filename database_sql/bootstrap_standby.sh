
#!/usr/bin/env bash
set -euo pipefail

# FoodGo PostgreSQL standby initialization
# Input: REPLICATION_PASSWORD environment variable
# Output: Standby data directory initialized from Primary

DATA_DIR="${PGDATA:-/var/lib/postgresql/data}"

if [ -z "${REPLICATION_PASSWORD:-}" ]; then
    echo "ERROR: REPLICATION_PASSWORD is missing."
    exit 1
fi

mkdir -p "$DATA_DIR"

# Never overwrite an existing database directory.
if [ -n "$(ls -A "$DATA_DIR")" ]; then
    echo "ERROR: Standby data directory is not empty."
    exit 1
fi

chown postgres:postgres "$DATA_DIR"
chmod 700 "$DATA_DIR"

echo "Starting base backup from postgres-primary..."

# Copy the complete PostgreSQL cluster.
export PGPASSWORD="$REPLICATION_PASSWORD"

gosu postgres pg_basebackup \
    -h postgres-primary \
    -p 5432 \
    -U replicator \
    -D "$DATA_DIR" \
    -Fp \
    -Xs \
    -P \
    -R

unset PGPASSWORD

# Create a password file for future WAL connections.
escaped_password="${REPLICATION_PASSWORD//\\/\\\\}"
escaped_password="${escaped_password//:/\\:}"

printf 'postgres-primary:5432:replication:replicator:%s\n' \
    "$escaped_password" > "$DATA_DIR/.pgpass"

chmod 600 "$DATA_DIR/.pgpass"
chown postgres:postgres "$DATA_DIR/.pgpass"

# Configure the standby to use its password file.
cat >> "$DATA_DIR/postgresql.auto.conf" <<'EOF'

primary_conninfo = 'host=postgres-primary port=5432 user=replicator passfile=/var/lib/postgresql/data/.pgpass application_name=postgres-standby'
EOF

chown postgres:postgres "$DATA_DIR/postgresql.auto.conf"

echo "Standby initialization completed."
