#!/bin/bash
set -e

echo "==============================="
echo " E-Commerce Data Pipeline Setup"
echo "==============================="

# --------------------------------------------------
# Load environment variables
# --------------------------------------------------
if [ -f ".env" ]; then
  echo "Loading environment variables from .env"
  set -o allexport
  source .env
  set +o allexport
fi

# --------------------------------------------------
# Detect execution environment
# --------------------------------------------------
if [ -d "/app" ]; then
  echo "Running inside Docker container"
  BASE_DIR="/app"
else
  echo "Running on local machine"
  BASE_DIR="."
fi

# --------------------------------------------------
# Validate required environment variables
# --------------------------------------------------
REQUIRED_VARS=(DB_HOST DB_PORT DB_NAME DB_USER DB_PASSWORD)

for VAR in "${REQUIRED_VARS[@]}"; do
  if [ -z "${!VAR}" ]; then
    echo "❌ ERROR: Environment variable $VAR is not set"
    exit 1
  fi
done

echo "Database Configuration:"
echo "  DB_HOST=$DB_HOST"
echo "  DB_PORT=$DB_PORT"
echo "  DB_NAME=$DB_NAME"
echo "  DB_USER=$DB_USER"

# --------------------------------------------------
# Wait for PostgreSQL using Python (Docker-safe)
# --------------------------------------------------
echo "Waiting for PostgreSQL to be ready..."
python <<EOF
import psycopg2
import os
import time

while True:
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            connect_timeout=3
        )
        conn.close()
        print("✅ PostgreSQL is ready")
        break
    except Exception:
        print("Waiting for database to be ready...")
        time.sleep(3)
EOF

# --------------------------------------------------
# Initialize database schemas
# --------------------------------------------------
echo "Initializing PostgreSQL schemas..."

python <<EOF
import psycopg2
import os

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)

conn.autocommit = True
cur = conn.cursor()

schemas = ["staging", "production", "warehouse"]

for schema in schemas:
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

cur.close()
conn.close()

print("✅ Schemas created successfully")
EOF

# --------------------------------------------------
# Create required directories
# --------------------------------------------------
echo "Creating required directories..."

mkdir -p \
  "$BASE_DIR/logs" \
  "$BASE_DIR/data/raw" \
  "$BASE_DIR/data/staging" \
  "$BASE_DIR/data/processed"

echo "✅ Directories created"

# --------------------------------------------------
# Run pipeline steps
# --------------------------------------------------
echo "Starting data generation..."
python "$BASE_DIR/scripts/data_generation/generate_data.py"

echo "Starting data ingestion..."
python "$BASE_DIR/scripts/ingestion/ingest_to_staging.py"

echo "Running data quality checks..."
python "$BASE_DIR/scripts/quality_checks/validate_data.py"

echo "Setup completed successfully 🎉"
