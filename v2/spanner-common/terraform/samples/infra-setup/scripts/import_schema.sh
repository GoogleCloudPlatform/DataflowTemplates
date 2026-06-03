#!/usr/bin/env bash
set -euo pipefail

# Imports the schema file into every logical database of a SINGLE Cloud SQL
# instance. Imports within an instance run sequentially
# because Cloud SQL permits only one import operation per instance at a time.

if [ -z "${PROJECT_ID:-}" ] || [ -z "${BUCKET_NAME:-}" ] || [ -z "${OBJECT_NAME:-}" ] || \
   [ -z "${INSTANCE_NAME:-}" ] || [ -z "${DATABASE_NAMES:-}" ]; then
  echo "[ERROR] Missing required environment variables: PROJECT_ID, BUCKET_NAME, OBJECT_NAME, INSTANCE_NAME, DATABASE_NAMES."
  exit 1
fi

echo "========================================="
echo "Importing schema into instance '$INSTANCE_NAME'..."
echo "========================================="

# Convert the comma-separated database list into a bash array
IFS=',' read -ra DATABASES <<< "$DATABASE_NAMES"

for db_name in "${DATABASES[@]}"; do
  echo "[INFO] Instance $INSTANCE_NAME: importing schema into '$db_name'..."

  success=false
  max_attempts=10
  base_delay=10

  for attempt in $(seq 1 $max_attempts); do
    if gcloud sql import sql "$INSTANCE_NAME" "gs://$BUCKET_NAME/$OBJECT_NAME" \
      --database="$db_name" \
      --project="$PROJECT_ID" \
      --quiet; then
      success=true
      break
    fi
    
    # Calculate exponential backoff with a cap of 60 seconds
    delay=$(( base_delay * 2 ** (attempt - 1) ))
    if [ $delay -gt 60 ]; then
      delay=60
    fi
    
    # Add a random jitter of 1-5 seconds to prevent thundering herds across parallel shards
    jitter=$(( RANDOM % 5 + 1 ))
    total_delay=$(( delay + jitter ))
    
    echo "[WARN] Instance $INSTANCE_NAME: import failed (Rate limit or IAM eventual consistency). Retrying in ${total_delay}s (attempt $attempt/$max_attempts)..."
    sleep $total_delay
  done

  if [ "$success" = false ]; then
    echo "[ERROR] Instance $INSTANCE_NAME: failed to import schema into '$db_name' after $max_attempts attempts."
    exit 1
  fi
done

echo "========================================="
echo "Schema import completed for instance '$INSTANCE_NAME'."
echo "========================================="
exit 0