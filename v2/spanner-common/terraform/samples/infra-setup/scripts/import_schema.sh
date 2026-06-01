#!/usr/bin/env bash
set -euo pipefail

echo "========================================="
echo "Starting sequential schema import..."
echo "========================================="

if [ -z "${PROJECT_ID:-}" ] || [ -z "${BUCKET_NAME:-}" ] || [ -z "${OBJECT_NAME:-}" ] || \
   [ -z "${PHYSICAL_COUNT:-}" ] || [ -z "${LOGICAL_COUNT:-}" ] || \
   [ -z "${INSTANCE_NAMES:-}" ] || [ -z "${DATABASE_NAMES:-}" ]; then
  echo "[ERROR] Missing required environment variables."
  exit 1
fi

# Convert comma-separated env inputs to bash arrays
IFS=',' read -ra INSTANCES <<< "$INSTANCE_NAMES"
IFS=',' read -ra DATABASES <<< "$DATABASE_NAMES"

TOTAL_SHARDS=$((PHYSICAL_COUNT * LOGICAL_COUNT))

for idx in $(seq 0 $((TOTAL_SHARDS - 1))); do
  PHYSICAL_IDX=$((idx / LOGICAL_COUNT))
  INSTANCE_NAME=${INSTANCES[$PHYSICAL_IDX]}
  DB_NAME=${DATABASES[$idx]}
  
  echo "[INFO] Shard $idx: Importing schema into '$DB_NAME' on '$INSTANCE_NAME'..."
  
  success=false
  for attempt in {1..6}; do
    if gcloud sql import sql "$INSTANCE_NAME" "gs://$BUCKET_NAME/$OBJECT_NAME" \
      --database="$DB_NAME" \
      --project="$PROJECT_ID" \
      --quiet; then
      success=true
      break
    fi
    echo "[WARN] GCS IAM eventual consistency delay. Retrying in 10s (attempt $attempt/6)..."
    sleep 10
  done
  
  if [ "$success" = false ]; then
    echo "[ERROR] Failed to import schema into database '$DB_NAME' after 6 attempts."
    exit 1
  fi
done

echo "========================================="
echo "Schema import completed successfully!"
echo "========================================="

exit 0
