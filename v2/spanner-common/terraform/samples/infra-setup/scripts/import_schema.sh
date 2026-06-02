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

# Function to import all logical databases sequentially for a single physical instance
import_instance_databases() {
  local p_idx="$1"
  local instance_name="${INSTANCES[$p_idx]}"
  
  for l_idx in $(seq 0 $((LOGICAL_COUNT - 1))); do
    local global_idx=$((p_idx * LOGICAL_COUNT + l_idx))
    local db_name="${DATABASES[$global_idx]}"
    
    echo "[INFO] Physical Shard $p_idx (Logical $l_idx): Importing schema into '$db_name'..."
    
    local success=false
    for attempt in {1..6}; do
      if gcloud sql import sql "$instance_name" "gs://$BUCKET_NAME/$OBJECT_NAME" \
        --database="$db_name" \
        --project="$PROJECT_ID" \
        --quiet; then
        success=true
        break
      fi
      echo "[WARN] Instance $instance_name: GCS IAM eventual consistency delay. Retrying in 10s (attempt $attempt/6)..."
      sleep 10
    done
    
    if [ "$success" = false ]; then
      echo "[ERROR] Instance $instance_name: Failed to import schema into '$db_name' after 6 attempts."
      return 1
    fi
  done
  return 0
}

# Spin up parallel background jobs, one for each physical database instance
echo "[INFO] Starting parallel schema imports across $PHYSICAL_COUNT physical database instances..."
pids=()
for p_idx in $(seq 0 $((PHYSICAL_COUNT - 1))); do
  import_instance_databases "$p_idx" &
  pids+=($!)
done

# Wait for all parallel background jobs to finish
echo "[INFO] Waiting for all parallel schema import jobs to complete..."
failed=0
for pid in "${pids[@]}"; do
  if ! wait "$pid"; then
    failed=$((failed + 1))
  fi
done

if [ "$failed" -ne 0 ]; then
  echo "[ERROR] $failed physical shards failed to complete their schema imports."
  exit 1
fi

echo "========================================="
echo "Parallel schema import completed successfully!"
echo "========================================="
exit 0
