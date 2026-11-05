#!/usr/bin/env bash
set -euo pipefail

if [ -z "${INSTANCE_NAME:-}" ] || [ -z "${PROJECT_ID:-}" ]; then
  echo "[ERROR] Missing required environment variables: INSTANCE_NAME, PROJECT_ID must be set."
  exit 1
fi

echo "================================================================="
echo "Listing and deleting Spanner backups for instance '$INSTANCE_NAME'..."
echo "================================================================="

BACKUPS=$(gcloud spanner backups list \
  --instance="$INSTANCE_NAME" \
  --project="$PROJECT_ID" \
  --format="value(BACKUP)" 2>/dev/null || true)

if [ -n "$BACKUPS" ]; then
  for backup in $BACKUPS; do
    echo "[INFO] Deleting Spanner backup: $backup"
    gcloud spanner backups delete "$backup" \
      --instance="$INSTANCE_NAME" \
      --project="$PROJECT_ID" \
      --quiet || true
  done
  echo "[SUCCESS] Finished deleting Spanner backups."
else
  echo "[INFO] No Spanner backups found."
fi

exit 0
