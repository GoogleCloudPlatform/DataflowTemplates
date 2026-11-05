#!/usr/bin/env bash
set -euo pipefail

echo "================================================================="
echo "Creating VPC Service Networking Connection via gcloud..."
echo "================================================================="

if [ -z "${NETWORK_NAME:-}" ] || [ -z "${RANGE_NAME:-}" ] || [ -z "${PROJECT_ID:-}" ]; then
  echo "[ERROR] Missing required environment variables: NETWORK_NAME, RANGE_NAME, PROJECT_ID must be set."
  exit 1
fi

gcloud services vpc-peerings connect \
  --service="servicenetworking.googleapis.com" \
  --network="$NETWORK_NAME" \
  --ranges="$RANGE_NAME" \
  --project="$PROJECT_ID" \
  --quiet
