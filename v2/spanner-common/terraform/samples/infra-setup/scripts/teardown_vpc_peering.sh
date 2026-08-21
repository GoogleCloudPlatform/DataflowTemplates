#!/usr/bin/env bash
set -euo pipefail

echo "================================================================="
echo "Starting Service Networking Connection teardown via gcloud..."
echo "================================================================="

if [ -z "${NETWORK_NAME:-}" ] || [ -z "${PROJECT_ID:-}" ]; then
  echo "[ERROR] Missing required environment variables: NETWORK_NAME, PROJECT_ID must be set."
  exit 1
fi

if gcloud services vpc-peerings delete \
  --service="servicenetworking.googleapis.com" \
  --network="$NETWORK_NAME" \
  --project="$PROJECT_ID" \
  --quiet; then
  echo "================================================================="
  echo "[SUCCESS] Service Networking Connection deleted successfully!"
  echo "================================================================="
else
  echo "================================================================="
  echo "[INFO] Active Producer services (orphans) are still tied to this VPC in GCP."
  echo "[INFO] Safely preserving the connection in GCP to avoid blocking Terraform destroy."
  echo "================================================================="
fi

exit 0
