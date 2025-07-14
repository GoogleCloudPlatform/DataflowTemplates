#!/bin/bash

# This script cleans up Datastream and Spanner resources older than 1 day.
# It is designed to ignore errors, ensuring that the CI/CD pipeline continues
# even if a resource deletion fails.

echo "--- Removing Datastream streams created more than 1 day ago ---"
STREAMS_TO_DELETE=$(gcloud datastream streams list --project cloud-teleport-testing --location=us-central1 \
  --limit=1000 --page-size=1000 --filter="createTime<-P24H" --format='value[separator=" "](name)' 2>/dev/null | grep -v nokill)

if [ -n "$STREAMS_TO_DELETE" ]; then
  echo "$STREAMS_TO_DELETE" | xargs -n1 gcloud datastream streams delete --project cloud-teleport-testing --location=us-central1 --quiet
else
  echo "No old datastream streams to delete or failed to list them."
fi

echo "--- Removing Spanner databases created more than 1 day ago ---"
DATABASES_TO_DELETE=$(gcloud spanner databases list --project=cloud-teleport-testing --instance=teleport \
  --limit=1000 --page-size=1000 --filter="createTime<-P24H" --format='value[separator=" "](name)' 2>/dev/null | grep -vE "export-test|pg-export-test|pg-spanner-teleport|teleport")

if [ -n "$DATABASES_TO_DELETE" ]; then
  echo "$DATABASES_TO_DELETE" | xargs -n1 gcloud spanner databases delete --project=cloud-teleport-testing --instance=teleport --quiet
else
  echo "No old spanner databases to delete or failed to list them."
fi

echo "--- Resource cleanup script finished ---"
