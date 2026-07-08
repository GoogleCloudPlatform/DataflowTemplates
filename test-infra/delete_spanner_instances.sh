#!/bin/bash

# This script deletes Google Cloud Spanner instances that are older than 1 day,
# with a specific exclusion list.

# Set the project ID
PROJECT_ID="cloud-teleport-testing"

# Set the exclusion list (space-separated)
EXCLUSION_LIST=("teleport" "beam-test")

# Set the cutoff time (1 day ago in ISO 8601 format)
if [[ $OSTYPE == "darwin"* ]]; then
  CUTOFF_TIME=$(date -v-1d -u +"%Y-%m-%dT%H:%M:%SZ")
elif [[ $OSTYPE == "linux-gnu"* ]]; then
  CUTOFF_TIME=$(date -d '1 day ago' -u +"%Y-%m-%dT%H:%M:%SZ")
else
  echo "Unsupported platform"
  exit 1
fi

# Get the list of spanner instances
SPANNERS=$(gcloud spanner instances list --project="$PROJECT_ID" --format="json")

# Filter and delete instances
echo "$SPANNERS" | jq -c '.[]' | while read i; do
    CREATION_TIME=$(echo "$i" | jq -r '.createTime')
    INSTANCE_NAME=$(echo "$i" | jq -r '.name' | sed 's:.*/::')

    # Check if the instance should be excluded
    EXCLUDE=false
    for excluded_name in "${EXCLUSION_LIST[@]}"; do
        if [[ "$INSTANCE_NAME" == "$excluded_name"* ]]; then # starts with
            EXCLUDE=true
            break
        fi
    done

    if [ "$EXCLUDE" = true ]; then
        echo "Skipping excluded instance '$INSTANCE_NAME'"
        continue
    fi

    # Check if the instance is older than the cutoff time
    if [[ "$CREATION_TIME" < "$CUTOFF_TIME" ]]; then
        echo "Instance '$INSTANCE_NAME' is older than cutoff time, checking for backups..."
        # List all backups for the instance
        BACKUPS=$(gcloud spanner backups list --instance="$INSTANCE_NAME" --project="$PROJECT_ID" --format="json")

        # Delete each backup
        echo "$BACKUPS" | jq -c '.[]' | while read b; do
            BACKUP_NAME=$(echo "$b" | jq -r '.name' | sed 's:.*/::')
            echo "Deleting backup '$BACKUP_NAME' for instance '$INSTANCE_NAME'..."
            gcloud spanner backups delete "$BACKUP_NAME" --instance="$INSTANCE_NAME" --project="$PROJECT_ID" --quiet
        done
        gcloud spanner instances delete "$INSTANCE_NAME" --project="$PROJECT_ID" --quiet
    else
        echo "Skipping instance '$INSTANCE_NAME' (created at $CREATION_TIME)."
    fi
done
