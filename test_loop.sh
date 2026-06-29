#!/bin/bash
PROJECT_ID="span-cloud-testing"
SPANNER_INSTANCE="asapha-test"
SOURCE_DB="poc-source"
echo "Inserting 20 rows..."
for i in {1..20}; do
  gcloud spanner databases execute-sql $SOURCE_DB \
    --instance=$SPANNER_INSTANCE \
    --project=$PROJECT_ID \
    --sql="INSERT INTO Users (Id, Name, Status) VALUES ($i, 'Test User $i', 'created')" --quiet &
done
wait
echo "Updating rows..."
for i in {1..20}; do
  gcloud spanner databases execute-sql $SOURCE_DB \
    --instance=$SPANNER_INSTANCE \
    --project=$PROJECT_ID \
    --sql="UPDATE Users SET Status='processing_$i' WHERE Id=$i" --quiet &
done
wait
echo "Done"
