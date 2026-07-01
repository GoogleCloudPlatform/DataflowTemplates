#!/bin/bash
# run_poc_tests.sh

set -e

export PROJECT_ID="span-cloud-testing"
export REGION="us-central1"
export SPANNER_INSTANCE="asapha-test"
export SOURCE_DB="poc-source"
export TARGET_DB="poc-target"
export GCS_STAGING_BUCKET="gs://${PROJECT_ID}-poc-staging"
export TEST_RUN_ID=$(LC_ALL=C tr -dc 'a-z0-9' < /dev/urandom | head -c 6)

COMMAND=$1

case "$COMMAND" in
  setup)
    echo "=== Creating GCS Bucket ==="
    gcloud storage buckets create $GCS_STAGING_BUCKET --location=$REGION --project=$PROJECT_ID || true
    gcloud storage rm -r $GCS_STAGING_BUCKET/tmp/dlq || true

    echo "=== Creating Source Database ==="
    gcloud spanner databases create $SOURCE_DB \
      --instance=$SPANNER_INSTANCE \
      --project=$PROJECT_ID || true

    gcloud spanner databases ddl update $SOURCE_DB \
      --instance=$SPANNER_INSTANCE \
      --project=$PROJECT_ID \
      --ddl="CREATE TABLE Users (Id INT64 NOT NULL, Name STRING(MAX), Status STRING(MAX)) PRIMARY KEY (Id); CREATE CHANGE STREAM UsersChangeStream FOR Users;" || true

    echo "=== Creating Target Database ==="
    gcloud spanner databases create $TARGET_DB \
      --instance=$SPANNER_INSTANCE \
      --project=$PROJECT_ID || true

    gcloud spanner databases ddl update $TARGET_DB \
      --instance=$SPANNER_INSTANCE \
      --project=$PROJECT_ID \
      --ddl="CREATE TABLE Users (Id INT64 NOT NULL, Name STRING(MAX), Status STRING(MAX)) PRIMARY KEY (Id); CREATE TABLE shadow_Users (Id INT64 NOT NULL, processed_commit_ts TIMESTAMP, record_seq INT64) PRIMARY KEY (Id);" || true

    echo "=== Creating Session File ==="
    cat <<FILE_EOF > session.json
{
  "SpSchema": {
    "Users": {
      "Name": "Users",
      "ColIds": ["Id", "Name", "Status"],
      "ColDefs": {
        "Id": {"Name": "Id", "T": {"Name": "INT64"}},
        "Name": {"Name": "Name", "T": {"Name": "STRING", "Len": 9223372036854775807}},
        "Status": {"Name": "Status", "T": {"Name": "STRING", "Len": 9223372036854775807}}
      },
      "PrimaryKeys": [{"ColId": "Id"}]
    }
  },
  "SrcSchema": {
    "Users": {
      "Name": "Users",
      "ColIds": ["Id", "Name", "Status"],
      "ColDefs": {
        "Id": {"Name": "Id", "Type": {"Name": "INT"}},
        "Name": {"Name": "Name", "Type": {"Name": "VARCHAR"}},
        "Status": {"Name": "Status", "Type": {"Name": "VARCHAR"}}
      },
      "PrimaryKeys": [{"ColId": "Id"}]
    }
  },
  "ToSpanner": {
    "Users": {
      "Name": "Users",
      "Cols": {
        "Id": "Id",
        "Name": "Name",
        "Status": "Status"
      }
    }
  },
  "ToSource": {
    "Users": {
      "Name": "Users",
      "Cols": {
        "Id": "Id",
        "Name": "Name",
        "Status": "Status"
      }
    }
  },
  "SyntheticPKeys": {}
}
FILE_EOF
    gcloud storage cp session.json $GCS_STAGING_BUCKET/session.json

    echo "=== Creating Shard File ==="
    cat <<FILE_EOF > shard.json
[
  {
    "projectId": "$PROJECT_ID",
    "instanceId": "$SPANNER_INSTANCE",
    "databaseId": "$TARGET_DB"
  }
]
FILE_EOF
    gcloud storage cp shard.json $GCS_STAGING_BUCKET/shard.json

    echo "=== Staging Dataflow Template ==="
    mvn clean package -PtemplatesStage -Dmaven.test.skip=true -Denforcer.skip=true -pl v2/spanner-to-sourcedb \
      -DprojectId="$PROJECT_ID" \
      -DbucketName="$GCS_STAGING_BUCKET" -DstagePrefix="templates-${TEST_RUN_ID}" \
      -DtemplateName="Spanner_to_SourceDb" -Dspotless.check.skip=true -Dcheckstyle.skip=true

    TEMPLATE_PATH="${GCS_STAGING_BUCKET}/templates-${TEST_RUN_ID}/flex/Spanner_to_SourceDb"
    echo "Template built at: $TEMPLATE_PATH"
    
    echo "=== Launching Dataflow Job ==="
    gcloud dataflow flex-template run stateful-poc-job-${TEST_RUN_ID} \
      --template-file-gcs-location=$TEMPLATE_PATH \
      --region=$REGION \
      --project=$PROJECT_ID \
      --max-workers=5 \
      --parameters="changeStreamName=UsersChangeStream,instanceId=$SPANNER_INSTANCE,databaseId=$SOURCE_DB,spannerProjectId=$PROJECT_ID,metadataInstance=$SPANNER_INSTANCE,metadataDatabase=$TARGET_DB,sourceType=spanner,sessionFilePath=$GCS_STAGING_BUCKET/session.json,sourceShardsFilePath=$GCS_STAGING_BUCKET/shard.json,shadowTablePrefix=shadow_,workerMachineType=n1-standard-4,deadLetterQueueDirectory=$GCS_STAGING_BUCKET/dlq"
    
    echo "Job Launched!"
    ;;
    
  test)
    echo "=== Cleaning up source table ==="
    gcloud spanner databases execute-sql $SOURCE_DB \
      --instance=$SPANNER_INSTANCE \
      --project=$PROJECT_ID \
      --sql="DELETE FROM Users WHERE Id >= 1" || true

    echo "=== Inserting 100 Baseline rows ==="
    for i in {1..100}; do
      gcloud spanner databases execute-sql $SOURCE_DB \
        --instance=$SPANNER_INSTANCE \
        --project=$PROJECT_ID \
        --sql="INSERT INTO Users (Id, Name, Status) VALUES ($i, 'Test User $i', 'created')" --quiet &
    done
    wait

    echo "=== Simulating Spanner Hot Key Load & High Contention ==="
    for i in {1..20}; do
       gcloud spanner databases execute-sql $SOURCE_DB \
         --instance=$SPANNER_INSTANCE \
         --project=$PROJECT_ID \
         --sql="UPDATE Users SET Status='processing_$i' WHERE Id=1" --quiet &
    done
    wait
    
    echo "=== Updating all 100 rows to simulate widespread changes ==="
    for i in {1..100}; do
      gcloud spanner databases execute-sql $SOURCE_DB \
        --instance=$SPANNER_INSTANCE \
        --project=$PROJECT_ID \
        --sql="UPDATE Users SET Status='completed' WHERE Id=$i" --quiet &
    done
    wait

    echo "Wait 60s for Dataflow pipeline to process..."
    sleep 60
    
    echo "=== Validating Target DB (Users Table) ==="
    gcloud spanner databases execute-sql $TARGET_DB \
      --instance=$SPANNER_INSTANCE \
      --project=$PROJECT_ID \
      --sql="SELECT COUNT(*) AS TargetUsersCount FROM Users"
      
    echo "=== Validating Shadow Table ==="
    gcloud spanner databases execute-sql $TARGET_DB \
      --instance=$SPANNER_INSTANCE \
      --project=$PROJECT_ID \
      --sql="SELECT COUNT(*) AS ShadowUsersCount FROM shadow_Users"
      
    echo "=== Checking Dataflow Logs for simulated partial failures ==="
    # Find the job ID of the running stateful-poc-job
    JOB_ID=$(gcloud dataflow jobs list --project=$PROJECT_ID --region=$REGION --status=active --format="value(JOB_ID)" --filter="NAME:stateful-poc-job*")
    if [ ! -z "$JOB_ID" ]; then
      echo "Dataflow Job ID: $JOB_ID"
      gcloud logging read "resource.labels.job_id=\"$JOB_ID\" AND \"Simulated partial failure for testing!\"" --limit=10 --project=$PROJECT_ID
    else
      echo "No active Dataflow job found to fetch logs from."
    fi
    ;;
    
  teardown)
    echo "=== Cancelling Active Dataflow Jobs ==="
    JOB_IDS=$(gcloud dataflow jobs list --project=$PROJECT_ID --region=$REGION --status=active --format="value(JOB_ID)" --filter="NAME:stateful-poc-job*")
    for id in $JOB_IDS; do
      echo "Cancelling job $id..."
      gcloud dataflow jobs cancel $id --project=$PROJECT_ID --region=$REGION || true
    done
    
    echo "=== Tearing down Spanner databases ==="
    gcloud spanner databases delete $SOURCE_DB --instance=$SPANNER_INSTANCE --project=$PROJECT_ID --quiet || true
    gcloud spanner databases delete $TARGET_DB --instance=$SPANNER_INSTANCE --project=$PROJECT_ID --quiet || true
    echo "Teardown complete."
    ;;
    
  *)
    echo "Usage: $0 [setup|test|teardown]"
    exit 1
    ;;
esac
