export PROJECT=span-cloud-ck-testing-external
export BUCKET_NAME=iamsandeep-bucket-ck-003
export REGION=us-central1


export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Sourcedb_to_Spanner_Flex"

### Required
export SOURCE_CONFIG_URL="gs://${BUCKET_NAME}/shardConfig_ea_111_118.json"
export INSTANCE_ID=iamsandeep-test-instance-002
export DATABASE_ID=load_test_db_7
export PROJECT_ID=span-cloud-ck-testing-external
export OUTPUT_DIRECTORY="gs://${BUCKET_NAME}/bulkoutput/"
export FETCH_SIZE=10000
### Optional
#export JDBC_DRIVER_JARS=""
MAX_WORKERS=300
NUM_WORKERS=30
export JDBC_DRIVER_CLASS_NAME=com.mysql.jdbc.Driver
export USERNAME="root"
export PASSWORD="Welcome@1"
export TABLES=""
#export NUM_PARTITIONS=0
export SPANNER_HOST=https://batch-spanner.googleapis.com
#export MAX_CONNECTIONS=0
export SESSION_FILE_PATH="gs://$BUCKET_NAME/shard0.session.json"
#export DISABLED_ALGORITHMS=<disabledAlgorithms>
#export EXTRA_FILES_TO_STAGE=<extraFilesToStage>
export DEFAULT_LOG_LEVEL=INFO

gcloud dataflow flex-template run "iamsandeep-sourcedb-to-spanner-flex-job-paxos-delay-015" \
  --project "$PROJECT" \
  --region "$REGION" \
  --max-workers "$MAX_WORKERS" \
  --num-workers "$NUM_WORKERS" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --additional-experiments="disable_runner_v2" \
  --parameters "jdbcDriverClassName=$JDBC_DRIVER_CLASS_NAME" \
  --parameters "sourceConfigURL=$SOURCE_CONFIG_URL" \
  --parameters "fetchSize=$FETCH_SIZE" \
  --parameters "username=$USERNAME" \
  --parameters "password=$PASSWORD" \
  --parameters "tables=$TABLES" \
  --parameters "instanceId=$INSTANCE_ID" \
  --parameters "databaseId=$DATABASE_ID" \
  --parameters "projectId=$PROJECT_ID" \
  --parameters "spannerHost=$SPANNER_HOST" \
  --parameters "sessionFilePath=$SESSION_FILE_PATH" \
  --parameters "outputDirectory=$OUTPUT_DIRECTORY" \
  --parameters "maxCommitDelay=50" \
#  --parameters "defaultLogLevel=$DEFAULT_LOG_LEVEL"
