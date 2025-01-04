# update backend.conf
state_bucket=$(yq e '.global.state_bucket' config.yaml)
sed -i "s/^bucket = .*/bucket = \"${state_bucket}\"/" backend.conf

#Create Dataflow Templates Bucket
echo "--------------------------------------"
echo "Create Dataflow Template Bucket"
echo "--------------------------------------"
cd resources/cloud-storage/dataflow-template
rm backend.conf
ln -s ../../../backend.conf backend.conf
terraform init -backend-config=backend.conf
terraform plan
terraform apply --auto-approve

#Create Pubsub
echo "--------------------"
echo "Create Pubsub"
echo "--------------------"
cd ../../pubsub
rm backend.conf
ln -s ../../backend.conf backend.conf
terraform init -backend-config=backend.conf
terraform plan
terraform apply --auto-approve

#Create Spanner
echo "--------------------"
echo "Create Spanner"
echo "--------------------"
cd ../spanner
rm backend.conf
ln -s ../../backend.conf backend.conf
terraform init -backend-config=backend.conf
terraform plan
terraform apply --auto-approve

#Create VPC
echo "--------------------"
echo "Create VPC"
echo "--------------------"
cd ../vpc
rm backend.conf
ln -s ../../backend.conf backend.conf
terraform init -backend-config=backend.conf
terraform plan
terraform apply --auto-approve

#Create Subnets
echo "--------------------"
echo "Create Subnets"
echo "--------------------"
cd ../subnets
rm backend.conf
ln -s ../../backend.conf backend.conf
terraform init -backend-config=backend.conf
terraform plan
terraform apply --auto-approve

#Create Cassandra
echo "-----------------------"
echo "Create Cassandra VM"
echo "-----------------------"
cd ../compute-engine/cassandra-1
rm backend.conf
ln -s ../../../backend.conf backend.conf
terraform init -backend-config=backend.conf
terraform plan
terraform apply --auto-approve

#Create Cassandra
echo "-----------------------"
echo "Create MySQL VM"
echo "-----------------------"
cd ../mysql-1
rm backend.conf
ln -s ../../../backend.conf backend.conf
terraform init -backend-config=backend.conf
terraform plan
terraform apply --auto-approve

#ConfigureFirewall
echo "-----------------------"
echo "Create Firewall"
echo "-----------------------"
cd ../../firewall
rm backend.conf
ln -s ../../backend.conf backend.conf
terraform init -backend-config=backend.conf
terraform plan
terraform apply --auto-approve

cd ../../
DF_BUCKET=$(yq eval '.dataflow-bucket.name' config.yaml)
STATE_BUCKET=$(yq eval '.global.state_bucket' config.yaml)
PROJECT_ID=$(yq eval '.global.project_id' config.yaml)
REGION=$(yq eval '.global.region' config.yaml)
DATAFLOW_BUCKET=$(yq eval '.dataflow-bucket.name' config.yaml)

#Upload tf state
#cd terraform-state-bucket
gsutil cp terraform-state-bucket/terraform.tfstate gs://$STATE_BUCKET/tf-state/terraform.tfstate

# cd ..
# STATE_BUCKET=$(yq eval '.global.state_bucket' config.yaml)
# PROJECT_ID=$(yq eval '.global.project_id' config.yaml)
# REGION=$(yq eval '.global.region' config.yaml)
# DATAFLOW_BUCKET=$(yq eval '.dataflow-bucket.name' config.yaml)

# export PROJECT=$PROJECT_ID
# export BUCKET_NAME=$DATAFLOW_BUCKET
# export REGION=$REGION
# export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Spanner_to_SourceDb"

# ### Required
# export CHANGE_STREAM_NAME=<changeStreamName>
# export INSTANCE_ID=<instanceId>
# export DATABASE_ID=<databaseId>
# export SPANNER_PROJECT_ID=<spannerProjectId>
# export METADATA_INSTANCE=<metadataInstance>
# export METADATA_DATABASE=<metadataDatabase>
# export SOURCE_SHARDS_FILE_PATH=<sourceShardsFilePath>

# echo $PROJECT
# echo $BUCKET_NAME
# echo $REGION
# echo $TEMPLATE_SPEC_GCSPATH
# cd ..

# # mvn clean package -PtemplatesStage -DskipTests -DprojectId="$PROJECT" -DbucketName="$BUCKET_NAME" -DstagePrefix="templates" -DtemplateName="Spanner_to_SourceDb" -pl v2/spanner-to-sourcedb -am
# gcloud dataflow flex-template run "spanner-to-sourcedb-job" \
#   --project "$PROJECT" \
#   --region "$REGION" \
#   --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
#   --parameters "changeStreamName=$CHANGE_STREAM_NAME" \
#   --parameters "instanceId=$INSTANCE_ID" \
#   --parameters "databaseId=$DATABASE_ID" \
#   --parameters "spannerProjectId=$SPANNER_PROJECT_ID" \
#   --parameters "metadataInstance=$METADATA_INSTANCE" \
#   --parameters "metadataDatabase=$METADATA_DATABASE" \
#   --parameters "startTimestamp=$START_TIMESTAMP" \
#   --parameters "endTimestamp=$END_TIMESTAMP" \
#   --parameters "shadowTablePrefix=$SHADOW_TABLE_PREFIX" \
#   --parameters "sourceShardsFilePath=$SOURCE_SHARDS_FILE_PATH" \
#   --parameters "sessionFilePath=$SESSION_FILE_PATH" \
#   --parameters "filtrationMode=$FILTRATION_MODE" \
#   --parameters "shardingCustomJarPath=$SHARDING_CUSTOM_JAR_PATH" \
#   --parameters "shardingCustomClassName=$SHARDING_CUSTOM_CLASS_NAME" \
#   --parameters "shardingCustomParameters=$SHARDING_CUSTOM_PARAMETERS" \
#   --parameters "sourceDbTimezoneOffset=$SOURCE_DB_TIMEZONE_OFFSET" \
#   --parameters "dlqGcsPubSubSubscription=$DLQ_GCS_PUB_SUB_SUBSCRIPTION" \
#   --parameters "skipDirectoryName=$SKIP_DIRECTORY_NAME" \
#   --parameters "maxShardConnections=$MAX_SHARD_CONNECTIONS" \
#   --parameters "deadLetterQueueDirectory=$DEAD_LETTER_QUEUE_DIRECTORY" \
#   --parameters "dlqMaxRetryCount=$DLQ_MAX_RETRY_COUNT" \
#   --parameters "runMode=$RUN_MODE" \
#   --parameters "dlqRetryMinutes=$DLQ_RETRY_MINUTES"

