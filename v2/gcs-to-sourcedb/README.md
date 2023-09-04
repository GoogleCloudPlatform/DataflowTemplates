# GCS to SourceDB Dataflow Template

The [GCSToSourcedb](src/main/java/com/google/cloud/teleport/v2/templates/GCSToSourcedb.java) pipeline
ingests Spanner change stream data read from GCS, orders them and writes the data to source database by applying necessary transformation logic.
Currently only MySQL database is supported as source.


## Getting Started

### Requirements
* Java 11
* Maven


### Building Template
This is a Flex Template meaning that the pipeline code will be containerized and the container will be
used to launch the Dataflow pipeline.

#### Building Container Image
* Set environment variables.

Note: Some variable depend on whether the input buffer is Kafka or Pub/Sub

```sh
export PROJECT=<my-project>
export IMAGE_NAME=gcs-to-sourcedb
export BUCKET_NAME=gs://<bucket-name>
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java11-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export APP_ROOT=/template/${IMAGE_NAME}
export DATAFLOW_JAVA_COMMAND_SPEC=${APP_ROOT}/resources/${IMAGE_NAME}-command-spec.json
export TEMPLATE_IMAGE_SPEC=${BUCKET_NAME}/images/${IMAGE_NAME}-image-spec.json

gcloud config set project ${PROJECT}
```

* Build and push image to Google Container Repository

```sh
mvn clean package \
-Dimage=${TARGET_GCR_IMAGE} \
-Dbase-container-image=${BASE_CONTAINER_IMAGE} \
-Dbase-container-image.version=${BASE_CONTAINER_IMAGE_VERSION} \
-Dapp-root=${APP_ROOT} \
-Dcommand-spec=${DATAFLOW_JAVA_COMMAND_SPEC} \
-am -pl ${IMAGE_NAME}
```

### Testing Template

The template unit tests can be run using:
```sh
mvn test
```

### Executing Template
Template can be executed using the following API call:

```sh
export JOB_NAME="${IMAGE_NAME}-`date +%Y%m%d-%H%M%S-%N`"
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>
export REGION=us-central1
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/GCS_to_Sourcedb"

### Required
export SOURCE_SHARDS_FILE_PATH=<sourceShardsFilePath>
export SESSION_FILE_PATH=<sessionFilePath>
export START_TIMESTAMP=<startTimestamp>
export WINDOW_DURATION=<windowDuration>
export GCSINPUT_DIRECTORY_PATH=<GCSInputDirectoryPath>
export SPANNER_PROJECT_ID=<spannerProjectId>
export METADATA_INSTANCE=<metadataInstance>
export METADATA_DATABASE=<metadataDatabase>

### Optional
export SOURCE_TYPE="mysql"
export SOURCE_DB_TIMEZONE_OFFSET="+00:00"
export TIMER_INTERVAL="1"
export GCSLOOKUP_RETRY_COUNT="3"
export GCSLOOKUP_RETRY_INTERVAL="3"
export RUN_MODE="regular"

gcloud dataflow flex-template run "gcs-to-sourcedb-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "sourceShardsFilePath=$SOURCE_SHARDS_FILE_PATH" \
  --parameters "sessionFilePath=$SESSION_FILE_PATH" \
  --parameters "sourceType=$SOURCE_TYPE" \
  --parameters "sourceDbTimezoneOffset=$SOURCE_DB_TIMEZONE_OFFSET" \
  --parameters "timerInterval=$TIMER_INTERVAL" \
  --parameters "startTimestamp=$START_TIMESTAMP" \
  --parameters "windowDuration=$WINDOW_DURATION" \
  --parameters "GCSInputDirectoryPath=$GCSINPUT_DIRECTORY_PATH" \
  --parameters "spannerProjectId=$SPANNER_PROJECT_ID" \
  --parameters "metadataInstance=$METADATA_INSTANCE" \
  --parameters "metadataDatabase=$METADATA_DATABASE" \
  --parameters "GCSLookupRetryCount=$GCSLOOKUP_RETRY_COUNT" \
  --parameters "GCSLookupRetryInterval=$GCSLOOKUP_RETRY_INTERVAL" \
  --parameters "runMode=$RUN_MODE"

```
