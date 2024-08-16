# GCS Avro To Spanner using Slow Changing Dimensions (SCD)

The [GCS Avro to Spanner using SCD](src/main/java/com/google/cloud/teleport/v2/spanner/AvroToSpannerScd.java) pipeline
ingests Avro data read from GCS into Spanner using SCD Types.

Currently, it supports:

* **SCD Type 1**: updates existing row if the primary key exists, or inserts a new row otherwise.
* **SCD Type 2**: updates existing row's end date to the current timestamp if the primary key exists, and inserts a new row with null end date and start date with the current timestamp if the column is passed.

## Getting Started

### Requirements

* Java 11
* Maven

### Building Template

This is a Flex Template meaning that the pipeline code will be containerized and the container will be
used to launch the Dataflow pipeline.

#### Building Container Image

* Set environment variables.

Note: Some variable depend on the SCD Type of choice.

```sh
export PROJECT=<my-project>
export IMAGE_NAME=gcs-avro-to-spanner-scd
export BUCKET_NAME=gs://<bucket-name>
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java11-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export APP_ROOT=/template/${IMAGE_NAME}
export DATAFLOW_JAVA_COMMAND_SPEC=${APP_ROOT}/resources/${IMAGE_NAME}-command-spec.json
export TEMPLATE_IMAGE_SPEC=${BUCKET_NAME}/images/${IMAGE_NAME}-image-spec.json

gcloud config set project ${PROJECT}
```

* Build and push image to Google Container Repository.

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
export REGION=<region>
export TEMPLATE_SPEC_GCS_PATH="gs://$BUCKET_NAME/templates/flex/gcs_avro_to_spanner_scd"

### Required
export INPUT_FILE_PATTERN=<inputFilePattern>
export INPUT_AVRO_SCHEMA=<avroSchema>
export SPANNER_INSTANCE_ID=<instanceId>
export SPANNER_DATABASE_ID=<databaseId>
export SPANNER_TABLE_NAME=<tableName>

### Optional
export SPANNER_PROJECT_ID=<spannerProjectId>
export SPANNER_HOST=https://batch-spanner.googleapis.com
export SPANNER_PRIORITY=<spannerPriority>
export SPANNER_BATCH_SIZE=<batchSize>
export SCD_TYPE=<scdType>
export WAIT_UNTIL_FINISH=false

### Required, for SCD Type TYPE_2.
export SPANNER_PRIMARY_KEY_COLUMN_NAMES=<primaryKeyColumnName>
export SPANNER_END_DATE_COLUMN_NAME=<startDateColumnName>

#### Optional, for SCD Type 2.
export SPANNER_START_DATE_COLUMN_NAME=<startDateColumnName>


gcloud dataflow jobs run "gcs-avro-to-spanner-scd-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --gcs-location "$TEMPLATE_SPEC_GCS_PATH" \
  --parameters "inputFilePattern=$INPUT_FILE_PATTERN" \
  --parameters "spannerProjectId=$SPANNER_PROJECT_ID" \
  --parameters "instanceId=$SPANNER_INSTANCE_ID" \
  --parameters "databaseId=$SPANNER_DATABASE_ID" \
  --parameters "spannerHost=$SPANNER_HOST" \
  --parameters "spannerPriority=$SPANNER_PRIORITY" \
  --parameters "spannerBatchSize=$SPANNER_BATCH_SIZE" \
  --parameters "tableName=$SPANNER_TABLE_NAME" \
  --parameters "scdType=$SCD_TYPE" \
  --parameters "primaryKeyColumnNames=$SPANNER_PRIMARY_KEY_COLUMN_NAMES" \
  --parameters "startDateColumnName=$SPANNER_START_DATE_COLUMN_NAME" \
  --parameters "endDateColumnName=$SPANNER_END_DATE_COLUMN_NAME" \
  --parameters "waitUntilFinish=$WAIT_UNTIL_FINISH"
```
