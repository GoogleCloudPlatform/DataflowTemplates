# DataStream to Spanner Dataflow Template

The [DataStreamToSpanner](src/main/java/com/google/cloud/teleport/v2/templates/DataStreamToSpanner.java) pipeline
ingests data supplied by DataStream, optionally applies a Javascript or Python UDF if supplied 
and writes the data to Cloud Spanner database.

## Getting Started

### Requirements
* Java 8
* Maven
* DataStream stream is created and sending data to storage
* GCS Bucket that contains data

### Building Template
This is a Flex Template meaning that the pipeline code will be containerized and the container will be
used to launch the Dataflow pipeline.

#### Building Container Image
* Set environment variables.

```sh
export PROJECT=<my-project>
export IMAGE_NAME=datastream-to-spanner
export BUCKET_NAME=gs://<bucket-name>
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java8-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export APP_ROOT=/template/${IMAGE_NAME}
export DATAFLOW_JAVA_COMMAND_SPEC=${APP_ROOT}/resources/${IMAGE_NAME}-command-spec.json
export TEMPLATE_IMAGE_SPEC=${BUCKET_NAME}/images/${IMAGE_NAME}-image-spec.json

export TOPIC=projects/${PROJECT}/topics/<topic-name>
export SUBSCRIPTION=projects/${PROJECT}/subscriptions/<subscription-name>
export DEADLETTER_TABLE=${PROJECT}:${DATASET_TEMPLATE}.dead_letter

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

The template requires the following parameters:
* inputFilePattern: The GCS location which contains the DataStream's data.
* instanceId: The Cloud Spanner instance id where the stream must be replicated.
* databaseId: The Cloud Spanner database id where the stream must be replicated.
* outputDeadletterTable: Deadletter table for failed inserts.

Template can be executed using the following API call:
```sh
export JOB_NAME="${IMAGE_NAME}-`date +%Y%m%d-%H%M%S-%N`"
gcloud beta dataflow flex-template run ${JOB_NAME} \
        --project=${PROJECT} --region=us-central1 \
        --template-file-gcs-location=${TEMPLATE_IMAGE_SPEC} \
        --parameters instanceId=${INSTANCE_ID},databaseId=${DATABASE_ID},inputFilePattern=${GCS_LOCATION},outputDeadletterTable=${DEADLETTER_TABLE}
```
