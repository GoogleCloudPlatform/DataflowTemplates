# Spanner Changestream to Sharded File Sink Dataflow Template

The [SpannerChangeStreamsToShardedFileSink](src/main/java/com/google/cloud/teleport/v2/templates/SpannerChangeStreamsToShardedFileSink.java) pipeline
ingests Spanner change stream data to GCS file sink sharded by logical shard id values.


## Getting Started

### Requirements
* Java 11
* Maven

### Building Template
This is a Flex Template meaning that the pipeline code will be containerized and the container will be
used to launch the Dataflow pipeline.

#### Building Container Image
* Set environment variables.


```sh
export PROJECT=<my-project>
export IMAGE_NAME=spanner-change-streams-to-sharded-file-sink
export BUCKET_NAME=gs://<bucket-name>
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/spanner-change-streams-to-sharded-file-sink
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
gcloud beta dataflow flex-template run ${JOB_NAME} \
        --project=${PROJECT} --region=us-central1 \
        --template-file-gcs-location=${TEMPLATE_IMAGE_SPEC} \
        --additional-experiments=use_runner_v2 \
        --parameters "changeStreamName=<change-stream-name>" \
        --parameters "instanceId=<spanner-isntance-id>" \
        --parameters "databaseId=<spanner-database-id>" \
        --parameters "spannerProjectId=<spanner-project-id>" \
        --parameters "metadataInstance=<spanner-metadata-instance>" \
        --parameters "metadataDatabase=<spanner-metadata-db>" \
        --parameters "sessionFilePath=<session-file-path>" \
        --parameters "startTimestamp=2023-08-10T09:00:00Z" \
        --parameters "gcsOutputDirectory=<gcs-output-directory-path>" \
        --parameters "windowDuration=<window-duration>"


```
