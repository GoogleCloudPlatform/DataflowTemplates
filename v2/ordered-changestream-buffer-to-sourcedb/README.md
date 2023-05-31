# Ordered Changestream Buffer to SourceDB Dataflow Template

The [OrderedChangestraemBufferToSourcedb](src/main/java/com/google/cloud/teleport/v2/templates/OrderedBufferToSourcedb.java) pipeline
ingests Spanner change stream data read from buffer like PubSub or Kafka, and writes the data to source database by applying necessary transformation logic.
Currently only MySQL database is supported as source.


## Getting Started

### Requirements
* Java 11
* Maven
* When the buffer is PubSub, PubSub subscribers per shard are setup, with subscription id as the shard id and the attritibue filter set to shard id fileter, example: attributes.shardId="shardA"
* When the buffer is Kafka, Kafka cluster is setup and change stream data is present on Kafka topic
* Network connectivity is setup between GCP and source database location

### Building Template
This is a Flex Template meaning that the pipeline code will be containerized and the container will be
used to launch the Dataflow pipeline.

#### Building Container Image
* Set environment variables.

Note: Some variable depend on whether the input buffer is Kafka or Pub/Sub

```sh
export PROJECT=<my-project>
export IMAGE_NAME=ordered-changestream-buffer-to-sourcedb
export BUCKET_NAME=gs://<bucket-name>
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java11-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export APP_ROOT=/template/${IMAGE_NAME}
export DATAFLOW_JAVA_COMMAND_SPEC=${APP_ROOT}/resources/${IMAGE_NAME}-command-spec.json
export TEMPLATE_IMAGE_SPEC=${BUCKET_NAME}/images/${IMAGE_NAME}-image-spec.json
export SOURCE_SHARD_FILE=gs://<file-location-to-source-shard>
export SESSION_FILE=gs://<file-location-to-session-file>

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
        --parameters sourceShardsFilePath=${SOURCE_SHARD_FILE},sessionFilePath=${SESSION_FILE}

```
