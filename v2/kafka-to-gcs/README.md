# Kafka To Google Cloud Storage Dataflow Template

The [KafkaToGCS](src/main/java/com/google/cloud/teleport/v2/templates/KafkaToGCS.java) pipeline reads message from Kafka topic(s) and stores to Google Cloud Storage bucket in user specified file format. The sink data can be stored in a Text, Avro or a Parquet File Format.

## Getting started

### Requirements
* Java 8
* Maven
* Kafka Bootstrap Server(s).
* Kafka Topic(s) exists.
* Google Cloud Storage output bucket exists.


### Building Template
This is a flex template meaning that the pipeline code will be containerized and the container will be used to launch the pipeline.

#### Building Container Image
* Set environment variables that will be used in the build process.
```sh
export PROJECT=<my-project>
export IMAGE_NAME=<my-image-name>
export BUCKET_NAME=gs://<bucket-name>
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java8-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export TEMPLATE_MODULE=kafka-to-gcs
export APP_ROOT=/template/${TEMPLATE_MODULE}
export COMMAND_SPEC=${APP_ROOT}/resources/${TEMPLATE_MODULE}-command-spec.json
export TEMPLATE_IMAGE_SPEC=${BUCKET_NAME}/images/${TEMPLATE_MODULE}-image-spec.json

export BOOTSTRAP=<my-comma-separated-bootstrap-servers>
export TOPICS=<my-topics>
export OUTPUT_DIRECTORY=gs://<bucket-name>/path/
```
* Build and push image to Google Container Repository
```sh
mvn clean package -Dimage=${TARGET_GCR_IMAGE} \
                  -Dbase-container-image=${BASE_CONTAINER_IMAGE} \
                  -Dbase-container-image.version=${BASE_CONTAINER_IMAGE_VERSION} \
                  -Dapp-root=${APP_ROOT} \
                  -Dcommand-spec=${COMMAND_SPEC} \
                  -am -pl ${TEMPLATE_MODULE}
```

#### Creating Image Spec

Create file in Cloud Storage with path to container image in Google Container Repository.
```json
{
  "image": "gcr.io/project/my-image-name:latest",
  "sdk_info": {
    "language": "JAVA"
  }
}
```

### Testing Template

The template unit tests can be run using:
```sh
mvn test
```

### Executing Template

The template requires the following parameters:
* bootstrapServers: Kafka Bootstrap Server(s).
* inputTopics: Kafka topic(s) to read the input from.
* outputDirectory: Cloud Storage bucket to output files to.


The template has the following optional parameters:
* outputFileFormat: The format of the file to write to. Valid formats are Text, Avro and Parquet. Default: text.
* outputFilenamePrefix: The filename prefix of the files to write to. Default: output.
* windowDuration: The window duration in which data will be written. Default: 5m.
* numShards: Number of file shards to create. Default: decided by runner.

Template can be executed using the following gcloud command.
```sh
export JOB_NAME="${TEMPLATE_MODULE}-`date +%Y%m%d-%H%M%S-%N`"
gcloud beta dataflow flex-template run ${JOB_NAME} \
        --project=${PROJECT} --region=us-central1 \
        --template-file-gcs-location=${TEMPLATE_IMAGE_SPEC} \
        --parameters inputTopics=${TOPICS},bootstrapServers=${BOOTSTRAP},outputDirectory=${OUTPUT_DIRECTORY},outputFileFormat=text,outputFilenamePrefix=output,windowDuration=5m,numShards=5

```
