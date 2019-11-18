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
export PROJECT=my-project
export IMAGE_NAME=my-image-name
export BUCKET_NAME=gs://<bucket-name>
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java8-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export APP_ROOT=/template/<template-class>
export COMMAND_SPEC=${APP_ROOT}/resources/kafka-to-gcs-command-spec.json
```
* Build and push image to Google Container Repository
```sh
mvn clean package -Dimage=${TARGET_GCR_IMAGE} \
                  -Dbase-container-image=${BASE_CONTAINER_IMAGE} \
                  -Dbase-container-image.version=${BASE_CONTAINER_IMAGE_VERSION} \
                  -Dapp-root=${APP_ROOT} \
                  -Dcommand-spec=${COMMAND_SPEC}
```

#### Creating Image Spec

Create file in Cloud Storage with path to container image in Google Container Repository.
```json
{
  "docker_template_spec": {
    "docker_image": "gcr.io/project/my-image-name"
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

  
Template can be executed using the following API call:
```sh
API_ROOT_URL="https://dataflow.googleapis.com"
TEMPLATES_LAUNCH_API="${API_ROOT_URL}/v1b3/projects/${PROJECT}/templates:launch"
JOB_NAME="kafka-to-gcs-`date +%Y%m%d-%H%M%S-%N`"
time curl -X POST -H "Content-Type: application/json"     \
     -H "Authorization: Bearer $(gcloud auth print-access-token)" \
     "${TEMPLATES_LAUNCH_API}"`
     `"?validateOnly=false"`
     `"&dynamicTemplate.gcsPath=${BUCKET_NAME}/path/to/image-spec"`
     `"&dynamicTemplate.stagingLocation=${BUCKET_NAME}/staging" \
     -d '
      {
       "jobName":"'$JOB_NAME'",
       "parameters": {
           "bootstrapServers":"broker_1:9092,broker_2:9092",
           "inputTopics":"topic1,topic2",
           "outputDirectory":"'$BUCKET_NAME/path/to/output-location'",
           "outputFileFormat":"text",
           "outputFilenamePrefix":"output",
           "windowDuration":"5m",
           "numShards":"5"
        }
       }
      '
```
