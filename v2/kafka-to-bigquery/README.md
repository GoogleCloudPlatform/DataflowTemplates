# Kafka to BigQuery Dataflow Template

The [KafkaToBigQuery](src/main/java/com/google/cloud/teleport/v2/templates/KafkaToBigQuery.java) pipeline is a 
streaming pipeline which ingests text data from Kafka, executes a UDF, and outputs the resulting records to BigQuery. 
Any errors which occur in the transformation of the data, execution of the UDF, or inserting into the output table will be
inserted into a separate errors table in BigQuery. The errors table will be created if it does
not exist prior to execution. Both output and error tables are specified by the user as parameters.

## Getting Started

### Requirements
* Java 8
* Maven
* The Kafka topic(s) exists and the message is encoded in a valid JSON format.
* The BigQuery output table exists.
* The Kafka brokers are reachable from the Dataflow worker machines.

### Building Template
This template is a dynamic template meaning that the pipeline code will be containerized and the container will be 
run on Dataflow. 

#### Building Container Image
* Set Environment Variables
```sh
export PROJECT=my-project
export IMAGE_NAME=my-image-name
export OUTPUT_TABLE=${PROJECT}:dataflow_template.temperature_data
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java8-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export APP_ROOT=/template/<template-class>
export COMMAND_SPEC=${APP_ROOT}/resources/kafka-to-bigquery-command-spec.json
export BOOTSTRAP=my-comma-separated-bootstrap-servers
export TOPICS=my-topics
export JS_PATH=gs://path/to/udf
export JS_FUNC_NAME=my-js-function
```
* Build and push image to GCR
```sh
mvn clean package -Dimage=${TARGET_GCR_IMAGE} \
                  -Dbase-container-image=${BASE_CONTAINER_IMAGE} \
                  -Dbase-container-image.version=${BASE_CONTAINER_IMAGE_VERSION} \
                  -Dapp-root=${APP_ROOT} \
                  -Dcommand-spec=${COMMAND_SPEC}
```

#### Creating Image Spec

Create file in Cloud Storage with path to container image in Google Container Repository:
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
* outputTableSpec: BigQuery table to write Kafka messages to.
* inputTopics: Comma separated list of Kafka topics to read from.
* javascriptTextTransformGcsPath: Path to javascript function in GCS.
* javascriptTextTransformFunctionName: Name of javascript function. 
* bootstrapServers: Comma separated list of bootstrap servers.

The template allows for the user to supply the following optional parameters:
* outputDeadletterTable: BigQuery table to output deadletter records to. Default: outputTableSpec_error_records

Template can be executed using the following API call.
```sh
API_ROOT_URL="https://dataflow.googleapis.com"
TEMPLATES_LAUNCH_API="${API_ROOT_URL}/v1b3/projects/${PROJECT}/templates:launch"
JOB_NAME="kafka-to-bigquery-`date +%Y%m%d-%H%M%S-%N`"
time curl -X POST -H "Content-Type: application/json"     \
     -H "Authorization: Bearer $(gcloud auth print-access-token)" \
     "${TEMPLATES_LAUNCH_API}"`
     `"?validateOnly=false"`
     `"&dynamicTemplate.gcsPath=gs://path/to/kafka-image-spec"`
     `"&dynamicTemplate.stagingLocation=gs://path/to/staging" \
     -d '
       {
        "jobName":"'$JOB_NAME'",
        "parameters": {
            "outputTableSpec":"'$OUTPUT_TABLE'",
            "inputTopics":"'$TOPICS'",
            "javascriptTextTransformGcsPath":"'$JS_PATH'",
            "javascriptTextTransformFunctionName":"'$JS_FUNC_NAME'",
            "bootstrapServers":"'$BOOTSTRAP'",
        }
     }
    '
```