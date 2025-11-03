# DataStream to PostgreSQL Dataflow Template

The [DataStreamToPostgres](src/main/java/com/google/cloud/teleport/v2/templates/DataStreamToPostgres.java) pipeline
ingests data supplied by DataStream and writes the data to a Postgres Database.

## Getting Started
TODO theres a lot left to add below

### Requirements
* Java 11
* Maven
* DataStream stream is created and sending data to storage
* Postgres DB is accessible via Dataflow and has schema matched to Oracle.

### Building Template
This is a Flex Template meaning that the pipeline code will be containerized and the container will be
used to launch the Dataflow pipeline.

#### Building Container Image
* Set environment variables.

```sh
export PROJECT=<my-project>
export IMAGE_NAME=datastream-to-postgres
export BUCKET_NAME=gs://<bucket-name>
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java17-template-launcher-base
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

#### Creating Image Spec

* Create file in Cloud Storage with path to container image in Google Container Repository.
```sh
echo '{
    "image":"'${TARGET_GCR_IMAGE}'",
    "metadata":{"name":"DataStream to PostgreSQL",
    "description":"Replicate DataStream data into a PostgreSQL database.",
    "parameters":[
        {
            "name":"inputFilePattern",
            "label":"Cloud Storage Input File(s)",
            "helpText":"Path of the file pattern glob to read from.",
            "paramType":"GCS_READ_FILE"
        },
        {
            "name":"gcsPubSubSubscription",
            "label":"Pub/Sub subscription for GCS notifications",
            "helpText":"The Pub/Sub subscription with DataStream file notifications.",
            "paramType":"TEXT"
        },
        {
            "name":"inputFileFormat",
            "label":"Input file format",
            "helpText":"The GCS output format avro/json.",
            "paramType":"TEXT",
            "isOptional":true
        },
        {
            "name":"streamName",
            "label":"DataStream stream name",
            "helpText":"The DataStream Stream to Reference.",
            "paramType":"TEXT"
        },
        {
            "name":"rfcStartDateTime",
            "label":"Start date/time for processing",
            "helpText":"The starting DateTime used to fetch from Cloud Storage (https://tools.ietf.org/html/rfc3339).",
            "paramType":"TEXT",
            "isOptional":true
        },
        {
            "name":"dataStreamRootUrl",
            "label":"DataStream API root URL",
            "helpText":"Datastream API Root URL (only required for testing).",
            "paramType":"TEXT",
            "isOptional":true
        },
        {
            "name":"databaseHost",
            "label":"Database host",
            "helpText":"Database Host to connect on.",
            "paramType":"TEXT"
        },
        {
            "name":"databasePort",
            "label":"Database port",
            "helpText":"Database Port to connect on.",
            "paramType":"TEXT",
            "isOptional":true
        },
        {
            "name":"databaseUser",
            "label":"Database user",
            "helpText":"Database User to connect with.",
            "paramType":"TEXT",
            "isOptional":true
        },
        {
            "name":"databasePassword",
            "label":"Database password",
            "helpText":"Database Password for given user.",
            "paramType":"PASSWORD"
        },
        {
            "name":"databaseName",
            "label":"Database name",
            "helpText":"The database name to connect to.",
            "paramType":"TEXT",
            "isOptional":true
        },
        {
            "name":"datastreamSourceType",
            "label":"Datastream source type override",
            "helpText":"Override the source type detection for Datastream CDC data. When specified, this value will be used instead of deriving the source type from the read_method field. Valid values include 'mysql', 'postgresql', 'oracle', etc. This parameter is useful when the read_method field contains 'cdc' and the actual source type cannot be determined automatically.",
            "paramType":"TEXT",
            "isOptional":true
        }
    ]},
    "sdk_info":{"language":"JAVA"}
}' > image_spec.json
gsutil cp image_spec.json ${TEMPLATE_IMAGE_SPEC}
rm image_spec.json
```

### Testing Template

The template unit tests can be run using:
```sh
mvn test
```

### Executing Template

The template requires the following parameters:
* outputDatasetTemplate: The name of the dataset or templated logic to extract it (ie. 'prefix_{schema_name}')
* outputTableNameTemplate: The name of the table or templated logic to extract it (ie. 'prefix_{table_name}')

The template has the following optional parameters:
* maxRetryAttempts: Max retry attempts, must be > 0. Default: no retries
* maxRetryDuration: Max retry duration in milliseconds, must be > 0. Default: no retries

Template can be executed using the following API call:
```sh
export JOB_NAME="${IMAGE_NAME}-`date +%Y%m%d-%H%M%S-%N`"
gcloud beta dataflow flex-template run ${JOB_NAME} \
        --project=${PROJECT} --region=us-central1 \
        --template-file-gcs-location=${TEMPLATE_IMAGE_SPEC} \
        --parameters inputSubscription=${SUBSCRIPTION},outputDeadletterTable=${DEADLETTER_TABLE}
```
