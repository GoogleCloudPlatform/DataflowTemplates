# BigQuery to Google Cloud Storage Dataflow Template

The [BigQueryToParquet](src/main/java/com/google/cloud/teleport/v2/templates/BigQueryToParquet.java) pipeline exports data
from a BigQuery table into one or more Parquet files in Google Cloud Storage.

## Getting Started

### Requirements
* Java 8
* Maven
* BigQuery table exists
* Cloud Storage bucket exists

### Building Template
This is a Flex Template meaning that the pipeline code will be containerized and the container will be
run on Dataflow.

#### Building Container Image
* Set environment variables that will be used in the build process.
```sh
export PROJECT=<my-project>
export IMAGE_NAME=<my-image-name>
export BUCKET_NAME=gs://<bucket-name>
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java8-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export TEMPLATE_MODULE=bigquery-to-parquet
export APP_ROOT=/template/${TEMPLATE_MODULE}
export COMMAND_SPEC=${APP_ROOT}/resources/${TEMPLATE_MODULE}-command-spec.json
export TEMPLATE_IMAGE_SPEC=${BUCKET_NAME}/images/${TEMPLATE_MODULE}-image-spec.json

export INPUT_TABLE_SPEC=<my-project:my-dataset.my-table>
export PARQUET_BUCKET_NAME=gs://<bucket-name>
export NUM_SHARDS=3
export BQ_FIELDS=my-field1,my-field2

gcloud config set project ${PROJECT}
```
* Build and push image to Google Container Repository
```sh
mvn clean package \
    -Dimage=${TARGET_GCR_IMAGE} \
    -Dbase-container-image=${BASE_CONTAINER_IMAGE} \
    -Dbase-container-image.version=${BASE_CONTAINER_IMAGE_VERSION} \
    -Dapp-root=${APP_ROOT} \
    -Dcommand-spec=${COMMAND_SPEC} \
    -am -pl ${TEMPLATE_MODULE}
```

#### Creating Image Spec

Create file in Cloud Storage with path to container image in Google Container Repository.
```sh
echo '{
    "image":"'${TARGET_GCR_IMAGE}'",
    "metadata":{"name":"BigQuery to Google Cloud Storage",
    "description":"Replicate BigQuery data into Parquet files in GCS",
    "parameters":[
        {
            "name":"tableRef",
            "label":"BigQuery table to export from",
            "helpText":"BigQuery table to export from",
            "paramType":"TEXT",
            "isOptional":false
        },
        {
            "name":"bucket",
            "label":"Destination GCS Bucket",
            "helpText":"Cloud Storage bucket to land exported data in.",
            "paramType":"TEXT",
            "isOptional":false
        },
        {
            "name":"numShards",
            "label":"Number of file shards to create",
            "helpText":"Number of file shards to create. Default: 0 (runner will decide number of shards).",
            "paramType":"TEXT",
            "isOptional":true
        },
        {
            "name":"fields",
            "label":"Fields in table to export.",
            "helpText":"Fields in table to export. Default: None (all fields will be exported).",
            "paramType":"TEXT",
            "isOptional":true
        },
        {
            "name":"autoscalingAlgorithm","label":"Autoscaling algorithm to use",
            "helpText":"Autoscaling algorithm to use: THROUGHPUT_BASED",
            "paramType":"TEXT",
            "isOptional":true
        },
        {
            "name":"numWorkers","label":"Number of workers Dataflow will start with",
            "helpText":"Number of workers Dataflow will start with",
            "paramType":"TEXT",
            "isOptional":true
        },

        {
            "name":"maxNumWorkers","label":"Maximum number of workers Dataflow job will use",
            "helpText":"Maximum number of workers Dataflow job will use",
            "paramType":"TEXT",
            "isOptional":true
        },
        {
            "name":"workerMachineType","label":"Worker Machine Type to use in Dataflow Job",
            "helpText":"Machine Type to Use: n1-standard-4",
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
* tableRef: BigQuery table to export from
* bucket: Cloud Storage bucket to land exported data in.

The template has the following optional parameters:
* numShards: Number of file shards to create. Default: 0 (runner will decide number of shards).
* fields: Fields in table to export. Default: None (all fields will be exported).

Template can be executed using the following gcloud command:
```sh
export JOB_NAME="${TEMPLATE_MODULE}-`date +%Y%m%d-%H%M%S-%N`"
gcloud beta dataflow flex-template run ${JOB_NAME} \
        --project=${PROJECT} --region=us-central1 \
        --template-file-gcs-location=${TEMPLATE_IMAGE_SPEC} \
        --parameters tableRef=${INPUT_TABLE_SPEC},bucket=${PARQUET_BUCKET_NAME},numShards=${NUM_SHARDS},fields=${BQ_FIELDS}
```
