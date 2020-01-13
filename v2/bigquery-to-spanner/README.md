# BigQuery to Spanner Dataflow Template

The [BigQueryToSpanner](src/main/java/com/google/cloud/teleport/v2/templates/BigQueryToSpanner.java) pipeline exports data
from a BigQuery table into a Spanner table.

## Getting Started

### Requirements
* Java 8
* Maven
* BigQuery tables exists
* Spanner tables exists with equivalent schemas

### Building Template
This is a dynamic template meaning that the pipeline code will be containerized and the container will be
run on Dataflow.

#### Building Container Image
* Set environment variables that will be used in the build process.
```sh
export PROJECT=my-project
export IMAGE_NAME=my-image-name
export BUCKET_NAME=gs://<bucket-name>
export TABLE=my-table
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java8-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export APP_ROOT=/template/<template-class>
export COMMAND_SPEC=${APP_ROOT}/resources/bigquery-to-spanner-command-spec.json
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
* tableRef: BigQuery table to export from.
* bucket: Cloud Storage bucket to land exported data in.

The template has the following optional parameters:
* numShards: number of file shards to create. Default: 0 (runner will decide number of shards).
* fields: fields in table to export. Default: None (all fields will be exported).

Template can be executed using the following API call:
```sh
API_ROOT_URL="https://dataflow.googleapis.com"
TEMPLATES_LAUNCH_API="${API_ROOT_URL}/v1b3/projects/${PROJECT}/templates:launch"
JOB_NAME="bigquery-to-spanner-`date +%Y%m%d-%H%M%S-%N`"
time curl -X POST -H "Content-Type: application/json"     \
     -H "Authorization: Bearer $(gcloud auth print-access-token)" \
     "${TEMPLATES_LAUNCH_API}"`
     `"?validateOnly=false"`
     `"&dynamicTemplate.gcsPath=gs://path/to/image/spec"`
     `"&dynamicTemplate.stagingLocation=gs://path/to/stagingLocation" \
     -d '
      {
        "jobName":"'$JOB_NAME'",
        "parameters": {
          "bigQueryTables" "'$BIG_QUERY_TABLES'",
          "spannerTables": "$SPANNER_TABLES",
          "spannerDatabase": "$SPANNER_DATABASE",
          "spannerInstance": "$SPANNER_INSTANCE"
        }
      }
      '
