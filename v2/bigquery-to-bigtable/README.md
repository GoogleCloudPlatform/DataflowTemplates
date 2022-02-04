# BigQuery to Bigtable Dataflow Template

The [BigQueryToBigtable](src/main/java/com/google/cloud/teleport/v2/templates/BigQueryToBigtable.java) pipeline exports data
from BigQuery using a query into a Cloud Bigtable table.

## Getting Started

### Requirements
* Java 8
* Maven
* BigQuery table exists
* Bigtable table exists with a column family

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
export TEMPLATE_MODULE=bigquery-to-bigtable
export APP_ROOT=/template/${TEMPLATE_MODULE}
export COMMAND_SPEC=${APP_ROOT}/resources/${TEMPLATE_MODULE}-command-spec.json
export TEMPLATE_IMAGE_SPEC=${BUCKET_NAME}/images/${TEMPLATE_MODULE}-image-spec.json

export REGION=us-central1
export READ_QUERY="select * from my-table"
export READ_ID_COLUMN=<id>
export BIGTABLE_WRITE_PROJECT_ID=<my-project>
export BIGTABLE_WRITE_INSTANCE_ID=<my-instance>
export BIGTABLE_WRITE_TABLE_ID=<my-table>
export BIGTABLE_WRITE_COLUMN_FAMILY=<my-column-family>

gcloud config set project ${PROJECT}
```
* Build and push image to Google Container Repository from the v2 directory
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
  "image": "'${TARGET_GCR_IMAGE}'",
  "metadata": {
    "name": "BigQuery to Bigtable",
    "description": "Export BigQuery data into Bigtable",
    "parameters": [
      {
        "name": "readQuery",
        "label": "BigQuery query",
        "helpText": "BigQuery query to export data from",
        "paramType": "TEXT",
        "isOptional": false
      },
      {
        "name": "readIdColumn",
        "label": "BigQuery query unique column ID",
        "helpText": "BigQuery query unique column ID",
        "paramType": "TEXT",
        "isOptional": false
      },
      {
        "name": "bigtableWriteProjectId",
        "label": "Bigtable project id",
        "helpText": "Bigtable project id to write to",
        "paramType": "TEXT",
        "isOptional": false
      },
      {
        "name": "bigtableWriteInstanceId",
        "label": "Bigtable instance id",
        "helpText": "Bigtable instance id to write to",
        "paramType": "TEXT",
        "isOptional": false
      },
      {
        "name": "bigtableWriteAppProfile",
        "label": "Bigtable app profile",
        "helpText": "Bigtable app profile to use for the export",
        "paramType": "TEXT",
        "isOptional": true
      },
      {
        "name": "bigtableWriteTableId",
        "label": "Bigtable table id",
        "helpText": "Bigtable table id to write to",
        "paramType": "TEXT",
        "isOptional": false
      },
      {
        "name": "bigtableWriteColumnFamily",
        "label": "Bigtable table column family",
        "helpText": "Bigtable table column family id to write to",
        "paramType": "TEXT",
        "isOptional": false
      }
    ]
  },
  "sdk_info": {
    "language": "JAVA"
  }
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
* readQuery: BigQuery query to export data from
* readIdColumn: BigQuery query unique column ID
* bigtableWriteProjectId: Bigtable project id to write to
* bigtableWriteInstanceId: Bigtable instance id to write to
* bigtableWriteTableId: Bigtable table id to write to
* bigtableWriteColumnFamily: Bigtable table column family id to write to

The template has the following optional parameters:
* bigtableWriteAppProfile: Bigtable app profile to use for the export

Template can be executed using the following gcloud command:
```sh
export JOB_NAME="${TEMPLATE_MODULE}-`date +%Y%m%d-%H%M%S-%N`"
gcloud beta dataflow flex-template run ${JOB_NAME} \
        --project=${PROJECT} --region=${REGION} \
        --template-file-gcs-location=${TEMPLATE_IMAGE_SPEC} \
        --parameters ^~^readQuery="${READ_QUERY}" \
        --parameters readIdColumn=${READ_ID_COLUMN},bigtableWriteProjectId=${BIGTABLE_WRITE_PROJECT_ID},bigtableWriteInstanceId=${BIGTABLE_WRITE_INSTANCE_ID},bigtableWriteTableId=${BIGTABLE_WRITE_TABLE_ID},bigtableWriteColumnFamily=${BIGTABLE_WRITE_COLUMN_FAMILY}
```

Note: The `^~^` prefix on readQuery is used to make `~` a delimiter instead of 
commas. This allows commas to be used in the query. Read more about [gcloud topic escaping](https://cloud.google.com/sdk/gcloud/reference/topic/escaping).

#### Example query

Here is an example query using a public dataset. It combines a few values into a rowkey with a `#` between each value.

```
export READ_QUERY="SELECT CONCAT(SenderCompID,'#', OrderID) as rowkey, * FROM bigquery-public-data.cymbal_investments.trade_capture_report LIMIT 100"
```