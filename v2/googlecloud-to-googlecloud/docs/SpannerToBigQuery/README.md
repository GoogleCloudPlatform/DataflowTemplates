# Spanner to BigQuery Dataflow Template

The 
[Spanner to BigQuery](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/googlecloud-to-googlecloud/src/main/java/com/google/cloud/teleport/v2/templates/SpannerToBigQuery.java)
pipeline is a  batch pipeline that reads records from a Spanner Table and writes them to a BigQuery table.

## Requirements

* Java 8 or 11
* Maven
* The Spanner table exists

## Getting Started

### Building Template

This is a Flex Template meaning that the pipeline code will be containerized and the container will be run on Dataflow.

### Building Container Image

**Set environment variables to be used in the build process.**

```sh
export PROJECT="<project-id>"
export IMAGE_NAME=spanner-to-bigquery
export BUCKET="gs://<GCS-bucket-name>"
export TEMPLATE_MODULE=googlecloud-to-googlecloud
export COMMAND_MODULE=spanner-to-bigquery

export TARGET_GCR_IMAGE="gcr.io/$PROJECT/images/$IMAGE_NAME"
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java8-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export APP_ROOT="/template/$COMMAND_MODULE"
export COMMAND_SPEC="$APP_ROOT/resources/$COMMAND_MODULE-command-spec.json"

export IMAGE_SPEC_METADATA="<path to local image-spec-metadata.json file>"
export IMAGE_SPEC_GCSPATH="$BUCKET/images/$COMMAND_MODULE-image-spec.json"
```

**Build and push image to Google Container Repository from the DataflowTemplates directory.**

```sh
mvn clean package -f unified-templates.xml \
    -Dimage="$TARGET_GCR_IMAGE" \
    -Dbase-container-image="$BASE_CONTAINER_IMAGE" \
    -Dbase-container-image.version="$BASE_CONTAINER_IMAGE_VERSION" \
    -Dapp-root="$APP_ROOT" \
    -Dcommand-spec="$COMMAND_SPEC" \
    -am -pl "v2/$TEMPLATE_MODULE"
```

### Creating Image Spec

Create a metadata file locally using the contents of the `spanner-to-bigquery-image-spec-metadata.json` file in this
directory.

Run the following command to build and upload complete image spec file to Google Container Repository.

```sh
gcloud dataflow flex-template build "$IMAGE_SPEC_GCSPATH" \
        --image "$TARGET_GCR_IMAGE" \
        --sdk-language "JAVA" \
        --metadata-file "$IMAGE_SPEC_METADATA"
```

### Testing Template

The template unit tests can be run using:

```sh
mvn test -f unified-templates.xml -pl v2/"$TEMPLATE_MODULE" -am
```

### Executing Template

The template requires the following parameters:

* spannerInstanceId:  The instance id for the Spanner table.
* spannerDatabaseId: The database id for the Spanner table.
* spannerTableId: The Spanner table to read data from.
* sqlQuery: The SQL Query to use for reading from Spanner table.
* bigQuerySchemaPath: GCS Path to JSON Schema file for BigQuery Table. Example: gs://your-bucket/your-bigquery-schema.json
* outputTableSpec: The location of the output table. Example: project-id:dataset.table.

The template has the following optional parameters:

* createDisposition: Create disposition for the BigQuery Table. Can be either CREATE_IF_NEEDED or CREATE_NEVER. Default: CREATE_IF_NEEDED.
* writeDisposition: Write Disposition for the BigQuery Table. Can be either WRITE_APPEND, WRITE_EMPTY, or WRITE_TRUNCATE. 
Default: WRITE_APPEND. 
* spannerRpcPriority: Spanner RPC Priority for the job. Can be HIGH, MEDIUM, or LOW. Default: HIGH. 

Template can be executed using the following gcloud command:

```sh
export JOB_NAME="<your-job-name>"
export REGION="<your-region>"

export SPANNER_TABLE="<spanner-table-id>"
export SPANNER_INSTANCE="<spanner-instance-id>"
export SPANNER_DATABASE="<spanner-database-id>"
export SQL_QUERY="<your-sql-query>"
export SCHEMA_PATH="<gcs-path-to-json-schema-file>"
export OUTPUT_TABLE="<project-id:dataset.table>"

gcloud dataflow flex-template run "$JOB_NAME-$(date +'%Y%m%d-%H%M%S-%N')" \
        --project="$PROJECT" \
        --region="$REGION" \
        --template-file-gcs-location="$IMAGE_SPEC_GCSPATH" \
        --parameters spannerInstanceId="$SPANNER_INSTANCE" \
        --parameters spannerDatabaseId="$SPANNER_DATABASE" \
        --parameters spannerTableId="$SPANNER_TABLE" \
        --parameters sqlQuery="$SQL_QUERY" \
        --parameters bigQuerySchemaPath="$SCHEMA_PATH" \
        --parameters outputTableSpec="$OUTPUT_TABLE" 
```
