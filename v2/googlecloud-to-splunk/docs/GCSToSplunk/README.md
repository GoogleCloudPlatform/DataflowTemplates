# GCS To Splunk Dataflow Template

The [GCSToSplunk](../../src/main/java/com/google/cloud/teleport/v2/templates/GCSToSplunk.java) pipeline ingests
data from CSV files stored in GCS, optionally executes a UDF,
and writes those records into Splunk's HEC endpoint.

The template creates the Splunk payload as a JSON element using one of the following:

1. CSV headers (default)
2. JSON schema
3. Javascript UDF


If a Javascript UDF and JSON schema are both inputted as parameters, 
only the Javascript UDF will be executed.


## Getting Started

### Requirements

* Java 8
* Maven
* Cloud Storage bucket exists
* Splunk HEC instance exists

### Building Template

This is a Flex Template meaning that the pipeline code will be containerized, and the container will be
run on Dataflow.

#### Set Environment Variables

```sh
export PROJECT=<my-project>
export IMAGE_NAME=<my-image-name>
export BUCKET_NAME=gs://<bucket-name>
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java8-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export TEMPLATE_MODULE=googlecloud-to-splunk
export APP_ROOT=/template/gcs-to-splunk 
export COMMAND_SPEC=${APP_ROOT}/resources/gcs-to-splunk-command-spec.json
```

#### Build and Push Image to Google Container Repository

```sh
mvn clean package -f unified-templates.xml -pl v2/googlecloud-to-splunk -am \
    -Dimage=${TARGET_GCR_IMAGE} \
    -Dbase-container-image=${BASE_CONTAINER_IMAGE} \
    -Dbase-container-image.version=${BASE_CONTAINER_IMAGE_VERSION} \
    -Dapp-root=${APP_ROOT} \
    -Dcommand-spec=${COMMAND_SPEC}
```

#### Create Image Spec

Create a file with the metadata required for launching the Flex template. Once
created, this file should be placed in GCS.

The `gcs-to-splunk-image-spec.json` file in this directory
contains most of the content for this file. Simply update `image` property to
the value of `${TARGET_GCR_IMAGE}` defined earlier.

### Executing Template

The template requires the following parameters:

* inputFileSpec: Pattern of where the CSV file(s) are located in GCS, ex: gs://mybucket/somepath/*.csv
* containsHeaders: Set to "true" if CSV file contains headers, or "false" otherwise. An error is thrown if all files read from `inputFileSpec` do not follow the same header format. If set to "false", a JSON schema or Javascript UDF will need to be supplied. Default: null
* invalidOutputPath: Google Cloud Storage path where to write objects that could not be pushed to Splunk. Ex: gs://mybucket/somepath
* token: Splunk Http Event Collector (HEC) authentication token.
* url: Splunk Http Event Collector (HEC) url. This should be routable from the VPC in which the pipeline runs. e.g. https://splunk-hec-host:8088
* batchCount: Batch size for sending multiple events to Splunk HEC. Default: 1 (no batching).
* parallelism: Maximum number of parallel requests. Default: 1 (no parallelism).
* disableCertificateValidation: Disable SSL certificate validation (true/false). Default: false (validation enabled). If true, the certificates are not validated (all certificates are trusted).

The template has the following optional parameters:
* delimiter: Delimiting character in CSV file(s). Default: use delimiter found in `csvFormat` Example: ,
* csvFormat: CSV format according to [Apache Commons CSV format](https://commons.apache.org/proper/commons-csv/apidocs/org/apache/commons/csv/CSVFormat.html). Default is: DEFAULT
* jsonSchemaPath: Path to JSON schema, ex gs://path/to/schema. Default: null.
* javascriptTextTransformGcsPath: GCS path to Javascript UDF source. UDF will be preferred option for transformation if supplied. If this parameter is supplied, a `javascriptTextTransformFunctionName` parameter will also be required. Otherwise, the UDF will not execute. Default: null
* javascriptTextTransformFunctionName: UDF Javascript Function Name. If a `javascriptTextTransformGcsPath` parameter is supplied, this parameter is required. Otherwise, the UDF will not execute. Default: null

Template can be executed using the following `gcloud` command:

```sh
export JOB_NAME="${TEMPLATE_MODULE}-`date +%Y%m%d-%H%M%S`"
gcloud beta dataflow flex-template run ${JOB_NAME} \
        --project=${PROJECT} --region=us-central1 \
        --template-file-gcs-location=gs://path-to-image-spec-file \
        --parameters inputFileSpec=gs://path-to-csv-file  \
        --parameters containsHeaders=<true-or-false> \
        --parameters invalidOutputPath=gs://path-to-error-folder \
        --parameters url=<splunk-hec-url> \
        --parameters token=<splunk-token> \
        --parameters batchCount=<batch-count> \
        --parameters parallelism=1 \
        --parameters disableCertificateValidation="true"
```
