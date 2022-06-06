# GCS To Splunk Dataflow Template

The [GCSToSplunk](../../src/main/java/com/google/cloud/teleport/v2/templates/GCSToSplunk.java) pipeline ingests
data from CSV files stored in GCS, optionally executes a UDF, converts the output to [SplunkEvent](https://github.com/apache/beam/blob/master/sdks/java/io/splunk/src/main/java/org/apache/beam/sdk/io/splunk/SplunkEvent.java) objects,
and writes those records into Splunk's HEC endpoint.
The template creates the SplunkEvent's payload as a JSON element using one of the following:
1. Javascript UDF (if provided)
2. JSON schema (if provided)
3. CSV headers* (default)

If either a UDF or JSON schema is provided then it will be used instead of the CSV headers.

## Getting Started

### Requirements
* Java 8
* Maven
* Cloud Storage bucket exists
* Splunk HEC instance exists

### Building Template
This is a Flex Template meaning that the pipeline code will be containerized, and the container will be
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
export TEMPLATE_MODULE=googlecloud-to-splunk
export APP_ROOT=/template/gcs-to-splunk 
export COMMAND_SPEC=${APP_ROOT}/resources/gcs-to-splunk-command-spec.json
export TEMPLATE_IMAGE_SPEC=${BUCKET_NAME}/images/gcs-to-splunk-image-spec.json

export INPUT_FILE_SPEC=<gcs-path-to-csv>
export CONTAINS_HEADERS=<true-or-false>
export INVALID_OUTPUT_PATH=<gcs-error-path>
export SPLUNK_URL=<splunk-hec-url>
export SPLUNK_TOKEN=<splunk-token>
export BATCH_COUNT=<batch-count>
export PARALLELISM=1

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
    "defaultEnvironment": {},
    "image": "gcr.io/cloud-teleport-testing/gcs-to-splunk",
    "metadata": {
        "description": "A pipeline that reads from a GCS bucket and writes to Splunk's HTTP Event Collector (HEC).",
        "name": "GCS to Splunk",
        "parameters": [
            {
                "helpText": "GCS files to read the input. Example: gs://mybucket/test-*.csv",
                "isOptional": false,
                "label": "GCS file pattern to search for CSV files",
                "name": "inputFileSpec",
                "paramType": "TEXT",
                "regexes": [
                    "^gs:\\/\\/[^\\n\\r]+$"
                ]
            },
            {
                "helpText": "True if CSVs include headers or False if they do not.",
                "isOptional": false,
                "label": "GCS file pattern to search for CSV files",
                "name": "containsHeaders",
                "paramType": "TEXT",
                "regexes": [
                    "^true|^false"
                ]
            },
            {
                "helpText": "The delimiter that the CSV uses. Example: ,",
                "isOptional": true,
                "label": "CSV delimiter",
                "name": "delimiter",
                "paramType": "TEXT"
            },
            {
                "helpText": "Csv format according to [Apache Commons CSV format](https://commons.apache.org/proper/commons-csv/apidocs/org/apache/commons/csv/CSVFormat.html). Default is: Default.<br><b>NOTE:</b> Must match format names exactly found [here](http://commons.apache.org/proper/commons-csv/apidocs/org/apache/commons/csv/CSVFormat.Predefined.html#Default)",
                "isOptional": true,
                "label": "CSV delimiter",
                "name": "csvFormat",
                "paramType": "TEXT"
            },
            {
                "helpText": "Path to JSON schema, ex gs://path/to/schema. Default: null.",
                "isOptional": true,
                "label": "Path to JSON schema",
                "name": "jsonSchemaPath",
                "paramType": "TEXT"
            },
            {
                "helpText": "The full URL of your .js file. Example: gs://your-bucket/your-function.js",
                "isOptional": true,
                "label": "GCS location of your JavaScript UDF",
                "name": "javascriptTextTransformGcsPath",
                "paramType": "TEXT",
                "regexes": [
                    "^gs:\\/\\/[^\\n\\r]+$"
                ]
            },
            {
                "helpText": "The function name should only contain letters, digits and underscores. Example: 'transform' or 'transform_udf1'.",
                "isOptional": true,
                "label": "The name of the JavaScript function you wish to call as your UDF",
                "name": "javascriptTextTransformFunctionName",
                "paramType": "TEXT",
                "regexes": [
                    "[a-zA-Z0-9_]+"
                ]
            },
            {
                "helpText": "Google Cloud Storage path where to write objects that could not be converted to Splunk objects or pushed to Splunk. Ex: gs://your-bucket/your-path",
                "isOptional": false,
                "label": "Invalid events output path",
                "name": "invalidOutputPath",
                "paramType": "GCS_WRITE_FOLDER",
                "regexes": [
                    "^gs:\\/\\/[^\\n\\r]+$"
                ]
            },
            {
                "helpText": "Splunk Http Event Collector (HEC) authentication token.",
                "isOptional": false,
                "label": "HEC Authentication token.",
                "name": "token",
                "paramType": "TEXT"
            },
            {
                "helpText": "Splunk Http Event Collector (HEC) url. This should be routable from the VPC in which the pipeline runs. e.g. https://splunk-hec-host:8088",
                "isOptional": false,
                "label": "Splunk HEC URL.",
                "name": "url",
                "paramType": "TEXT"
            },
            {
                "helpText": "Batch size for sending multiple events to Splunk HEC. Default: 1 (no batching).",
                "isOptional": false,
                "label": "Batch size for sending multiple events to Splunk HEC.",
                "name": "batchCount",
                "paramType": "TEXT",
                "regexes": [
                    "[0-9]+"
                ]
            },
            {
                "helpText": "Maximum number of parallel requests. Default: 1 (no parallelism).",
                "isOptional": false,
                "label": "Maximum number of parallel requests.",
                "name": "parallelism",
                "paramType": "TEXT",
                "regexes": [
                    "[0-9]+"
                ]
            },
            {
                "helpText": "Disable SSL certificate validation (true/false). Default: false (validation enabled). If true, the certificates are not validated (all certificates are trusted) and  `rootCaCertificatePath` parameter is ignored.",
                "isOptional": false,
                "label": "Disable SSL certificate validation.",
                "name": "disableCertificateValidation",
                "paramType": "TEXT",
                "regexes": [
                    "^true|^false"
                ]
            }
        ]
    },
    "sdkInfo": {
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
* inputFileSpec: Pattern of where data lives, ex: gs://mybucket/somepath/*.csv
* containsHeaders: Set to true or false if CSV file contains headers.
* invalidOutputPath: Pattern of where to output failures, ex: gs://mybucket/errorpath
* token: Splunk Http Event Collector (HEC) authentication token.
* url: Splunk Http Event Collector (HEC) url. This should be routable from the VPC in which the pipeline runs. e.g. https://splunk-hec-host:8088
* batchCount: Batch size for sending multiple events to Splunk HEC. Default: 1 (no batching).
* parallelism: Maximum number of parallel requests. Default: 1 (no parallelism).
* disableCertificateValidation: Disable SSL certificate validation (true/false). Default: false (validation enabled). If true, the certificates are not validated (all certificates are trusted) and  `rootCaCertificatePath` parameter is ignored.

The template has the following optional parameters:
* delimiter: Delimiting character in CSV file(s). Default: use delimiter found in csvFormat
* csvFormat: Csv format according to [Apache Commons CSV format](https://commons.apache.org/proper/commons-csv/apidocs/org/apache/commons/csv/CSVFormat.html). Default is: Default
  <br><b>NOTE:</b> Must match format names exactly found [here](http://commons.apache.org/proper/commons-csv/apidocs/org/apache/commons/csv/CSVFormat.Predefined.html#Default)
* jsonSchemaPath: Path to JSON schema, ex gs://path/to/schema. Default: null.
* javascriptTextTransformGcsPath: GCS path to Javascript UDF source. UDF will be preferred option for transformation if supplied. Default: null
* javascriptTextTransformFunctionName: UDF Javascript Function Name. Default: null

Template can be executed using the following gcloud command:
```sh
export JOB_NAME="${TEMPLATE_MODULE}-`date +%Y%m%d-%H%M%S`"
gcloud beta dataflow flex-template run ${JOB_NAME} \
        --project=${PROJECT} --region=us-central1 \
        --template-file-gcs-location=${TEMPLATE_IMAGE_SPEC} \
        --parameters inputFileSpec=${INPUT_FILE_SPEC}  \
        --parameters containsHeaders=${CONTAINS_HEADERS} \
        --parameters invalidOutputPath=${INVALID_OUTPUT_PATH} \
        --parameters url=${SPLUNK_URL} \
        --parameters token=${SPLUNK_TOKEN} \
        --parameters batchCount=${BATCH_COUNT} \
        --parameters parallelism=${PARALLELISM} \
        --parameters disableCertificateValidation="true"
```
