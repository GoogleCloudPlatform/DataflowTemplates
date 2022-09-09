# Spanner Change Streams To Google Cloud Storage Dataflow Template

The [SpannerChangeStreamsToGCS]
(src/main/java/com/google/cloud/teleport/v2/templates/SpannerChangeStreamsToGCS.java)
pipeline reads messages from Cloud Spanner Change Streams and stores them in a
Google Cloud Storage bucket using the specified file format.

Data will be bucketed by timestamp into different windows. By default, the
window size is 5 minutes.

The data can be stored in a Text or Avro File Format.

NOTE: This template is currently unreleased. If you wish to use it now, you
will need to follow the steps outlined below to add it to and run it from
your own Google Cloud project. Make sure to be in the /v2 directory.

## Getting started

### Requirements
* Java 8
* Maven
* Spanner Instance exists
* Spanner Database exists
* Spanner Metadata Instance exists
* Spanner Metadata Database exists
* Spanner change stream exists
* Google Cloud Storage output bucket exists.

### Building Template
This is a Flex Template meaning that the pipeline code will be containerized and the container will be
run on Dataflow.

##### Building Container Image

* Set environment variables that will be used in the build process.

```sh
export PROJECT=project
export IMAGE_NAME=googlecloud-to-googlecloud
export BUCKET_NAME=gs://bucket
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/images/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java8-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export TEMPLATE_MODULE=googlecloud-to-googlecloud
export COMMAND_MODULE=spanner-changestreams-to-gcs
export APP_ROOT=/template/${COMMAND_MODULE}
export COMMAND_SPEC=${APP_ROOT}/resources/${COMMAND_MODULE}-command-spec.json
export TEMPLATE_IMAGE_SPEC=${BUCKET_NAME}/images/${COMMAND_MODULE}-image-spec.json
```
* Build and push image to Google Container Repository from the v2 directory

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

```sh
echo '{
    "image": "'${TARGET_GCR_IMAGE}'",
    "metadata":{"name":"Spanner change streams to GCS",
    "description":"Streaming pipeline. Streams change stream records and writes them into a Google Cloud Storage bucket. Note the created pipeline will run on Dataflow Runner V2",
    "parameters":[
        {
            "label": "Spanner Project ID",
            "help_text": "Project to read change streams from. The default for this parameter is the project where the Dataflow pipeline is running.",
            "name": "spannerProjectId",
            "is_optional": true,
            "param_type": "TEXT"
        },
        {
            "label": "Spanner instance ID",
            "help_text": "The Spanner instance to read change streams from.",
            "name": "spannerInstanceId",
            "param_type": "TEXT"
        },
        {
            "label": "Spanner database ID",
            "help_text": "The Spanner database to read change streams from.",
            "name": "spannerDatabaseId",
            "param_type": "TEXT"
        },
        {
            "label": "Spanner metadata instance ID",
            "help_text": "The Spanner instance to use for the change stream metadata table.",
            "name": "spannerMetadataInstanceId",
            "param_type": "TEXT"
        },
        {
            "label": "Spanner metadata database ID",
            "help_text": "The Spanner database to use for the change stream metadata table.",
            "name": "spannerMetadataDatabaseId",
            "param_type": "TEXT"
        },
        {
            "label": "Spanner change stream",
            "help_text": "The Spanner change stream to read from.",
            "name": "spannerChangeStreamName",
            "param_type": "TEXT"
        },
        {
            "label": "Pipeline start time",
            "help_text": "The starting DateTime to use for reading change streams (https://tools.ietf.org/html/rfc3339). Defaults to now.",
            "name": "startTimestamp",
            "is_optional": true,
            "param_type": "TEXT"
        },
        {
            "label": "Pipeline end time",
            "help_text": "The ending DateTime to use for reading change streams (https://tools.ietf.org/html/rfc3339). Defaults to max, which represents an infinite time in the future.",
            "name": "endTimestamp",
            "is_optional": true,
            "param_type": "TEXT"
        },
        {
            "label": "Output file format",
            "help_text": "The format of the output GCS file. Allowed formats are TEXT, AVRO. Default is AVRO.",
            "name": "outputFileFormat",
            "is_optional": true,
            "param_type": "TEXT"
        },
        {
            "label": "Window duration",
            "help_text": "The window duration in which data will be written. Defaults to 5m. Allowed formats are: <int>s (for seconds, example: 5s), <int>m (for minutes, example: 12m), <int>h (for hours, example: 2h).",
            "name": "windowDuration",
            "is_optional": true,
            "param_type": "TEXT"
        },
        {
            "label": "The RPC priority.",
            "help_text": "Priority for Spanner RPC invocations. Defaults to HIGH. Allowed priorities are LOW, MEDIUM,HIGH. Defaults to HIGH",
            "name": "rpcPriority",
            "is_optional": true,
            "param_type": "TEXT"
        },
        {
            "label": "File location for change stream output in Cloud Storage",
            "help_text": "This is the file location for change stream  output in Cloud Storage, in the format: gs://${BUCKET}/${ROOT_PATH}/.",
            "name": "outputDirectory",
            "param_type": "TEXT"
        },
        {
            "label": "The filename prefix of the files to write to",
            "help_text": "The filename prefix of the files to write to. Default file prefix is set to \"output\"",
            "name": "outputFilenamePrefix",
            "param_type": "TEXT",
            "is_optional": true
        },
        {
            "label": "Maximum output shards",
            "help_text": "The maximum number of output shards produced when writing. Default number is runner defined",
            "name": "numShards",
            "param_type": "TEXT",
            "is_optional": true
        }
        ]},
    "sdk_info": {
            "language": "JAVA"
    }
}'> image-spec.json
gsutil cp image-spec.json ${TEMPLATE_IMAGE_SPEC}
```


### Testing Template

The template unit tests can be run using:
```sh
mvn test
```

### Executing Template

The template requires the following parameters:
* spannerInstanceId: The Spanner Instance ID
* spannerDatabaseId: The Spanner database ID
* spannerMetadataInstanceId: The Spanner Metadata Instance ID.
* spannerMetadataDatabaseId: The Spanner Metadata Database ID.
* spannerChangeStream: The Spanner change stream.
* outputDirectory: The GCS Output Directory.

The template has the following optional parameters:
* startTimestamp: The starting DateTime to use for reading change streams. Defaults to now.
* endTimestamp: The ending DateTime to use for reading change streams. Defaults to infinite.
* outputFileFormat: The format of the output GCS file. Defaults to AVRO. Can be either TEXT or AVRO.
* windowDuration: The window duration in which data will be written. Defaults to 5m.
* rpcPriority: The priority for Spanner RPC priority. Defaults to HIGH.
* outputFileNamePrefix: The output filename prefix. Defaults to "output"
* numShards: the maximum number of output shards produced when writing. Default is 1.

Template can be executed using the following gcloud command:

```sh
export JOB_NAME="${TEMPLATE_MODULE}-`date +%Y%m%d-%H%M%S-%N`"

export SPANNER_INSTANCE=spanner-instance
export SPANNER_DATABASE=spanner-database
export SPANNER_METADATA_INSTANCE=spanner-metadata-instance
export SPANNER_METADATA_DATABASE=spanner-metadata-database
export SPANNER_CHANGE_STREAM=spanner-changestream
export OUTPUT_DIRECTORY=${BUCKET_NAME}/output-directory/
export OUTPUT_FILE_FORMAT=TEXT
gcloud beta dataflow flex-template run ${JOB_NAME} \
        --project=${PROJECT} --region=us-central1 \
        --template-file-gcs-location=${TEMPLATE_IMAGE_SPEC} \
        --parameters ^~^spannerInstanceId=${SPANNER_INSTANCE}~spannerDatabaseId=${SPANNER_DATABASE}~spannerMetadataInstanceId=${SPANNER_METADATA_INSTANCE}~spannerMetadataDatabaseId=${SPANNER_METADATA_DATABASE}~spannerChangeStreamName=${SPANNER_CHANGE_STREAM}~outputDirectory=${OUTPUT_DIRECTORY}~outputFileFormat=${OUTPUT_FILE_FORMAT}

```

OPTIONAL Dataflow Params:

--numWorkers=2
--maxNumWorkers=10
--workerMachineType=n1-highcpu-4
