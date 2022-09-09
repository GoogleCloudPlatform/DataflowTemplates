# DataStream to BigQuery Dataflow Template

The [DataStreamToBigQuery](src/main/java/com/google/cloud/teleport/v2/templates/DataStreamToBigQuery.java) pipeline
ingests data supplied by DataStream, optionally applies a Javascript or Python UDF if supplied
and writes the data to BigQuery staging tables.  A Merge is run against BigQuery to load data into
the final table.

## Getting Started

### Requirements
* Java 8
* Maven
* DataStream stream is created and sending data to storage
* PubSub Subscription exists or GCS Bucket contains data

### Building Template
This is a Flex Template meaning that the pipeline code will be containerized and the container will be
used to launch the Dataflow pipeline.

#### Building Container Image
* Set environment variables.

```sh
export PROJECT=<my-project>
export IMAGE_NAME=datastream-to-bigquery
export BUCKET_NAME=gs://<bucket-name>
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java8-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export APP_ROOT=/template/${IMAGE_NAME}
export DATAFLOW_JAVA_COMMAND_SPEC=${APP_ROOT}/resources/${IMAGE_NAME}-command-spec.json
export TEMPLATE_IMAGE_SPEC=${BUCKET_NAME}/images/${IMAGE_NAME}-image-spec.json

export TOPIC=projects/${PROJECT}/topics/<topic-name>
export SUBSCRIPTION=projects/${PROJECT}/subscriptions/<subscription-name>
export DEADLETTER_TABLE=${PROJECT}:${DATASET_TEMPLATE}.dead_letter

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
    "metadata":{"name":"Datastream to BigQuery",
    "description":"Streaming pipeline. Ingests messages from a stream in Datastream, transforms them, and writes them to a pre-existing BigQuery dataset as a set of tables.",
    "parameters":[
        {
            "label": "File location for Datastream file output in Cloud Storage.",
            "help_text": "This is the file location for Datastream file output in Cloud Storage, in the format: gs://${BUCKET}/${ROOT_PATH}/.",
            "name": "inputFilePattern",
            "param_type": "GCS_READ_FILE"
        },
        {
            "label": "The Pub/Sub subscription on the Cloud Storage bucket.",
            "help_text": "The Pub/Sub subscription used by Cloud Storage to notify Dataflow of new files available for processing, in the format: projects/{PROJECT_NAME}/subscriptions/{SUBSCRIPTION_NAME}",
            "name": "gcsPubSubSubscription",
            "is_optional": false,
            regexes: "^projects\\/[^\\n\\r\\/]+\\/subscriptions\\/[^\\n\\r\\/]+$|^$",
            "param_type": "PUBSUB_SUBSCRIPTION"
        },
        {
            "label": "Datastream output file format (avro/json).",
            "help_text": "The format of the output files produced by Datastream. Value can be 'avro' or 'json'.",
            "name": "inputFileFormat",
            "param_type": "TEXT",
            "is_optional": false
        },
        {
            "name": "rfcStartDateTime",
            "label": "The starting DateTime used to fetch from GCS (https://tools.ietf.org/html/rfc3339).",
            "help_text": "The starting DateTime used to fetch from GCS (https://tools.ietf.org/html/rfc3339).",
            "param_type": "TEXT",
            "is_optional": true
        },
        {
            "name": "javascriptTextTransformGcsPath",
            "label": "GCS location of your JavaScript UDF",
            "help_text": "The full URL of your .js file. Example: gs://your-bucket/your-function.js",
            regexes: "^gs:\\/\\/[^\\n\\r]+$",
            "param_type": "GCS_READ_FILE",
            "is_optional": true
        },
        {
            "name": "javascriptTextTransformFunctionName",
            "label": "The name of the JavaScript function you wish to call as your UDF",
            "help_text": "The function name should only contain letters, digits and underscores. Example: 'transform' or 'transform_udf1'.",
            regexes: "[a-zA-Z0-9_]+",
            "param_type": "TEXT",
            "is_optional": true
        },
        {
            "name": "outputStagingDatasetTemplate",
            "label": "Name or template for the dataset to contain staging tables.",
            "help_text": "This is the name for the dataset to contain staging tables. This parameter supports templates (e.g. {_metadata_dataset}_log or my_dataset_log). Normally, this parameter is a dataset name.",
            "param_type": "TEXT"
        },
        {
            "name": "outputDatasetTemplate",
            "label": "Template for the dataset to contain replica tables.",
            "help_text": "This is the name for the dataset to contain replica tables. This parameter supports templates (e.g. {_metadata_dataset} or my_dataset). Normally, this parameter is a dataset name.",
            "param_type": "TEXT"
        },
        {
            "label": "Project name for BigQuery datasets.",
            "help_text": "Project for BigQuery datasets to output data into. The default for this parameter is the project where the Dataflow pipeline is running.",
            "name": "outputProjectId",
            "is_optional": true,
            "param_type": "TEXT"
        },
        {
            "label": "Template for the name of staging tables.",
            "name": "outputStagingTableNameTemplate",
            "help_text": "This is the template for the name of staging tables (e.g. {_metadata_table}). Default is {_metadata_table}.",
            "is_optional": true,
            "param_type": "TEXT"
        },
        {
            "label": "Template for the name of replica tables.",
            "name": "outputTableNameTemplate",
            "help_text": "This is the template for the name of replica tables (e.g. {_metadata_table}). Default is {_metadata_table}.",
            "is_optional": true,
            "param_type": "TEXT"
        },
        {
            "label": "Dead letter queue directory.",
            "name": "deadLetterQueueDirectory",
            "help_text": "This is the file path for Dataflow to write the dead letter queue output. This path should not be in the same path as the Datastream file output.",
            "is_optional": false,
            "param_type": "TEXT"
        },
        {
            "label": "Name or template for the stream to poll for schema information.",
            "name": "streamName",
            "help_text": "This is the name or template for the stream to poll for schema information. Default is {_metadata_stream}. The default value is enough under most conditions.",
            "is_optional": true,
            "param_type": "TEXT"
        },
        {
            "name":"dataStreamRootUrl",
            "label":"Datastream API URL (only required for testing)",
            "help_text": "Datastream API URL",
            "param_type": "TEXT",
            "is_optional": true
        },
        {
            "name": "mergeFrequencyMinutes",
            "label": "The number of minutes between merges for a given table.",
            "help_text": "The number of minutes between merges for a given table.",
            "param_type": "TEXT",
            "is_optional": true
        },
        {
            "name": "dlqRetryMinutes",
            "label": "The number of minutes between DLQ Retries.",
            "help_text": "The number of minutes between DLQ Retries.",
            "param_type": "TEXT",
            "is_optional": true
        },
        {
            "name": "applyMerge",
            "label": "A switch to disable MERGE queries for the job.",
            "help_text": "A switch to disable MERGE queries for the job.",
            "param_type": "TEXT",
            "is_optional": true
        },
        {
            "name":"autoscalingAlgorithm",
            "label":"Autoscaling algorithm to use",
            "help_text": "Autoscaling algorithm to use: THROUGHPUT_BASED",
            "param_type": "TEXT",
            "is_optional": true
        },
        {
            "name":"numWorkers",
            "label":"Number of workers Dataflow will start with",
            "help_text": "Number of workers Dataflow will start with",
            "param_type": "TEXT",
            "is_optional": true
        },
        {
            "name":"maxNumWorkers",
            "label":"Maximum number of workers Dataflow job will use",
            "help_text": "Maximum number of workers Dataflow job will use",
            "param_type": "TEXT",
            "is_optional": true
        },
        {
            "name":"numberOfWorkerHarnessThreads",
            "label":"Dataflow job will use max number of threads per worker",
            "help_text": "Maximum number of threads per worker Dataflow job will use",
            "param_type": "TEXT",
            "is_optional": true
        },
        {
            "name":"dumpHeapOnOOM",
            "label":"Dataflow will dump heap on an OOM error",
            "help_text": "Dataflow will dump heap on an OOM error",
            "param_type": "TEXT",
            "is_optional": true
        },
        {
            "name":"saveHeapDumpsToGcsPath",
            "label":"Dataflow will dump heap on an OOM error to supplied GCS path",
            "help_text": "Dataflow will dump heap on an OOM error to supplied GCS path",
            "param_type": "TEXT",
            "is_optional": true
        },
        {
            "name":"workerMachineType",
            "label":"Worker Machine Type to use in Dataflow Job",
            "help_text": "Machine Type to Use: n1-standard-4",
            "param_type": "TEXT",
            "is_optional": true
        },
        {
            "name":"maxStreamingRowsToBatch",
            "label":"Max number of rows per BigQueryIO batch",
            "help_text": "Max number of rows per BigQueryIO batch",
            "param_type": "TEXT",
            "is_optional": true
        },
        {
            "name":"maxStreamingBatchSize",
            "label":"Maximum byte size of a single streaming insert to BigQuery.",
            "help_text":"Sets the maximum byte size of a single streaming insert to BigQuery. This option could fix 'row too large' errors.",
            "param_type":"TEXT",
            "regexes":[
                "^[1-9][0-9]+$",
            ],
            "is_optional":true
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
* inputFilePattern: This is the file location for Datastream file output in Cloud Storage, in the format: gs://${BUCKET}/${ROOT_PATH}/.
* gcsPubSubSubscription: The Pub/Sub subscription used by Cloud Storage to notify Dataflow of new files available for processing, in the format: projects/{PROJECT_NAME}/subscriptions/{SUBSCRIPTION_NAME}
* inputFileFormat: The format of the output files produced by Datastream. Value can be 'avro' or 'json'.
* outputStagingDatasetTemplate: This is the name for the dataset to contain staging tables. This parameter supports templates (e.g. {_metadata_dataset}_log or my_dataset_log). Normally, this parameter is a dataset name.
* outputDatasetTemplate: This is the name for the dataset to contain replica tables. This parameter supports templates (e.g. {_metadata_dataset} or my_dataset). Normally, this parameter is a dataset name.
* deadLetterQueueDirectory: This is the file path for Dataflow to write the dead letter queue output. This path should not be in the same path as the Datastream file output.

The template has the following optional parameters:
* rfcStartDateTime: The starting DateTime used to fetch from GCS (https://tools.ietf.org/html/rfc3339).
* javascriptTextTransformGcsPath: The full URL of your .js file. Example: gs://your-bucket/your-function.js
* javascriptTextTransformFunctionName: The function name should only contain letters, digits and underscores. Example: 'transform' or 'transform_udf1'.
* outputProjectId: Project for BigQuery datasets to output data into. The default for this parameter is the project where the Dataflow pipeline is running.
* outputStagingTableNameTemplate: This is the template for the name of staging tables (e.g. {_metadata_table}). Default is {_metadata_table}.
* outputTableNameTemplate: This is the template for the name of replica tables (e.g. {_metadata_table}). Default is {_metadata_table}.
* streamName: This is the name or template for the stream to poll for schema information. Default is {_metadata_stream}. The default value is enough under most conditions.
* dataStreamRootUrl: Datastream API URL (only required for testing)
* mergeFrequencyMinutes: The number of minutes between merges for a given table.
* dlqRetryMinutes: The number of minutes between DLQ Retries.
* applyMerge: A switch to disable MERGE queries for the job.
* autoscalingAlgorithm: Autoscaling algorithm to use: THROUGHPUT_BASED
* numWorkers: Number of workers Dataflow will start with
* maxNumWorkers: Maximum number of workers Dataflow job will use
* numberOfWorkerHarnessThreads: Maximum number of threads per worker Dataflow job will use
* dumpHeapOnOOM: Dataflow will dump heap on an OOM error
* saveHeapDumpsToGcsPath: Dataflow will dump heap on an OOM error to supplied GCS path
* workerMachineType: Machine Type to Use: n1-standard-4
* maxStreamingRowsToBatch: Max number of rows per BigQueryIO batch

Template can be executed using the following API call:
```sh
export JOB_NAME="${IMAGE_NAME}-`date +%Y%m%d-%H%M%S-%N`"
gcloud beta dataflow flex-template run ${JOB_NAME} \
        --project=${PROJECT} --region=us-central1 \
        --template-file-gcs-location=${TEMPLATE_IMAGE_SPEC} \
        --parameters=inputFilePattern=${FILE_PATTERN} \
        --parameters=gcsPubSubSubscription=${GCS_PUBSUB_SUBSCRIPTION} \
        --parameters=inputFileFormat=${INPUT_FILE_FORMAT} \
        --parameters=outputStagingDatasetTemplate=${OUTPUT_STAGING_DATASET_TEMPLATE} \
        --parameters=outputDatasetTemplate=${OUTPUT_DATASET_TEMPLATE} \
        --parameters=deadLetterQueueDirectory=${DLQ_DIR}
```
