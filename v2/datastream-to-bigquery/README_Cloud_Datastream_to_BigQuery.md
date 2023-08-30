
Datastream to BigQuery template
---
The Datastream to BigQuery template is a streaming pipeline that reads <a
href="https://cloud.google.com/datastream/docs">Datastream</a> data and
replicates it into BigQuery. The template reads data from Cloud Storage using
Pub/Sub notifications and replicates it into a time partitioned BigQuery staging
table. Following replication, the template executes a MERGE in BigQuery to upsert
all change data capture (CDC) changes into a replica of the source table.

The template handles creating and updating the BigQuery tables managed by the
replication. When data definition language (DDL) is required, a callback to
Datastream extracts the source table schema and translates it into BigQuery data
types. Supported operations include the following:
- New tables are created as data is inserted.
- New columns are added to BigQuery tables with null initial values.
- Dropped columns are ignored in BigQuery and future values are null.
- Renamed columns are added to BigQuery as new columns.
- Type changes are not propagated to BigQuery.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/datastream-to-bigquery)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Cloud_Datastream_to_BigQuery).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required Parameters

* **inputFilePattern** (File location for Datastream file output in Cloud Storage.): This is the file location for Datastream file output in Cloud Storage, in the format: gs://${BUCKET}/${ROOT_PATH}/.
* **inputFileFormat** (Datastream output file format (avro/json).): The format of the output files produced by Datastream. Value can be 'avro' or 'json'. Defaults to: avro.
* **gcsPubSubSubscription** (The Pub/Sub subscription on the Cloud Storage bucket.): The Pub/Sub subscription used by Cloud Storage to notify Dataflow of new files available for processing, in the format: projects/{PROJECT_NAME}/subscriptions/{SUBSCRIPTION_NAME}.
* **outputStagingDatasetTemplate** (Name or template for the dataset to contain staging tables.): This is the name for the dataset to contain staging tables. This parameter supports templates (e.g. {_metadata_dataset}_log or my_dataset_log). Normally, this parameter is a dataset name. Defaults to: {_metadata_dataset}.
* **outputDatasetTemplate** (Template for the dataset to contain replica tables.): This is the name for the dataset to contain replica tables. This parameter supports templates (e.g. {_metadata_dataset} or my_dataset). Normally, this parameter is a dataset name. Defaults to: {_metadata_dataset}.
* **deadLetterQueueDirectory** (Dead letter queue directory.): This is the file path for Dataflow to write the dead letter queue output. This path should not be in the same path as the Datastream file output. Defaults to empty.

### Optional Parameters

* **streamName** (Name or template for the stream to poll for schema information.): This is the name or template for the stream to poll for schema information. Default is {_metadata_stream}. The default value is enough under most conditions.
* **rfcStartDateTime** (The starting DateTime used to fetch from Cloud Storage (https://tools.ietf.org/html/rfc3339).): The starting DateTime used to fetch from Cloud Storage (https://tools.ietf.org/html/rfc3339). Defaults to: 1970-01-01T00:00:00.00Z.
* **fileReadConcurrency** (File read concurrency): The number of concurrent DataStream files to read. Default is 10.
* **outputProjectId** (Project Id for BigQuery datasets.): Project for BigQuery datasets to output data into. The default for this parameter is the project where the Dataflow pipeline is running.
* **outputStagingTableNameTemplate** (Template for the name of staging tables.): This is the template for the name of staging tables (e.g. {_metadata_table}). Default is {_metadata_table}_log.
* **outputTableNameTemplate** (Template for the name of replica tables.): This is the template for the name of replica tables (e.g. {_metadata_table}). Default is {_metadata_table}.
* **ignoreFields** (Fields to be ignored): Fields to ignore in BigQuery (comma separator). (Example: _metadata_stream,_metadata_schema). Defaults to: _metadata_stream,_metadata_schema,_metadata_table,_metadata_source,_metadata_tx_id,_metadata_dlq_reconsumed,_metadata_primary_keys,_metadata_error,_metadata_retry_count.
* **mergeFrequencyMinutes** (The number of minutes between merges for a given table): The number of minutes between merges for a given table. Defaults to: 5.
* **dlqRetryMinutes** (The number of minutes between DLQ Retries.): The number of minutes between DLQ Retries. Defaults to: 10.
* **dataStreamRootUrl** (Datastream API Root URL (only required for testing)): Datastream API Root URL. Defaults to: https://datastream.googleapis.com/.
* **applyMerge** (A switch to disable MERGE queries for the job.): A switch to disable MERGE queries for the job. Defaults to: true.
* **mergeConcurrency** (Concurrent queries for merge.): The number of concurrent BigQuery MERGE queries. Only effective when applyMerge is set to true. Default is 30.
* **partitionRetentionDays** (Partition retention days.): The number of days to use for partition retention when running BigQuery merges. Default is 1.
* **javascriptTextTransformGcsPath** (Cloud Storage path to Javascript UDF source): The Cloud Storage path pattern for the JavaScript code containing your user-defined functions. (Example: gs://your-bucket/your-function.js).
* **javascriptTextTransformFunctionName** (UDF Javascript Function Name): The name of the function to call from your JavaScript file. Use only letters, digits, and underscores. (Example: 'transform' or 'transform_udf1').
* **javascriptTextTransformReloadIntervalMinutes** (JavaScript UDF auto-reload interval (minutes)): Define the interval that workers may check for JavaScript UDF changes to reload the files. Defaults to: 60.
* **pythonTextTransformGcsPath** (Gcs path to python UDF source): The Cloud Storage path pattern for the Python code containing your user-defined functions. (Example: gs://your-bucket/your-transforms/*.py).
* **pythonRuntimeVersion** (Python UDF Runtime Version): The runtime version to use for this Python UDF.
* **pythonTextTransformFunctionName** (UDF Python Function Name): The name of the function to call from your JavaScript file. Use only letters, digits, and underscores. (Example: transform_udf1).
* **runtimeRetries** (Python runtime retry attempts): The number of times a runtime will be retried before failing. Defaults to: 5.
* **useStorageWriteApi** (Use BigQuery Storage Write API): If enabled (set to true) the pipeline will use Storage Write API when writing the data to BigQuery (see https://cloud.google.com/blog/products/data-analytics/streaming-data-into-bigquery-using-storage-write-api). If this is enabled and at-least-once semantics (useStorageWriteApiAtLeastOnce) option is off then "Number of streams for BigQuery Storage Write API" and "Triggering frequency in seconds for BigQuery Storage Write API" must be provided. Defaults to: false.
* **useStorageWriteApiAtLeastOnce** (Use at at-least-once semantics in BigQuery Storage Write API): This parameter takes effect only if "Use BigQuery Storage Write API" is enabled. If enabled the at-least-once semantics will be used for Storage Write API, otherwise exactly-once semantics will be used. Defaults to: false.
* **numStorageWriteApiStreams** (Number of streams for BigQuery Storage Write API): Number of streams defines the parallelism of the BigQueryIO’s Write transform and roughly corresponds to the number of Storage Write API’s streams which will be used by the pipeline. See https://cloud.google.com/blog/products/data-analytics/streaming-data-into-bigquery-using-storage-write-api for the recommended values. Defaults to: 0.
* **storageWriteApiTriggeringFrequencySec** (Triggering frequency in seconds for BigQuery Storage Write API): Triggering frequency will determine how soon the data will be visible for querying in BigQuery. See https://cloud.google.com/blog/products/data-analytics/streaming-data-into-bigquery-using-storage-write-api for the recommended values.


## User-Defined functions (UDFs)

The Datastream to BigQuery Template supports User-Defined functions (UDFs).
UDFs allow you to customize functionality by providing a JavaScript function
without having to maintain or build the entire template code.

Check [Create user-defined functions for Dataflow templates](https://cloud.google.com/dataflow/docs/guides/templates/create-template-udf)
and [Using UDFs](https://github.com/GoogleCloudPlatform/DataflowTemplates#using-udfs)
for more information about how to create and test those functions.


## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!
[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/datastream-to-bigquery/src/main/java/com/google/cloud/teleport/v2/templates/DataStreamToBigQuery.java)

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates#templates-plugin)
. Install the plugin with the following command before proceeding:

```shell
mvn clean install -pl plugins/templates-maven-plugin -am
```

### Building Template

This template is a Flex Template, meaning that the pipeline code will be
containerized and the container will be executed on Dataflow. Please
check [Use Flex Templates](https://cloud.google.com/dataflow/docs/guides/templates/using-flex-templates)
and [Configure Flex Templates](https://cloud.google.com/dataflow/docs/guides/templates/configuring-flex-templates)
for more information.

#### Staging the Template

If the plan is to just stage the template (i.e., make it available to use) by
the `gcloud` command or Dataflow "Create job from template" UI,
the `-PtemplatesStage` profile should be used:

```shell
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>

mvn clean package -PtemplatesStage  \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-DstagePrefix="templates" \
-DtemplateName="Cloud_Datastream_to_BigQuery" \
-pl v2/datastream-to-bigquery \
-am
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Cloud_Datastream_to_BigQuery
```

The specific path should be copied as it will be used in the following steps.

#### Running the Template

**Using the staged template**:

You can use the path above run the template (or share with others for execution).

To start a job with the template at any time using `gcloud`, you are going to
need valid resources for the required parameters.

Provided that, the following command line can be used:

```shell
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>
export REGION=us-central1
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Cloud_Datastream_to_BigQuery"

### Required
export INPUT_FILE_PATTERN=<inputFilePattern>
export INPUT_FILE_FORMAT=avro
export GCS_PUB_SUB_SUBSCRIPTION=<gcsPubSubSubscription>
export OUTPUT_STAGING_DATASET_TEMPLATE={_metadata_dataset}
export OUTPUT_DATASET_TEMPLATE={_metadata_dataset}
export DEAD_LETTER_QUEUE_DIRECTORY=""

### Optional
export STREAM_NAME=<streamName>
export RFC_START_DATE_TIME=1970-01-01T00:00:00.00Z
export FILE_READ_CONCURRENCY=10
export OUTPUT_PROJECT_ID=<outputProjectId>
export OUTPUT_STAGING_TABLE_NAME_TEMPLATE={_metadata_table}_log
export OUTPUT_TABLE_NAME_TEMPLATE={_metadata_table}
export IGNORE_FIELDS=_metadata_stream,_metadata_schema,_metadata_table,_metadata_source,_metadata_tx_id,_metadata_dlq_reconsumed,_metadata_primary_keys,_metadata_error,_metadata_retry_count
export MERGE_FREQUENCY_MINUTES=5
export DLQ_RETRY_MINUTES=10
export DATA_STREAM_ROOT_URL=https://datastream.googleapis.com/
export APPLY_MERGE=true
export MERGE_CONCURRENCY=30
export PARTITION_RETENTION_DAYS=1
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>
export JAVASCRIPT_TEXT_TRANSFORM_RELOAD_INTERVAL_MINUTES=60
export PYTHON_TEXT_TRANSFORM_GCS_PATH=<pythonTextTransformGcsPath>
export PYTHON_RUNTIME_VERSION=<pythonRuntimeVersion>
export PYTHON_TEXT_TRANSFORM_FUNCTION_NAME=<pythonTextTransformFunctionName>
export RUNTIME_RETRIES=5
export USE_STORAGE_WRITE_API=false
export USE_STORAGE_WRITE_API_AT_LEAST_ONCE=false
export NUM_STORAGE_WRITE_API_STREAMS=0
export STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC=<storageWriteApiTriggeringFrequencySec>

gcloud dataflow flex-template run "cloud-datastream-to-bigquery-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "inputFilePattern=$INPUT_FILE_PATTERN" \
  --parameters "inputFileFormat=$INPUT_FILE_FORMAT" \
  --parameters "gcsPubSubSubscription=$GCS_PUB_SUB_SUBSCRIPTION" \
  --parameters "streamName=$STREAM_NAME" \
  --parameters "rfcStartDateTime=$RFC_START_DATE_TIME" \
  --parameters "fileReadConcurrency=$FILE_READ_CONCURRENCY" \
  --parameters "outputProjectId=$OUTPUT_PROJECT_ID" \
  --parameters "outputStagingDatasetTemplate=$OUTPUT_STAGING_DATASET_TEMPLATE" \
  --parameters "outputStagingTableNameTemplate=$OUTPUT_STAGING_TABLE_NAME_TEMPLATE" \
  --parameters "outputDatasetTemplate=$OUTPUT_DATASET_TEMPLATE" \
  --parameters "outputTableNameTemplate=$OUTPUT_TABLE_NAME_TEMPLATE" \
  --parameters "ignoreFields=$IGNORE_FIELDS" \
  --parameters "mergeFrequencyMinutes=$MERGE_FREQUENCY_MINUTES" \
  --parameters "deadLetterQueueDirectory=$DEAD_LETTER_QUEUE_DIRECTORY" \
  --parameters "dlqRetryMinutes=$DLQ_RETRY_MINUTES" \
  --parameters "dataStreamRootUrl=$DATA_STREAM_ROOT_URL" \
  --parameters "applyMerge=$APPLY_MERGE" \
  --parameters "mergeConcurrency=$MERGE_CONCURRENCY" \
  --parameters "partitionRetentionDays=$PARTITION_RETENTION_DAYS" \
  --parameters "javascriptTextTransformGcsPath=$JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH" \
  --parameters "javascriptTextTransformFunctionName=$JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME" \
  --parameters "javascriptTextTransformReloadIntervalMinutes=$JAVASCRIPT_TEXT_TRANSFORM_RELOAD_INTERVAL_MINUTES" \
  --parameters "pythonTextTransformGcsPath=$PYTHON_TEXT_TRANSFORM_GCS_PATH" \
  --parameters "pythonRuntimeVersion=$PYTHON_RUNTIME_VERSION" \
  --parameters "pythonTextTransformFunctionName=$PYTHON_TEXT_TRANSFORM_FUNCTION_NAME" \
  --parameters "runtimeRetries=$RUNTIME_RETRIES" \
  --parameters "useStorageWriteApi=$USE_STORAGE_WRITE_API" \
  --parameters "useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE" \
  --parameters "numStorageWriteApiStreams=$NUM_STORAGE_WRITE_API_STREAMS" \
  --parameters "storageWriteApiTriggeringFrequencySec=$STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC"
```

For more information about the command, please check:
https://cloud.google.com/sdk/gcloud/reference/dataflow/flex-template/run


**Using the plugin**:

Instead of just generating the template in the folder, it is possible to stage
and run the template in a single command. This may be useful for testing when
changing the templates.

```shell
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>
export REGION=us-central1

### Required
export INPUT_FILE_PATTERN=<inputFilePattern>
export INPUT_FILE_FORMAT=avro
export GCS_PUB_SUB_SUBSCRIPTION=<gcsPubSubSubscription>
export OUTPUT_STAGING_DATASET_TEMPLATE={_metadata_dataset}
export OUTPUT_DATASET_TEMPLATE={_metadata_dataset}
export DEAD_LETTER_QUEUE_DIRECTORY=""

### Optional
export STREAM_NAME=<streamName>
export RFC_START_DATE_TIME=1970-01-01T00:00:00.00Z
export FILE_READ_CONCURRENCY=10
export OUTPUT_PROJECT_ID=<outputProjectId>
export OUTPUT_STAGING_TABLE_NAME_TEMPLATE={_metadata_table}_log
export OUTPUT_TABLE_NAME_TEMPLATE={_metadata_table}
export IGNORE_FIELDS=_metadata_stream,_metadata_schema,_metadata_table,_metadata_source,_metadata_tx_id,_metadata_dlq_reconsumed,_metadata_primary_keys,_metadata_error,_metadata_retry_count
export MERGE_FREQUENCY_MINUTES=5
export DLQ_RETRY_MINUTES=10
export DATA_STREAM_ROOT_URL=https://datastream.googleapis.com/
export APPLY_MERGE=true
export MERGE_CONCURRENCY=30
export PARTITION_RETENTION_DAYS=1
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>
export JAVASCRIPT_TEXT_TRANSFORM_RELOAD_INTERVAL_MINUTES=60
export PYTHON_TEXT_TRANSFORM_GCS_PATH=<pythonTextTransformGcsPath>
export PYTHON_RUNTIME_VERSION=<pythonRuntimeVersion>
export PYTHON_TEXT_TRANSFORM_FUNCTION_NAME=<pythonTextTransformFunctionName>
export RUNTIME_RETRIES=5
export USE_STORAGE_WRITE_API=false
export USE_STORAGE_WRITE_API_AT_LEAST_ONCE=false
export NUM_STORAGE_WRITE_API_STREAMS=0
export STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC=<storageWriteApiTriggeringFrequencySec>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="cloud-datastream-to-bigquery-job" \
-DtemplateName="Cloud_Datastream_to_BigQuery" \
-Dparameters="inputFilePattern=$INPUT_FILE_PATTERN,inputFileFormat=$INPUT_FILE_FORMAT,gcsPubSubSubscription=$GCS_PUB_SUB_SUBSCRIPTION,streamName=$STREAM_NAME,rfcStartDateTime=$RFC_START_DATE_TIME,fileReadConcurrency=$FILE_READ_CONCURRENCY,outputProjectId=$OUTPUT_PROJECT_ID,outputStagingDatasetTemplate=$OUTPUT_STAGING_DATASET_TEMPLATE,outputStagingTableNameTemplate=$OUTPUT_STAGING_TABLE_NAME_TEMPLATE,outputDatasetTemplate=$OUTPUT_DATASET_TEMPLATE,outputTableNameTemplate=$OUTPUT_TABLE_NAME_TEMPLATE,ignoreFields=$IGNORE_FIELDS,mergeFrequencyMinutes=$MERGE_FREQUENCY_MINUTES,deadLetterQueueDirectory=$DEAD_LETTER_QUEUE_DIRECTORY,dlqRetryMinutes=$DLQ_RETRY_MINUTES,dataStreamRootUrl=$DATA_STREAM_ROOT_URL,applyMerge=$APPLY_MERGE,mergeConcurrency=$MERGE_CONCURRENCY,partitionRetentionDays=$PARTITION_RETENTION_DAYS,javascriptTextTransformGcsPath=$JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH,javascriptTextTransformFunctionName=$JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME,javascriptTextTransformReloadIntervalMinutes=$JAVASCRIPT_TEXT_TRANSFORM_RELOAD_INTERVAL_MINUTES,pythonTextTransformGcsPath=$PYTHON_TEXT_TRANSFORM_GCS_PATH,pythonRuntimeVersion=$PYTHON_RUNTIME_VERSION,pythonTextTransformFunctionName=$PYTHON_TEXT_TRANSFORM_FUNCTION_NAME,runtimeRetries=$RUNTIME_RETRIES,useStorageWriteApi=$USE_STORAGE_WRITE_API,useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE,numStorageWriteApiStreams=$NUM_STORAGE_WRITE_API_STREAMS,storageWriteApiTriggeringFrequencySec=$STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC" \
-pl v2/datastream-to-bigquery \
-am
```
