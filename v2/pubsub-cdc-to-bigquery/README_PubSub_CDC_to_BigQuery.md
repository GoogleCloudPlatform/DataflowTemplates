Pub/Sub CDC to Bigquery Template
---
Streaming pipeline. Ingests JSON-encoded messages from a Pub/Sub topic, transforms them using a JavaScript user-defined function (UDF), and writes them to a pre-existing BigQuery table as BigQuery elements.

:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided-templates)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=PubSub_CDC_to_BigQuery).


:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required Parameters

* **inputSubscription** (Pub/Sub input subscription): Pub/Sub subscription to read the input from, in the format of 'projects/your-project-id/subscriptions/your-subscription-name' (Example: projects/your-project-id/subscriptions/your-subscription-name).
* **outputDatasetTemplate** (BigQuery Dataset Name or Template: dataset_name or {column_name}): The name for the dataset to contain the replica table. Defaults to: {_metadata_dataset}.
* **outputTableNameTemplate** (BigQuery Table Name or Template: table_name or {column_name}): The location of the BigQuery table to write the output to. If a table does not already exist one will be created automatically. Defaults to: _metadata_table.

### Optional Parameters

* **autoMapTables** (Auto Map Tables): Determines if new columns and tables should be automatically created in BigQuery. Defaults to: true.
* **schemaFilePath** (Cloud Storage file with BigQuery schema fields to be used in DDL): This is the file location that contains the table definition to be used when creating the table in BigQuery. If left blank the table will get created with generic string typing.
* **outputTableSpec** (BigQuery output table (Deprecated)): BigQuery table location to write the output to. The name should be in the format <project>:<dataset>.<table_name>. The table's schema must match input objects.
* **outputDeadletterTable** (The dead-letter table name to output failed messages to BigQuery): Messages failed to reach the output table for all kind of reasons (e.g., mismatched schema, malformed json) are written to this table. If it doesn't exist, it will be created during pipeline execution. If not specified, "outputTableSpec_error_records" is used instead. (Example: your-project-id:your-dataset.your-table-name).
* **deadLetterQueueDirectory** (Dead Letter Queue Directory): The name of the directory on Cloud Storage you want to write dead letters messages to. Defaults to empty.
* **windowDuration** (Window duration): The window duration/size in which DLQ data will be written to Cloud Storage. Allowed formats are: Ns (for seconds, example: 5s), Nm (for minutes, example: 12m), Nh (for hours, example: 2h). (Example: 5m). Defaults to: 5s.
* **threadCount** (Thread Number): The number of parallel threads you want to split your data into. Defaults to: 100.
* **javascriptTextTransformGcsPath** (Cloud Storage path to Javascript UDF source): The Cloud Storage path pattern for the JavaScript code containing your user-defined functions. (Example: gs://your-bucket/your-function.js).
* **javascriptTextTransformFunctionName** (UDF Javascript Function Name): The name of the function to call from your JavaScript file. Use only letters, digits, and underscores. (Example: 'transform' or 'transform_udf1').
* **pythonTextTransformGcsPath** (Gcs path to python UDF source): The Cloud Storage path pattern for the Python code containing your user-defined functions. (Example: gs://your-bucket/your-transforms/*.py).
* **pythonRuntimeVersion** (Python UDF Runtime Version): The runtime version to use for this Python UDF.
* **pythonTextTransformFunctionName** (UDF Python Function Name): The name of the function to call from your JavaScript file. Use only letters, digits, and underscores. (Example: transform_udf1).
* **runtimeRetries** (Python runtime retry attempts): The number of times a runtime will be retried before failing. Defaults to: 5.
* **useStorageWriteApi** (Use BigQuery Storage Write API): If enabled (set to true) the pipeline will use Storage Write API when writing the data to BigQuery (see https://cloud.google.com/blog/products/data-analytics/streaming-data-into-bigquery-using-storage-write-api). If this is enabled and at-least-once semantics (useStorageWriteApiAtLeastOnce) option is off then "Number of streams for BigQuery Storage Write API" and "Triggering frequency in seconds for BigQuery Storage Write API" must be provided. Defaults to: false.
* **useStorageWriteApiAtLeastOnce** (Use at at-least-once semantics in BigQuery Storage Write API): This parameter takes effect only if "Use BigQuery Storage Write API" is enabled. If enabled the at-least-once semantics will be used for Storage Write API, otherwise exactly-once semantics will be used. Defaults to: false.
* **numStorageWriteApiStreams** (Number of streams for BigQuery Storage Write API): Number of streams defines the parallelism of the BigQueryIO’s Write transform and roughly corresponds to the number of Storage Write API’s streams which will be used by the pipeline. See https://cloud.google.com/blog/products/data-analytics/streaming-data-into-bigquery-using-storage-write-api for the recommended values. Defaults to: 0.
* **storageWriteApiTriggeringFrequencySec** (Triggering frequency in seconds for BigQuery Storage Write API): Triggering frequency will determine how soon the data will be visible for querying in BigQuery. See https://cloud.google.com/blog/products/data-analytics/streaming-data-into-bigquery-using-storage-write-api for the recommended values.


## User-Defined functions (UDFs)

The Pub/Sub CDC to Bigquery Template supports User-Defined functions (UDFs).
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
[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=/v2/pubsub-cdc-to-bigquery/src/main/java/com/google/cloud/teleport/v2/templates/PubSubCdcToBigQuery.java)

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
-DtemplateName="PubSub_CDC_to_BigQuery" \
-pl v2/pubsub-cdc-to-bigquery \
-am
```

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/PubSub_CDC_to_BigQuery
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/PubSub_CDC_to_BigQuery"

### Required
export INPUT_SUBSCRIPTION=<inputSubscription>
export OUTPUT_DATASET_TEMPLATE="{_metadata_dataset}"
export OUTPUT_TABLE_NAME_TEMPLATE="_metadata_table"

### Optional
export AUTO_MAP_TABLES=true
export SCHEMA_FILE_PATH=<schemaFilePath>
export OUTPUT_TABLE_SPEC=<outputTableSpec>
export OUTPUT_DEADLETTER_TABLE=<outputDeadletterTable>
export DEAD_LETTER_QUEUE_DIRECTORY=""
export WINDOW_DURATION="5s"
export THREAD_COUNT=100
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>
export PYTHON_TEXT_TRANSFORM_GCS_PATH=<pythonTextTransformGcsPath>
export PYTHON_RUNTIME_VERSION=<pythonRuntimeVersion>
export PYTHON_TEXT_TRANSFORM_FUNCTION_NAME=<pythonTextTransformFunctionName>
export RUNTIME_RETRIES=5
export USE_STORAGE_WRITE_API=false
export USE_STORAGE_WRITE_API_AT_LEAST_ONCE=false
export NUM_STORAGE_WRITE_API_STREAMS=0
export STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC=<storageWriteApiTriggeringFrequencySec>

gcloud dataflow flex-template run "pubsub-cdc-to-bigquery-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "inputSubscription=$INPUT_SUBSCRIPTION" \
  --parameters "autoMapTables=$AUTO_MAP_TABLES" \
  --parameters "schemaFilePath=$SCHEMA_FILE_PATH" \
  --parameters "outputDatasetTemplate=$OUTPUT_DATASET_TEMPLATE" \
  --parameters "outputTableNameTemplate=$OUTPUT_TABLE_NAME_TEMPLATE" \
  --parameters "outputTableSpec=$OUTPUT_TABLE_SPEC" \
  --parameters "outputDeadletterTable=$OUTPUT_DEADLETTER_TABLE" \
  --parameters "deadLetterQueueDirectory=$DEAD_LETTER_QUEUE_DIRECTORY" \
  --parameters "windowDuration=$WINDOW_DURATION" \
  --parameters "threadCount=$THREAD_COUNT" \
  --parameters "javascriptTextTransformGcsPath=$JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH" \
  --parameters "javascriptTextTransformFunctionName=$JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME" \
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
export INPUT_SUBSCRIPTION=<inputSubscription>
export OUTPUT_DATASET_TEMPLATE="{_metadata_dataset}"
export OUTPUT_TABLE_NAME_TEMPLATE="_metadata_table"

### Optional
export AUTO_MAP_TABLES=true
export SCHEMA_FILE_PATH=<schemaFilePath>
export OUTPUT_TABLE_SPEC=<outputTableSpec>
export OUTPUT_DEADLETTER_TABLE=<outputDeadletterTable>
export DEAD_LETTER_QUEUE_DIRECTORY=""
export WINDOW_DURATION="5s"
export THREAD_COUNT=100
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>
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
-DjobName="pubsub-cdc-to-bigquery-job" \
-DtemplateName="PubSub_CDC_to_BigQuery" \
-Dparameters="inputSubscription=$INPUT_SUBSCRIPTION,autoMapTables=$AUTO_MAP_TABLES,schemaFilePath=$SCHEMA_FILE_PATH,outputDatasetTemplate=$OUTPUT_DATASET_TEMPLATE,outputTableNameTemplate=$OUTPUT_TABLE_NAME_TEMPLATE,outputTableSpec=$OUTPUT_TABLE_SPEC,outputDeadletterTable=$OUTPUT_DEADLETTER_TABLE,deadLetterQueueDirectory=$DEAD_LETTER_QUEUE_DIRECTORY,windowDuration=$WINDOW_DURATION,threadCount=$THREAD_COUNT,javascriptTextTransformGcsPath=$JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH,javascriptTextTransformFunctionName=$JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME,pythonTextTransformGcsPath=$PYTHON_TEXT_TRANSFORM_GCS_PATH,pythonRuntimeVersion=$PYTHON_RUNTIME_VERSION,pythonTextTransformFunctionName=$PYTHON_TEXT_TRANSFORM_FUNCTION_NAME,runtimeRetries=$RUNTIME_RETRIES,useStorageWriteApi=$USE_STORAGE_WRITE_API,useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE,numStorageWriteApiStreams=$NUM_STORAGE_WRITE_API_STREAMS,storageWriteApiTriggeringFrequencySec=$STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC" \
-pl v2/pubsub-cdc-to-bigquery \
-am
```
