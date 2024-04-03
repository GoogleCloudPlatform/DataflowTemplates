
Pub/Sub CDC to Bigquery template
---
Streaming pipeline. Ingests JSON-encoded messages from a Pub/Sub topic,
transforms them using a JavaScript user-defined function (UDF), and writes them
to a pre-existing BigQuery table as BigQuery elements.



:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **inputSubscription** : Pub/Sub subscription to read the input from, in the format of 'projects/your-project-id/subscriptions/your-subscription-name' (Example: projects/your-project-id/subscriptions/your-subscription-name).
* **outputDatasetTemplate** : The name for the dataset to contain the replica table. Defaults to: {_metadata_dataset}.
* **outputTableNameTemplate** : The location of the BigQuery table to write the output to. If a table does not already exist one will be created automatically. Defaults to: _metadata_table.

### Optional parameters

* **autoMapTables** : Determines if new columns and tables should be automatically created in BigQuery. Defaults to: true.
* **schemaFilePath** : This is the file location that contains the table definition to be used when creating the table in BigQuery. If left blank the table will get created with generic string typing.
* **outputTableSpec** : BigQuery table location to write the output to. The name should be in the format `<project>:<dataset>.<table_name>`. The table's schema must match input objects.
* **outputDeadletterTable** : BigQuery table for failed messages. Messages failed to reach the output table for different reasons (e.g., mismatched schema, malformed json) are written to this table. If it doesn't exist, it will be created during pipeline execution. If not specified, "outputTableSpec_error_records" is used instead. (Example: your-project-id:your-dataset.your-table-name).
* **deadLetterQueueDirectory** : The name of the directory on Cloud Storage you want to write dead letters messages to. Defaults to empty.
* **windowDuration** : The window duration/size in which DLQ data will be written to Cloud Storage. Allowed formats are: Ns (for seconds, example: 5s), Nm (for minutes, example: 12m), Nh (for hours, example: 2h). (Example: 5m). Defaults to: 5s.
* **threadCount** : The number of parallel threads you want to split your data into. Defaults to: 100.
* **javascriptTextTransformGcsPath** : The Cloud Storage path pattern for the JavaScript code containing your user-defined functions. (Example: gs://your-bucket/your-function.js).
* **javascriptTextTransformFunctionName** : The name of the function to call from your JavaScript file. Use only letters, digits, and underscores. (Example: 'transform' or 'transform_udf1').
* **javascriptTextTransformReloadIntervalMinutes** : Define the interval that workers may check for JavaScript UDF changes to reload the files. Defaults to: 0.
* **pythonTextTransformGcsPath** : The Cloud Storage path pattern for the Python code containing your user-defined functions. (Example: gs://your-bucket/your-transforms/*.py).
* **pythonRuntimeVersion** : The runtime version to use for this Python UDF.
* **pythonTextTransformFunctionName** : The name of the function to call from your JavaScript file. Use only letters, digits, and underscores. (Example: transform_udf1).
* **runtimeRetries** : The number of times a runtime will be retried before failing. Defaults to: 5.
* **useStorageWriteApi** : If true, the pipeline uses the Storage Write API when writing the data to BigQuery (see https://cloud.google.com/blog/products/data-analytics/streaming-data-into-bigquery-using-storage-write-api). The default value is false. When using Storage Write API in exactly-once mode, you must set the following parameters: "Number of streams for BigQuery Storage Write API" and "Triggering frequency in seconds for BigQuery Storage Write API". If you enable Dataflow at-least-once mode or set the useStorageWriteApiAtLeastOnce parameter to true, then you don't need to set the number of streams or the triggering frequency.
* **useStorageWriteApiAtLeastOnce** : This parameter takes effect only if "Use BigQuery Storage Write API" is enabled. If enabled the at-least-once semantics will be used for Storage Write API, otherwise exactly-once semantics will be used. Defaults to: false.
* **numStorageWriteApiStreams** : Number of streams defines the parallelism of the BigQueryIO’s Write transform and roughly corresponds to the number of Storage Write API’s streams which will be used by the pipeline. See https://cloud.google.com/blog/products/data-analytics/streaming-data-into-bigquery-using-storage-write-api for the recommended values. Defaults to: 0.
* **storageWriteApiTriggeringFrequencySec** : Triggering frequency will determine how soon the data will be visible for querying in BigQuery. See https://cloud.google.com/blog/products/data-analytics/streaming-data-into-bigquery-using-storage-write-api for the recommended values.


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

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/pubsub-cdc-to-bigquery/src/main/java/com/google/cloud/teleport/v2/templates/PubSubCdcToBigQuery.java)

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates#templates-plugin).

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
-f v2/pubsub-cdc-to-bigquery
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
export OUTPUT_DATASET_TEMPLATE={_metadata_dataset}
export OUTPUT_TABLE_NAME_TEMPLATE=_metadata_table

### Optional
export AUTO_MAP_TABLES=true
export SCHEMA_FILE_PATH=<schemaFilePath>
export OUTPUT_TABLE_SPEC=<outputTableSpec>
export OUTPUT_DEADLETTER_TABLE=<outputDeadletterTable>
export DEAD_LETTER_QUEUE_DIRECTORY=""
export WINDOW_DURATION=5s
export THREAD_COUNT=100
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>
export JAVASCRIPT_TEXT_TRANSFORM_RELOAD_INTERVAL_MINUTES=0
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
export INPUT_SUBSCRIPTION=<inputSubscription>
export OUTPUT_DATASET_TEMPLATE={_metadata_dataset}
export OUTPUT_TABLE_NAME_TEMPLATE=_metadata_table

### Optional
export AUTO_MAP_TABLES=true
export SCHEMA_FILE_PATH=<schemaFilePath>
export OUTPUT_TABLE_SPEC=<outputTableSpec>
export OUTPUT_DEADLETTER_TABLE=<outputDeadletterTable>
export DEAD_LETTER_QUEUE_DIRECTORY=""
export WINDOW_DURATION=5s
export THREAD_COUNT=100
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>
export JAVASCRIPT_TEXT_TRANSFORM_RELOAD_INTERVAL_MINUTES=0
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
-Dparameters="inputSubscription=$INPUT_SUBSCRIPTION,autoMapTables=$AUTO_MAP_TABLES,schemaFilePath=$SCHEMA_FILE_PATH,outputDatasetTemplate=$OUTPUT_DATASET_TEMPLATE,outputTableNameTemplate=$OUTPUT_TABLE_NAME_TEMPLATE,outputTableSpec=$OUTPUT_TABLE_SPEC,outputDeadletterTable=$OUTPUT_DEADLETTER_TABLE,deadLetterQueueDirectory=$DEAD_LETTER_QUEUE_DIRECTORY,windowDuration=$WINDOW_DURATION,threadCount=$THREAD_COUNT,javascriptTextTransformGcsPath=$JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH,javascriptTextTransformFunctionName=$JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME,javascriptTextTransformReloadIntervalMinutes=$JAVASCRIPT_TEXT_TRANSFORM_RELOAD_INTERVAL_MINUTES,pythonTextTransformGcsPath=$PYTHON_TEXT_TRANSFORM_GCS_PATH,pythonRuntimeVersion=$PYTHON_RUNTIME_VERSION,pythonTextTransformFunctionName=$PYTHON_TEXT_TRANSFORM_FUNCTION_NAME,runtimeRetries=$RUNTIME_RETRIES,useStorageWriteApi=$USE_STORAGE_WRITE_API,useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE,numStorageWriteApiStreams=$NUM_STORAGE_WRITE_API_STREAMS,storageWriteApiTriggeringFrequencySec=$STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC" \
-f v2/pubsub-cdc-to-bigquery
```

## Terraform

Dataflow supports the utilization of Terraform to manage template jobs,
see [dataflow_flex_template_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_flex_template_job).

Terraform modules have been generated for most templates in this repository. This includes the relevant parameters
specific to the template. If available, they may be used instead of
[dataflow_flex_template_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_flex_template_job)
directly.

To use the autogenerated module, execute the standard
[terraform workflow](https://developer.hashicorp.com/terraform/intro/core-workflow):

```shell
cd v2/pubsub-cdc-to-bigquery/terraform/PubSub_CDC_to_BigQuery
terraform init
terraform apply
```

To use
[dataflow_flex_template_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_flex_template_job)
directly:

```terraform
provider "google-beta" {
  project = var.project
}
variable "project" {
  default = "<my-project>"
}
variable "region" {
  default = "us-central1"
}

resource "google_dataflow_flex_template_job" "pubsub_cdc_to_bigquery" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/PubSub_CDC_to_BigQuery"
  name              = "pubsub-cdc-to-bigquery"
  region            = var.region
  parameters        = {
    inputSubscription = "projects/your-project-id/subscriptions/your-subscription-name"
    outputDatasetTemplate = "{_metadata_dataset}"
    outputTableNameTemplate = "_metadata_table"
    # autoMapTables = "true"
    # schemaFilePath = "<schemaFilePath>"
    # outputTableSpec = "<outputTableSpec>"
    # outputDeadletterTable = "your-project-id:your-dataset.your-table-name"
    # deadLetterQueueDirectory = ""
    # windowDuration = "5m"
    # threadCount = "100"
    # javascriptTextTransformGcsPath = "gs://your-bucket/your-function.js"
    # javascriptTextTransformFunctionName = "'transform' or 'transform_udf1'"
    # javascriptTextTransformReloadIntervalMinutes = "0"
    # pythonTextTransformGcsPath = "gs://your-bucket/your-transforms/*.py"
    # pythonRuntimeVersion = "<pythonRuntimeVersion>"
    # pythonTextTransformFunctionName = "transform_udf1"
    # runtimeRetries = "5"
    # useStorageWriteApi = "false"
    # useStorageWriteApiAtLeastOnce = "false"
    # numStorageWriteApiStreams = "0"
    # storageWriteApiTriggeringFrequencySec = "<storageWriteApiTriggeringFrequencySec>"
  }
}
```
