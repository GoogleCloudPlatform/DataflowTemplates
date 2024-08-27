
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

### Required parameters

* **inputFilePattern** : The file location for Datastream file output in Cloud Storage, in the format: gs://<BUCKET_NAME>/<ROOT_PATH>/.
* **inputFileFormat** : The format of the output files produced by Datastream. Value can be 'avro' or 'json'. Defaults to: avro.
* **gcsPubSubSubscription** : The Pub/Sub subscription used by Cloud Storage to notify Dataflow of new files available for processing, in the format: projects/<PROJECT_ID>/subscriptions/<SUBSCRIPTION_NAME>.
* **outputStagingDatasetTemplate** : The name of the dataset that contains staging tables. This parameter supports templates, for example {_metadata_dataset}_log or my_dataset_log. Normally, this parameter is a dataset name. Defaults to: {_metadata_dataset}.
* **outputDatasetTemplate** : The name of the dataset that contains the replica tables. This parameter supports templates, for example {_metadata_dataset} or my_dataset. Normally, this parameter is a dataset name. Defaults to: {_metadata_dataset}.
* **deadLetterQueueDirectory** : The path that Dataflow uses to write the dead-letter queue output. This path must not be in the same path as the Datastream file output. Defaults to empty.

### Optional parameters

* **streamName** : The name or the template for the stream to poll for schema information. Defaults to: {_metadata_stream}. The default value is usually enough.
* **rfcStartDateTime** : The starting DateTime to use to fetch data from Cloud Storage (https://tools.ietf.org/html/rfc3339). Defaults to: 1970-01-01T00:00:00.00Z.
* **fileReadConcurrency** : The number of concurrent DataStream files to read. Default is 10.
* **outputProjectId** : The ID of the Google Cloud project that contains the BigQuery datasets to output data into. The default for this parameter is the project where the Dataflow pipeline is running.
* **outputStagingTableNameTemplate** : The template to use to name the staging tables. For example, {_metadata_table}). Defaults to: {_metadata_table}_log.
* **outputTableNameTemplate** : The template to use for the name of the replica tables, for example {_metadata_table}. Defaults to: {_metadata_table}.
* **ignoreFields** : Comma-separated fields to ignore in BigQuery. Defaults to: _metadata_stream,_metadata_schema,_metadata_table,_metadata_source,_metadata_tx_id,_metadata_dlq_reconsumed,_metadata_primary_keys,_metadata_error,_metadata_retry_count. (Example: _metadata_stream,_metadata_schema).
* **mergeFrequencyMinutes** : The number of minutes between merges for a given table. Defaults to: 5.
* **dlqRetryMinutes** : The number of minutes between DLQ Retries. Defaults to: 10.
* **dataStreamRootUrl** : The Datastream API root URL. Defaults to: https://datastream.googleapis.com/.
* **applyMerge** : Whether to disable MERGE queries for the job. Defaults to: true.
* **mergeConcurrency** : The number of concurrent BigQuery MERGE queries. Only effective when applyMerge is set to true. Defaults to: 30.
* **partitionRetentionDays** : The number of days to use for partition retention when running BigQuery merges. Defaults to: 1.
* **useStorageWriteApiAtLeastOnce** : This parameter takes effect only if "Use BigQuery Storage Write API" is enabled. If true, at-least-once semantics are used for the Storage Write API. Otherwise, exactly-once semantics are used. Defaults to: false.
* **javascriptTextTransformGcsPath** : The Cloud Storage URI of the .js file that defines the JavaScript user-defined function (UDF) to use. (Example: gs://my-bucket/my-udfs/my_file.js).
* **javascriptTextTransformFunctionName** : The name of the JavaScript user-defined function (UDF) to use. For example, if your JavaScript function code is `myTransform(inJson) { /*...do stuff...*/ }`, then the function name is `myTransform`. For sample JavaScript UDFs, see UDF Examples (https://github.com/GoogleCloudPlatform/DataflowTemplates#udf-examples).
* **javascriptTextTransformReloadIntervalMinutes** : Specifies how frequently to reload the UDF, in minutes. If the value is greater than 0, Dataflow periodically checks the UDF file in Cloud Storage, and reloads the UDF if the file is modified. This parameter allows you to update the UDF while the pipeline is running, without needing to restart the job. If the value is 0, UDF reloading is disabled. The default value is 0.
* **pythonTextTransformGcsPath** : The Cloud Storage path pattern for the Python code containing your user-defined functions. (Example: gs://your-bucket/your-transforms/*.py).
* **pythonRuntimeVersion** : The runtime version to use for this Python UDF.
* **pythonTextTransformFunctionName** : The name of the function to call from your JavaScript file. Use only letters, digits, and underscores. (Example: transform_udf1).
* **runtimeRetries** : The number of times a runtime will be retried before failing. Defaults to: 5.
* **useStorageWriteApi** : If true, the pipeline uses the BigQuery Storage Write API (https://cloud.google.com/bigquery/docs/write-api). The default value is `false`. For more information, see Using the Storage Write API (https://beam.apache.org/documentation/io/built-in/google-bigquery/#storage-write-api).
* **numStorageWriteApiStreams** : When using the Storage Write API, specifies the number of write streams. If `useStorageWriteApi` is `true` and `useStorageWriteApiAtLeastOnce` is `false`, then you must set this parameter. Defaults to: 0.
* **storageWriteApiTriggeringFrequencySec** : When using the Storage Write API, specifies the triggering frequency, in seconds. If `useStorageWriteApi` is `true` and `useStorageWriteApiAtLeastOnce` is `false`, then you must set this parameter.


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
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#templates-plugin).

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
-f v2/datastream-to-bigquery
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
export USE_STORAGE_WRITE_API_AT_LEAST_ONCE=false
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>
export JAVASCRIPT_TEXT_TRANSFORM_RELOAD_INTERVAL_MINUTES=0
export PYTHON_TEXT_TRANSFORM_GCS_PATH=<pythonTextTransformGcsPath>
export PYTHON_RUNTIME_VERSION=<pythonRuntimeVersion>
export PYTHON_TEXT_TRANSFORM_FUNCTION_NAME=<pythonTextTransformFunctionName>
export RUNTIME_RETRIES=5
export USE_STORAGE_WRITE_API=false
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
  --parameters "useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE" \
  --parameters "javascriptTextTransformGcsPath=$JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH" \
  --parameters "javascriptTextTransformFunctionName=$JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME" \
  --parameters "javascriptTextTransformReloadIntervalMinutes=$JAVASCRIPT_TEXT_TRANSFORM_RELOAD_INTERVAL_MINUTES" \
  --parameters "pythonTextTransformGcsPath=$PYTHON_TEXT_TRANSFORM_GCS_PATH" \
  --parameters "pythonRuntimeVersion=$PYTHON_RUNTIME_VERSION" \
  --parameters "pythonTextTransformFunctionName=$PYTHON_TEXT_TRANSFORM_FUNCTION_NAME" \
  --parameters "runtimeRetries=$RUNTIME_RETRIES" \
  --parameters "useStorageWriteApi=$USE_STORAGE_WRITE_API" \
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
export USE_STORAGE_WRITE_API_AT_LEAST_ONCE=false
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>
export JAVASCRIPT_TEXT_TRANSFORM_RELOAD_INTERVAL_MINUTES=0
export PYTHON_TEXT_TRANSFORM_GCS_PATH=<pythonTextTransformGcsPath>
export PYTHON_RUNTIME_VERSION=<pythonRuntimeVersion>
export PYTHON_TEXT_TRANSFORM_FUNCTION_NAME=<pythonTextTransformFunctionName>
export RUNTIME_RETRIES=5
export USE_STORAGE_WRITE_API=false
export NUM_STORAGE_WRITE_API_STREAMS=0
export STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC=<storageWriteApiTriggeringFrequencySec>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="cloud-datastream-to-bigquery-job" \
-DtemplateName="Cloud_Datastream_to_BigQuery" \
-Dparameters="inputFilePattern=$INPUT_FILE_PATTERN,inputFileFormat=$INPUT_FILE_FORMAT,gcsPubSubSubscription=$GCS_PUB_SUB_SUBSCRIPTION,streamName=$STREAM_NAME,rfcStartDateTime=$RFC_START_DATE_TIME,fileReadConcurrency=$FILE_READ_CONCURRENCY,outputProjectId=$OUTPUT_PROJECT_ID,outputStagingDatasetTemplate=$OUTPUT_STAGING_DATASET_TEMPLATE,outputStagingTableNameTemplate=$OUTPUT_STAGING_TABLE_NAME_TEMPLATE,outputDatasetTemplate=$OUTPUT_DATASET_TEMPLATE,outputTableNameTemplate=$OUTPUT_TABLE_NAME_TEMPLATE,ignoreFields=$IGNORE_FIELDS,mergeFrequencyMinutes=$MERGE_FREQUENCY_MINUTES,deadLetterQueueDirectory=$DEAD_LETTER_QUEUE_DIRECTORY,dlqRetryMinutes=$DLQ_RETRY_MINUTES,dataStreamRootUrl=$DATA_STREAM_ROOT_URL,applyMerge=$APPLY_MERGE,mergeConcurrency=$MERGE_CONCURRENCY,partitionRetentionDays=$PARTITION_RETENTION_DAYS,useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE,javascriptTextTransformGcsPath=$JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH,javascriptTextTransformFunctionName=$JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME,javascriptTextTransformReloadIntervalMinutes=$JAVASCRIPT_TEXT_TRANSFORM_RELOAD_INTERVAL_MINUTES,pythonTextTransformGcsPath=$PYTHON_TEXT_TRANSFORM_GCS_PATH,pythonRuntimeVersion=$PYTHON_RUNTIME_VERSION,pythonTextTransformFunctionName=$PYTHON_TEXT_TRANSFORM_FUNCTION_NAME,runtimeRetries=$RUNTIME_RETRIES,useStorageWriteApi=$USE_STORAGE_WRITE_API,numStorageWriteApiStreams=$NUM_STORAGE_WRITE_API_STREAMS,storageWriteApiTriggeringFrequencySec=$STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC" \
-f v2/datastream-to-bigquery
```

#### Troubleshooting
If there are compilation errors related to template metadata or template plugin framework,
make sure the plugin dependencies are up-to-date by running:
```
mvn clean install -pl plugins/templates-maven-plugin,metadata -am
```
See [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#templates-plugin)
for more information.



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
cd v2/datastream-to-bigquery/terraform/Cloud_Datastream_to_BigQuery
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

resource "google_dataflow_flex_template_job" "cloud_datastream_to_bigquery" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Cloud_Datastream_to_BigQuery"
  name              = "cloud-datastream-to-bigquery"
  region            = var.region
  parameters        = {
    inputFilePattern = "<inputFilePattern>"
    inputFileFormat = "avro"
    gcsPubSubSubscription = "<gcsPubSubSubscription>"
    outputStagingDatasetTemplate = "{_metadata_dataset}"
    outputDatasetTemplate = "{_metadata_dataset}"
    deadLetterQueueDirectory = ""
    # streamName = "<streamName>"
    # rfcStartDateTime = "1970-01-01T00:00:00.00Z"
    # fileReadConcurrency = "10"
    # outputProjectId = "<outputProjectId>"
    # outputStagingTableNameTemplate = "{_metadata_table}_log"
    # outputTableNameTemplate = "{_metadata_table}"
    # ignoreFields = "_metadata_stream,_metadata_schema"
    # mergeFrequencyMinutes = "5"
    # dlqRetryMinutes = "10"
    # dataStreamRootUrl = "https://datastream.googleapis.com/"
    # applyMerge = "true"
    # mergeConcurrency = "30"
    # partitionRetentionDays = "1"
    # useStorageWriteApiAtLeastOnce = "false"
    # javascriptTextTransformGcsPath = "gs://my-bucket/my-udfs/my_file.js"
    # javascriptTextTransformFunctionName = "<javascriptTextTransformFunctionName>"
    # javascriptTextTransformReloadIntervalMinutes = "0"
    # pythonTextTransformGcsPath = "gs://your-bucket/your-transforms/*.py"
    # pythonRuntimeVersion = "<pythonRuntimeVersion>"
    # pythonTextTransformFunctionName = "transform_udf1"
    # runtimeRetries = "5"
    # useStorageWriteApi = "false"
    # numStorageWriteApiStreams = "0"
    # storageWriteApiTriggeringFrequencySec = "<storageWriteApiTriggeringFrequencySec>"
  }
}
```
