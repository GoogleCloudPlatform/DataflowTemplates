
Cloud Storage Text to BigQuery (Stream) with Python UDF template
---
The Text Files on Cloud Storage to BigQuery pipeline is a streaming pipeline that
allows you to stream text files stored in Cloud Storage, transform them using a
Python User Defined Function (UDF) that you provide, and append the result to
BigQuery.

The pipeline runs indefinitely and needs to be terminated manually via a
<a
href="https://cloud.google.com/dataflow/docs/guides/stopping-a-pipeline#cancel">cancel</a>
and not a
<a
href="https://cloud.google.com/dataflow/docs/guides/stopping-a-pipeline#drain">drain</a>,
due to its use of the
<code>Watch</code> transform, which is a splittable <code>DoFn</code> that does
not support
draining.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/text-to-bigquery-stream)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Stream_GCS_Text_to_BigQuery_Xlang).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **inputFilePattern** : The path to the Cloud Storage text to read. (Example: gs://your-bucket/your-file.txt).
* **JSONPath** : The Cloud Storage path to the JSON file that defines your BigQuery schema. (Example: gs://your-bucket/your-schema.json).
* **outputTable** : The location of the BigQuery table in which to store your processed data. If you reuse an existing table, it will be overwritten. (Example: your-project:your-dataset.your-table).
* **bigQueryLoadingTemporaryDirectory** : Temporary directory for the BigQuery loading process. (Example: gs://your-bucket/your-files/temp-dir).

### Optional parameters

* **outputDeadletterTable** : BigQuery table for failed messages. Messages failed to reach the output table for different reasons (e.g., mismatched schema, malformed json) are written to this table. If it doesn't exist, it will be created during pipeline execution. If not specified, "outputTableSpec_error_records" is used instead. (Example: your-project-id:your-dataset.your-table-name).
* **useStorageWriteApiAtLeastOnce** : This parameter takes effect only if "Use BigQuery Storage Write API" is enabled. If enabled the at-least-once semantics will be used for Storage Write API, otherwise exactly-once semantics will be used. Defaults to: false.
* **useStorageWriteApi** : If enabled (set to true) the pipeline will use Storage Write API when writing the data to BigQuery (see https://cloud.google.com/blog/products/data-analytics/streaming-data-into-bigquery-using-storage-write-api). Defaults to: false.
* **numStorageWriteApiStreams** : Number of streams defines the parallelism of the BigQueryIO’s Write transform and roughly corresponds to the number of Storage Write API’s streams which will be used by the pipeline. See https://cloud.google.com/blog/products/data-analytics/streaming-data-into-bigquery-using-storage-write-api for the recommended values. Defaults to: 0.
* **storageWriteApiTriggeringFrequencySec** : Triggering frequency will determine how soon the data will be visible for querying in BigQuery. See https://cloud.google.com/blog/products/data-analytics/streaming-data-into-bigquery-using-storage-write-api for the recommended values.
* **pythonExternalTextTransformGcsPath** : The Cloud Storage path pattern for the Python code containing your user-defined functions. (Example: gs://your-bucket/your-function.py).
* **pythonExternalTextTransformFunctionName** : The name of the function to call from your Python file. Use only letters, digits, and underscores. (Example: 'transform' or 'transform_udf1').



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/googlecloud-to-googlecloud/src/main/java/com/google/cloud/teleport/v2/templates/TextToBigQueryStreaming.java)

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
-DtemplateName="Stream_GCS_Text_to_BigQuery_Xlang" \
-f v2/googlecloud-to-googlecloud
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Stream_GCS_Text_to_BigQuery_Xlang
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Stream_GCS_Text_to_BigQuery_Xlang"

### Required
export INPUT_FILE_PATTERN=<inputFilePattern>
export JSONPATH=<JSONPath>
export OUTPUT_TABLE=<outputTable>
export BIG_QUERY_LOADING_TEMPORARY_DIRECTORY=<bigQueryLoadingTemporaryDirectory>

### Optional
export OUTPUT_DEADLETTER_TABLE=<outputDeadletterTable>
export USE_STORAGE_WRITE_API_AT_LEAST_ONCE=false
export USE_STORAGE_WRITE_API=false
export NUM_STORAGE_WRITE_API_STREAMS=0
export STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC=<storageWriteApiTriggeringFrequencySec>
export PYTHON_EXTERNAL_TEXT_TRANSFORM_GCS_PATH=<pythonExternalTextTransformGcsPath>
export PYTHON_EXTERNAL_TEXT_TRANSFORM_FUNCTION_NAME=<pythonExternalTextTransformFunctionName>

gcloud dataflow flex-template run "stream-gcs-text-to-bigquery-xlang-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "outputDeadletterTable=$OUTPUT_DEADLETTER_TABLE" \
  --parameters "useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE" \
  --parameters "inputFilePattern=$INPUT_FILE_PATTERN" \
  --parameters "JSONPath=$JSONPATH" \
  --parameters "outputTable=$OUTPUT_TABLE" \
  --parameters "bigQueryLoadingTemporaryDirectory=$BIG_QUERY_LOADING_TEMPORARY_DIRECTORY" \
  --parameters "useStorageWriteApi=$USE_STORAGE_WRITE_API" \
  --parameters "numStorageWriteApiStreams=$NUM_STORAGE_WRITE_API_STREAMS" \
  --parameters "storageWriteApiTriggeringFrequencySec=$STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC" \
  --parameters "pythonExternalTextTransformGcsPath=$PYTHON_EXTERNAL_TEXT_TRANSFORM_GCS_PATH" \
  --parameters "pythonExternalTextTransformFunctionName=$PYTHON_EXTERNAL_TEXT_TRANSFORM_FUNCTION_NAME"
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
export JSONPATH=<JSONPath>
export OUTPUT_TABLE=<outputTable>
export BIG_QUERY_LOADING_TEMPORARY_DIRECTORY=<bigQueryLoadingTemporaryDirectory>

### Optional
export OUTPUT_DEADLETTER_TABLE=<outputDeadletterTable>
export USE_STORAGE_WRITE_API_AT_LEAST_ONCE=false
export USE_STORAGE_WRITE_API=false
export NUM_STORAGE_WRITE_API_STREAMS=0
export STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC=<storageWriteApiTriggeringFrequencySec>
export PYTHON_EXTERNAL_TEXT_TRANSFORM_GCS_PATH=<pythonExternalTextTransformGcsPath>
export PYTHON_EXTERNAL_TEXT_TRANSFORM_FUNCTION_NAME=<pythonExternalTextTransformFunctionName>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="stream-gcs-text-to-bigquery-xlang-job" \
-DtemplateName="Stream_GCS_Text_to_BigQuery_Xlang" \
-Dparameters="outputDeadletterTable=$OUTPUT_DEADLETTER_TABLE,useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE,inputFilePattern=$INPUT_FILE_PATTERN,JSONPath=$JSONPATH,outputTable=$OUTPUT_TABLE,bigQueryLoadingTemporaryDirectory=$BIG_QUERY_LOADING_TEMPORARY_DIRECTORY,useStorageWriteApi=$USE_STORAGE_WRITE_API,numStorageWriteApiStreams=$NUM_STORAGE_WRITE_API_STREAMS,storageWriteApiTriggeringFrequencySec=$STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC,pythonExternalTextTransformGcsPath=$PYTHON_EXTERNAL_TEXT_TRANSFORM_GCS_PATH,pythonExternalTextTransformFunctionName=$PYTHON_EXTERNAL_TEXT_TRANSFORM_FUNCTION_NAME" \
-f v2/googlecloud-to-googlecloud
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
cd v2/googlecloud-to-googlecloud/terraform/Stream_GCS_Text_to_BigQuery_Xlang
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

resource "google_dataflow_flex_template_job" "stream_gcs_text_to_bigquery_xlang" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Stream_GCS_Text_to_BigQuery_Xlang"
  name              = "stream-gcs-text-to-bigquery-xlang"
  region            = var.region
  parameters        = {
    inputFilePattern = "gs://your-bucket/your-file.txt"
    JSONPath = "gs://your-bucket/your-schema.json"
    outputTable = "your-project:your-dataset.your-table"
    bigQueryLoadingTemporaryDirectory = "gs://your-bucket/your-files/temp-dir"
    # outputDeadletterTable = "your-project-id:your-dataset.your-table-name"
    # useStorageWriteApiAtLeastOnce = "false"
    # useStorageWriteApi = "false"
    # numStorageWriteApiStreams = "0"
    # storageWriteApiTriggeringFrequencySec = "<storageWriteApiTriggeringFrequencySec>"
    # pythonExternalTextTransformGcsPath = "gs://your-bucket/your-function.py"
    # pythonExternalTextTransformFunctionName = "'transform' or 'transform_udf1'"
  }
}
```
