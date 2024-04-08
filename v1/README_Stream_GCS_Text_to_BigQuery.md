
Cloud Storage Text to BigQuery (Stream) template
---
The Text Files on Cloud Storage to BigQuery pipeline is a streaming pipeline that
allows you to stream text files stored in Cloud Storage, transform them using a
JavaScript User Defined Function (UDF) that you provide, and append the result to
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
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Stream_GCS_Text_to_BigQuery).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **inputFilePattern** : Path of the file pattern glob to read from. (Example: gs://your-bucket/path/*.csv).
* **JSONPath** : JSON file with BigQuery Schema description. JSON Example: {
	"BigQuery Schema": [
		{
			"name": "location",
			"type": "STRING"
		},
		{
			"name": "name",
			"type": "STRING"
		},
		{
			"name": "age",
			"type": "STRING"
		},
		{
			"name": "color",
			"type": "STRING"
		},
		{
			"name": "coffee",
			"type": "STRING"
		}
	]
}.
* **outputTable** : BigQuery table location to write the output to. The table's schema must match the input objects.
* **bigQueryLoadingTemporaryDirectory** : Temporary directory for BigQuery loading process (Example: gs://your-bucket/your-files/temp_dir).

### Optional parameters

* **outputDeadletterTable** : BigQuery table for failed messages. Messages failed to reach the output table for different reasons (e.g., mismatched schema, malformed json) are written to this table. If it doesn't exist, it will be created during pipeline execution. If not specified, "outputTableSpec_error_records" is used instead. (Example: your-project-id:your-dataset.your-table-name).
* **javascriptTextTransformGcsPath** : The Cloud Storage path pattern for the JavaScript code containing your user-defined functions.
* **javascriptTextTransformFunctionName** : The name of the function to call from your JavaScript file. Use only letters, digits, and underscores. (Example: transform_udf1).
* **javascriptTextTransformReloadIntervalMinutes** : Define the interval that workers may check for JavaScript UDF changes to reload the files. Defaults to: 0.


## User-Defined functions (UDFs)

The Cloud Storage Text to BigQuery (Stream) Template supports User-Defined functions (UDFs).
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

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v1/src/main/java/com/google/cloud/teleport/templates/TextToBigQueryStreaming.java)

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates#templates-plugin).

### Building Template

This template is a Classic Template, meaning that the pipeline code will be
executed only once and the pipeline will be saved to Google Cloud Storage for
further reuse. Please check [Creating classic Dataflow templates](https://cloud.google.com/dataflow/docs/guides/templates/creating-templates)
and [Running classic templates](https://cloud.google.com/dataflow/docs/guides/templates/running-templates)
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
-DtemplateName="Stream_GCS_Text_to_BigQuery" \
-f v1
```

The `-DgcpTempLocation=<temp-bucket-name>` parameter can be specified to set the GCS bucket used by the DataflowRunner to write
temp files to during serialization. The path used will be `gs://<temp-bucket-name>/temp/`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Classic Template was staged! gs://<bucket-name>/templates/Stream_GCS_Text_to_BigQuery
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/Stream_GCS_Text_to_BigQuery"

### Required
export INPUT_FILE_PATTERN=<inputFilePattern>
export JSONPATH=<JSONPath>
export OUTPUT_TABLE=<outputTable>
export BIG_QUERY_LOADING_TEMPORARY_DIRECTORY=<bigQueryLoadingTemporaryDirectory>

### Optional
export OUTPUT_DEADLETTER_TABLE=<outputDeadletterTable>
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>
export JAVASCRIPT_TEXT_TRANSFORM_RELOAD_INTERVAL_MINUTES=0

gcloud dataflow jobs run "stream-gcs-text-to-bigquery-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "outputDeadletterTable=$OUTPUT_DEADLETTER_TABLE" \
  --parameters "inputFilePattern=$INPUT_FILE_PATTERN" \
  --parameters "JSONPath=$JSONPATH" \
  --parameters "outputTable=$OUTPUT_TABLE" \
  --parameters "bigQueryLoadingTemporaryDirectory=$BIG_QUERY_LOADING_TEMPORARY_DIRECTORY" \
  --parameters "javascriptTextTransformGcsPath=$JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH" \
  --parameters "javascriptTextTransformFunctionName=$JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME" \
  --parameters "javascriptTextTransformReloadIntervalMinutes=$JAVASCRIPT_TEXT_TRANSFORM_RELOAD_INTERVAL_MINUTES"
```

For more information about the command, please check:
https://cloud.google.com/sdk/gcloud/reference/dataflow/jobs/run


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
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>
export JAVASCRIPT_TEXT_TRANSFORM_RELOAD_INTERVAL_MINUTES=0

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="stream-gcs-text-to-bigquery-job" \
-DtemplateName="Stream_GCS_Text_to_BigQuery" \
-Dparameters="outputDeadletterTable=$OUTPUT_DEADLETTER_TABLE,inputFilePattern=$INPUT_FILE_PATTERN,JSONPath=$JSONPATH,outputTable=$OUTPUT_TABLE,bigQueryLoadingTemporaryDirectory=$BIG_QUERY_LOADING_TEMPORARY_DIRECTORY,javascriptTextTransformGcsPath=$JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH,javascriptTextTransformFunctionName=$JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME,javascriptTextTransformReloadIntervalMinutes=$JAVASCRIPT_TEXT_TRANSFORM_RELOAD_INTERVAL_MINUTES" \
-f v1
```

## Terraform

Dataflow supports the utilization of Terraform to manage template jobs,
see [dataflow_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_job).

Terraform modules have been generated for most templates in this repository. This includes the relevant parameters
specific to the template. If available, they may be used instead of
[dataflow_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_job)
directly.

To use the autogenerated module, execute the standard
[terraform workflow](https://developer.hashicorp.com/terraform/intro/core-workflow):

```shell
cd v1/terraform/Stream_GCS_Text_to_BigQuery
terraform init
terraform apply
```

To use
[dataflow_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_job)
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

resource "google_dataflow_job" "stream_gcs_text_to_bigquery" {

  provider          = google-beta
  template_gcs_path = "gs://dataflow-templates-${var.region}/latest/Stream_GCS_Text_to_BigQuery"
  name              = "stream-gcs-text-to-bigquery"
  region            = var.region
  temp_gcs_location = "gs://bucket-name-here/temp"
  parameters        = {
    inputFilePattern = "gs://your-bucket/path/*.csv"
    JSONPath = "<JSONPath>"
    outputTable = "<outputTable>"
    bigQueryLoadingTemporaryDirectory = "gs://your-bucket/your-files/temp_dir"
    # outputDeadletterTable = "your-project-id:your-dataset.your-table-name"
    # javascriptTextTransformGcsPath = "<javascriptTextTransformGcsPath>"
    # javascriptTextTransformFunctionName = "transform_udf1"
    # javascriptTextTransformReloadIntervalMinutes = "0"
  }
}
```
