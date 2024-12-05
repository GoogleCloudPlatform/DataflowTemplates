
Text Files on Cloud Storage to BigQuery with BigQuery Storage API support template
---
The Cloud Storage Text to BigQuery pipeline is a batch pipeline that allows you
to read text files stored in Cloud Storage, transform them using a JavaScript
User Defined Function (UDF) that you provide, and append the result to a BigQuery
table.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/cloud-storage-to-bigquery)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=GCS_Text_to_BigQuery_Flex).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **inputFilePattern**: The gs:// path to the text in Cloud Storage you'd like to process. For example, `gs://your-bucket/your-file.txt`.
* **JSONPath**: The gs:// path to the JSON file that defines your BigQuery schema, stored in Cloud Storage. For example, `gs://your-bucket/your-schema.json`.
* **outputTable**: The location of the BigQuery table to use to store the processed data. If you reuse an existing table, it is overwritten. For example, `<PROJECT_ID>:<DATASET_NAME>.<TABLE_NAME>`.
* **javascriptTextTransformGcsPath**: The Cloud Storage URI of the `.js` file that defines the JavaScript user-defined function (UDF) you want to use. For example, `gs://your-bucket/your-transforms/*.js`.
* **javascriptTextTransformFunctionName**: The name of the JavaScript user-defined function (UDF) that you want to use. For example, if your JavaScript function code is `myTransform(inJson) { /*...do stuff...*/ }`, then the function name is `myTransform`. For sample JavaScript UDFs, see UDF Examples (https://github.com/GoogleCloudPlatform/DataflowTemplates#udf-examples) For example, `transform_udf1`.
* **bigQueryLoadingTemporaryDirectory**: Temporary directory for BigQuery loading process. For example, `gs://your-bucket/your-files/temp-dir`.

### Optional parameters

* **useStorageWriteApi**: If `true`, the pipeline uses the BigQuery Storage Write API (https://cloud.google.com/bigquery/docs/write-api). The default value is `false`. For more information, see Using the Storage Write API (https://beam.apache.org/documentation/io/built-in/google-bigquery/#storage-write-api).
* **useStorageWriteApiAtLeastOnce**: When using the Storage Write API, specifies the write semantics. To use at-least-once semantics (https://beam.apache.org/documentation/io/built-in/google-bigquery/#at-least-once-semantics), set this parameter to `true`. To use exactly-once semantics, set the parameter to `false`. This parameter applies only when `useStorageWriteApi` is `true`. The default value is `false`.


## User-Defined functions (UDFs)

The Text Files on Cloud Storage to BigQuery with BigQuery Storage API support Template supports User-Defined functions (UDFs).
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

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/googlecloud-to-googlecloud/src/main/java/com/google/cloud/teleport/v2/templates/TextIOToBigQuery.java)

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
-DtemplateName="GCS_Text_to_BigQuery_Flex" \
-f v2/googlecloud-to-googlecloud
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/GCS_Text_to_BigQuery_Flex
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/GCS_Text_to_BigQuery_Flex"

### Required
export INPUT_FILE_PATTERN=<inputFilePattern>
export JSONPATH=<JSONPath>
export OUTPUT_TABLE=<outputTable>
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>
export BIG_QUERY_LOADING_TEMPORARY_DIRECTORY=<bigQueryLoadingTemporaryDirectory>

### Optional
export USE_STORAGE_WRITE_API=false
export USE_STORAGE_WRITE_API_AT_LEAST_ONCE=false

gcloud dataflow flex-template run "gcs-text-to-bigquery-flex-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "inputFilePattern=$INPUT_FILE_PATTERN" \
  --parameters "JSONPath=$JSONPATH" \
  --parameters "outputTable=$OUTPUT_TABLE" \
  --parameters "javascriptTextTransformGcsPath=$JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH" \
  --parameters "javascriptTextTransformFunctionName=$JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME" \
  --parameters "bigQueryLoadingTemporaryDirectory=$BIG_QUERY_LOADING_TEMPORARY_DIRECTORY" \
  --parameters "useStorageWriteApi=$USE_STORAGE_WRITE_API" \
  --parameters "useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE"
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
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>
export BIG_QUERY_LOADING_TEMPORARY_DIRECTORY=<bigQueryLoadingTemporaryDirectory>

### Optional
export USE_STORAGE_WRITE_API=false
export USE_STORAGE_WRITE_API_AT_LEAST_ONCE=false

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="gcs-text-to-bigquery-flex-job" \
-DtemplateName="GCS_Text_to_BigQuery_Flex" \
-Dparameters="inputFilePattern=$INPUT_FILE_PATTERN,JSONPath=$JSONPATH,outputTable=$OUTPUT_TABLE,javascriptTextTransformGcsPath=$JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH,javascriptTextTransformFunctionName=$JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME,bigQueryLoadingTemporaryDirectory=$BIG_QUERY_LOADING_TEMPORARY_DIRECTORY,useStorageWriteApi=$USE_STORAGE_WRITE_API,useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE" \
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
cd v2/googlecloud-to-googlecloud/terraform/GCS_Text_to_BigQuery_Flex
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

resource "google_dataflow_flex_template_job" "gcs_text_to_bigquery_flex" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/GCS_Text_to_BigQuery_Flex"
  name              = "gcs-text-to-bigquery-flex"
  region            = var.region
  parameters        = {
    inputFilePattern = "<inputFilePattern>"
    JSONPath = "<JSONPath>"
    outputTable = "<outputTable>"
    javascriptTextTransformGcsPath = "<javascriptTextTransformGcsPath>"
    javascriptTextTransformFunctionName = "<javascriptTextTransformFunctionName>"
    bigQueryLoadingTemporaryDirectory = "<bigQueryLoadingTemporaryDirectory>"
    # useStorageWriteApi = "false"
    # useStorageWriteApiAtLeastOnce = "false"
  }
}
```
