
Pub/Sub to BigQuery with Python UDFs template
---
The Pub/Sub to BigQuery template is a streaming pipeline that reads
JSON-formatted messages from a Pub/Sub topic or subscription, and writes them to
a BigQuery table. You can use the template as a quick solution to move Pub/Sub
data to BigQuery. The template reads JSON-formatted messages from Pub/Sub and
converts them to BigQuery elements.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/pubsub-to-bigquery)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=PubSub_to_BigQuery_Xlang).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **outputTableSpec**: The BigQuery table to write to, formatted as `PROJECT_ID:DATASET_NAME.TABLE_NAME`.

### Optional parameters

* **inputTopic**: The Pub/Sub topic to read from, formatted as `projects/<PROJECT_ID>/topics/<TOPIC_NAME>`.
* **inputSubscription**: The Pub/Sub subscription to read from, formatted as `projects/<PROJECT_ID>/subscriptions/<SUBCRIPTION_NAME>`.
* **outputDeadletterTable**: The BigQuery table to use for messages that failed to reach the output table, formatted as `PROJECT_ID:DATASET_NAME.TABLE_NAME`. If the table doesn't exist, it is created when the pipeline runs. If this parameter is not specified, the value `OUTPUT_TABLE_SPEC_error_records` is used instead.
* **useStorageWriteApiAtLeastOnce**: When using the Storage Write API, specifies the write semantics. To use at-least-once semantics (https://beam.apache.org/documentation/io/built-in/google-bigquery/#at-least-once-semantics), set this parameter to true. To use exactly-once semantics, set the parameter to `false`. This parameter applies only when `useStorageWriteApi` is `true`. The default value is `false`.
* **useStorageWriteApi**: If true, the pipeline uses the BigQuery Storage Write API (https://cloud.google.com/bigquery/docs/write-api). The default value is `false`. For more information, see Using the Storage Write API (https://beam.apache.org/documentation/io/built-in/google-bigquery/#storage-write-api).
* **numStorageWriteApiStreams**: When using the Storage Write API, specifies the number of write streams. If `useStorageWriteApi` is `true` and `useStorageWriteApiAtLeastOnce` is `false`, then you must set this parameter. Defaults to: 0.
* **storageWriteApiTriggeringFrequencySec**: When using the Storage Write API, specifies the triggering frequency, in seconds. If `useStorageWriteApi` is `true` and `useStorageWriteApiAtLeastOnce` is `false`, then you must set this parameter.
* **pythonExternalTextTransformGcsPath**: The Cloud Storage path pattern for the Python code containing your user-defined functions. For example, `gs://your-bucket/your-function.py`.
* **pythonExternalTextTransformFunctionName**: The name of the function to call from your Python file. Use only letters, digits, and underscores. For example, `'transform' or 'transform_udf1'`.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/googlecloud-to-googlecloud/src/main/java/com/google/cloud/teleport/v2/templates/PubSubToBigQuery.java)

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
-DtemplateName="PubSub_to_BigQuery_Xlang" \
-f v2/googlecloud-to-googlecloud
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/PubSub_to_BigQuery_Xlang
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/PubSub_to_BigQuery_Xlang"

### Required
export OUTPUT_TABLE_SPEC=<outputTableSpec>

### Optional
export INPUT_TOPIC=<inputTopic>
export INPUT_SUBSCRIPTION=<inputSubscription>
export OUTPUT_DEADLETTER_TABLE=<outputDeadletterTable>
export USE_STORAGE_WRITE_API_AT_LEAST_ONCE=false
export USE_STORAGE_WRITE_API=false
export NUM_STORAGE_WRITE_API_STREAMS=0
export STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC=<storageWriteApiTriggeringFrequencySec>
export PYTHON_EXTERNAL_TEXT_TRANSFORM_GCS_PATH=<pythonExternalTextTransformGcsPath>
export PYTHON_EXTERNAL_TEXT_TRANSFORM_FUNCTION_NAME=<pythonExternalTextTransformFunctionName>

gcloud dataflow flex-template run "pubsub-to-bigquery-xlang-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "outputTableSpec=$OUTPUT_TABLE_SPEC" \
  --parameters "inputTopic=$INPUT_TOPIC" \
  --parameters "inputSubscription=$INPUT_SUBSCRIPTION" \
  --parameters "outputDeadletterTable=$OUTPUT_DEADLETTER_TABLE" \
  --parameters "useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE" \
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
export OUTPUT_TABLE_SPEC=<outputTableSpec>

### Optional
export INPUT_TOPIC=<inputTopic>
export INPUT_SUBSCRIPTION=<inputSubscription>
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
-DjobName="pubsub-to-bigquery-xlang-job" \
-DtemplateName="PubSub_to_BigQuery_Xlang" \
-Dparameters="outputTableSpec=$OUTPUT_TABLE_SPEC,inputTopic=$INPUT_TOPIC,inputSubscription=$INPUT_SUBSCRIPTION,outputDeadletterTable=$OUTPUT_DEADLETTER_TABLE,useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE,useStorageWriteApi=$USE_STORAGE_WRITE_API,numStorageWriteApiStreams=$NUM_STORAGE_WRITE_API_STREAMS,storageWriteApiTriggeringFrequencySec=$STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC,pythonExternalTextTransformGcsPath=$PYTHON_EXTERNAL_TEXT_TRANSFORM_GCS_PATH,pythonExternalTextTransformFunctionName=$PYTHON_EXTERNAL_TEXT_TRANSFORM_FUNCTION_NAME" \
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
cd v2/googlecloud-to-googlecloud/terraform/PubSub_to_BigQuery_Xlang
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

resource "google_dataflow_flex_template_job" "pubsub_to_bigquery_xlang" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/PubSub_to_BigQuery_Xlang"
  name              = "pubsub-to-bigquery-xlang"
  region            = var.region
  parameters        = {
    outputTableSpec = "<outputTableSpec>"
    # inputTopic = "<inputTopic>"
    # inputSubscription = "<inputSubscription>"
    # outputDeadletterTable = "<outputDeadletterTable>"
    # useStorageWriteApiAtLeastOnce = "false"
    # useStorageWriteApi = "false"
    # numStorageWriteApiStreams = "0"
    # storageWriteApiTriggeringFrequencySec = "<storageWriteApiTriggeringFrequencySec>"
    # pythonExternalTextTransformGcsPath = "<pythonExternalTextTransformGcsPath>"
    # pythonExternalTextTransformFunctionName = "<pythonExternalTextTransformFunctionName>"
  }
}
```
