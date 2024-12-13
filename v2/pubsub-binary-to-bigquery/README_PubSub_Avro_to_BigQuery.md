
Pub/Sub Avro to BigQuery template
---
The Pub/Sub Avro to BigQuery template is a streaming pipeline that ingests Avro
data from a Pub/Sub subscription into a BigQuery table. Any errors which occur
while writing to the BigQuery table are streamed into a Pub/Sub unprocessed
topic.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/pubsub-avro-to-bigquery)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=PubSub_Avro_to_BigQuery).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **schemaPath**: The Cloud Storage location of the Avro schema file. For example, `gs://path/to/my/schema.avsc`.
* **inputSubscription**: The Pub/Sub input subscription to read from. For example, `projects/<PROJECT_ID>/subscription/<SUBSCRIPTION_ID>`.
* **outputTableSpec**: The BigQuery output table location to write the output to. For example, `<PROJECT_ID>:<DATASET_NAME>.<TABLE_NAME>`.Depending on the `createDisposition` specified, the output table might be created automatically using the user provided Avro schema.
* **outputTopic**: The Pub/Sub topic to use for unprocessed records. For example, `projects/<PROJECT_ID>/topics/<TOPIC_NAME>`.

### Optional parameters

* **useStorageWriteApiAtLeastOnce**:  When using the Storage Write API, specifies the write semantics. To use at-least-once semantics (https://beam.apache.org/documentation/io/built-in/google-bigquery/#at-least-once-semantics), set this parameter to true. To use exactly-once semantics, set the parameter to `false`. This parameter applies only when `useStorageWriteApi` is `true`. The default value is `false`.
* **writeDisposition**: The BigQuery WriteDisposition (https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobconfigurationload) value. For example, `WRITE_APPEND`, `WRITE_EMPTY`, or `WRITE_TRUNCATE`. Defaults to `WRITE_APPEND`.
* **createDisposition**: The BigQuery CreateDisposition (https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobconfigurationload). For example, `CREATE_IF_NEEDED` and `CREATE_NEVER`. Defaults to `CREATE_IF_NEEDED`.
* **useStorageWriteApi**: If true, the pipeline uses the BigQuery Storage Write API (https://cloud.google.com/bigquery/docs/write-api). The default value is `false`. For more information, see Using the Storage Write API (https://beam.apache.org/documentation/io/built-in/google-bigquery/#storage-write-api).
* **numStorageWriteApiStreams**: When using the Storage Write API, specifies the number of write streams. If `useStorageWriteApi` is `true` and `useStorageWriteApiAtLeastOnce` is `false`, then you must set this parameter. Defaults to: 0.
* **storageWriteApiTriggeringFrequencySec**: When using the Storage Write API, specifies the triggering frequency, in seconds. If `useStorageWriteApi` is `true` and `useStorageWriteApiAtLeastOnce` is `false`, then you must set this parameter.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/pubsub-binary-to-bigquery/src/main/java/com/google/cloud/teleport/v2/templates/PubsubAvroToBigQuery.java)

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
-DtemplateName="PubSub_Avro_to_BigQuery" \
-f v2/pubsub-binary-to-bigquery
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/PubSub_Avro_to_BigQuery
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/PubSub_Avro_to_BigQuery"

### Required
export SCHEMA_PATH=<schemaPath>
export INPUT_SUBSCRIPTION=<inputSubscription>
export OUTPUT_TABLE_SPEC=<outputTableSpec>
export OUTPUT_TOPIC=<outputTopic>

### Optional
export USE_STORAGE_WRITE_API_AT_LEAST_ONCE=false
export WRITE_DISPOSITION=WRITE_APPEND
export CREATE_DISPOSITION=CREATE_IF_NEEDED
export USE_STORAGE_WRITE_API=false
export NUM_STORAGE_WRITE_API_STREAMS=0
export STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC=<storageWriteApiTriggeringFrequencySec>

gcloud dataflow flex-template run "pubsub-avro-to-bigquery-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "schemaPath=$SCHEMA_PATH" \
  --parameters "useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE" \
  --parameters "inputSubscription=$INPUT_SUBSCRIPTION" \
  --parameters "outputTableSpec=$OUTPUT_TABLE_SPEC" \
  --parameters "writeDisposition=$WRITE_DISPOSITION" \
  --parameters "createDisposition=$CREATE_DISPOSITION" \
  --parameters "outputTopic=$OUTPUT_TOPIC" \
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
export SCHEMA_PATH=<schemaPath>
export INPUT_SUBSCRIPTION=<inputSubscription>
export OUTPUT_TABLE_SPEC=<outputTableSpec>
export OUTPUT_TOPIC=<outputTopic>

### Optional
export USE_STORAGE_WRITE_API_AT_LEAST_ONCE=false
export WRITE_DISPOSITION=WRITE_APPEND
export CREATE_DISPOSITION=CREATE_IF_NEEDED
export USE_STORAGE_WRITE_API=false
export NUM_STORAGE_WRITE_API_STREAMS=0
export STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC=<storageWriteApiTriggeringFrequencySec>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="pubsub-avro-to-bigquery-job" \
-DtemplateName="PubSub_Avro_to_BigQuery" \
-Dparameters="schemaPath=$SCHEMA_PATH,useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE,inputSubscription=$INPUT_SUBSCRIPTION,outputTableSpec=$OUTPUT_TABLE_SPEC,writeDisposition=$WRITE_DISPOSITION,createDisposition=$CREATE_DISPOSITION,outputTopic=$OUTPUT_TOPIC,useStorageWriteApi=$USE_STORAGE_WRITE_API,numStorageWriteApiStreams=$NUM_STORAGE_WRITE_API_STREAMS,storageWriteApiTriggeringFrequencySec=$STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC" \
-f v2/pubsub-binary-to-bigquery
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
cd v2/pubsub-binary-to-bigquery/terraform/PubSub_Avro_to_BigQuery
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

resource "google_dataflow_flex_template_job" "pubsub_avro_to_bigquery" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/PubSub_Avro_to_BigQuery"
  name              = "pubsub-avro-to-bigquery"
  region            = var.region
  parameters        = {
    schemaPath = "<schemaPath>"
    inputSubscription = "<inputSubscription>"
    outputTableSpec = "<outputTableSpec>"
    outputTopic = "<outputTopic>"
    # useStorageWriteApiAtLeastOnce = "false"
    # writeDisposition = "WRITE_APPEND"
    # createDisposition = "CREATE_IF_NEEDED"
    # useStorageWriteApi = "false"
    # numStorageWriteApiStreams = "0"
    # storageWriteApiTriggeringFrequencySec = "<storageWriteApiTriggeringFrequencySec>"
  }
}
```
