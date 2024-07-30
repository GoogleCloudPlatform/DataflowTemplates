
Synchronizing CDC data to BigQuery template
---
A pipeline to synchronize a Change Data Capture streams to BigQuery.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/mysql-change-data-capture-to-bigquery)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Cdc_To_BigQuery_Template).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **inputSubscriptions** : The comma-separated list of Pub/Sub input subscriptions to read from, in the format `<SUBSCRIPTION_NAME>,<SUBSCRIPTION_NAME>, ...`.
* **changeLogDataset** : The BigQuery dataset to store the staging tables in, in the format <DATASET_NAME>.
* **replicaDataset** : The location of the BigQuery dataset to store the replica tables in, in the format <DATASET_NAME>.

### Optional parameters

* **inputTopics** : Comma-separated list of PubSub topics to where CDC data is being pushed.
* **updateFrequencySecs** : The interval at which the pipeline updates the BigQuery table replicating the MySQL database.
* **useSingleTopic** : Set this to `true` if you have configured your Debezium connector to publish all table updates to a single topic. Defaults to: false.
* **useStorageWriteApi** : If true, the pipeline uses the BigQuery Storage Write API (https://cloud.google.com/bigquery/docs/write-api). The default value is `false`. For more information, see Using the Storage Write API (https://beam.apache.org/documentation/io/built-in/google-bigquery/#storage-write-api).
* **useStorageWriteApiAtLeastOnce** :  When using the Storage Write API, specifies the write semantics. To use at-least once semantics (https://beam.apache.org/documentation/io/built-in/google-bigquery/#at-least-once-semantics), set this parameter to `true`. To use exactly-once semantics, set the parameter to `false`. This parameter applies only when `useStorageWriteApi` is `true`. The default value is `false`.
* **numStorageWriteApiStreams** : When using the Storage Write API, specifies the number of write streams. If `useStorageWriteApi` is `true` and `useStorageWriteApiAtLeastOnce` is `false`, then you must set this parameter. Defaults to: 0.
* **storageWriteApiTriggeringFrequencySec** : When using the Storage Write API, specifies the triggering frequency, in seconds. If `useStorageWriteApi` is `true` and `useStorageWriteApiAtLeastOnce` is `false`, then you must set this parameter.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/cdc-parent/cdc-change-applier/src/main/java/com/google/cloud/dataflow/cdc/applier/CdcToBigQueryChangeApplierPipeline.java)

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
-DtemplateName="Cdc_To_BigQuery_Template" \
-f v2/cdc-change-applier
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Cdc_To_BigQuery_Template
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Cdc_To_BigQuery_Template"

### Required
export INPUT_SUBSCRIPTIONS=<inputSubscriptions>
export CHANGE_LOG_DATASET=<changeLogDataset>
export REPLICA_DATASET=<replicaDataset>

### Optional
export INPUT_TOPICS=<inputTopics>
export UPDATE_FREQUENCY_SECS=<updateFrequencySecs>
export USE_SINGLE_TOPIC=false
export USE_STORAGE_WRITE_API=false
export USE_STORAGE_WRITE_API_AT_LEAST_ONCE=false
export NUM_STORAGE_WRITE_API_STREAMS=0
export STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC=<storageWriteApiTriggeringFrequencySec>

gcloud dataflow flex-template run "cdc-to-bigquery-template-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "inputTopics=$INPUT_TOPICS" \
  --parameters "inputSubscriptions=$INPUT_SUBSCRIPTIONS" \
  --parameters "changeLogDataset=$CHANGE_LOG_DATASET" \
  --parameters "replicaDataset=$REPLICA_DATASET" \
  --parameters "updateFrequencySecs=$UPDATE_FREQUENCY_SECS" \
  --parameters "useSingleTopic=$USE_SINGLE_TOPIC" \
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
export INPUT_SUBSCRIPTIONS=<inputSubscriptions>
export CHANGE_LOG_DATASET=<changeLogDataset>
export REPLICA_DATASET=<replicaDataset>

### Optional
export INPUT_TOPICS=<inputTopics>
export UPDATE_FREQUENCY_SECS=<updateFrequencySecs>
export USE_SINGLE_TOPIC=false
export USE_STORAGE_WRITE_API=false
export USE_STORAGE_WRITE_API_AT_LEAST_ONCE=false
export NUM_STORAGE_WRITE_API_STREAMS=0
export STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC=<storageWriteApiTriggeringFrequencySec>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="cdc-to-bigquery-template-job" \
-DtemplateName="Cdc_To_BigQuery_Template" \
-Dparameters="inputTopics=$INPUT_TOPICS,inputSubscriptions=$INPUT_SUBSCRIPTIONS,changeLogDataset=$CHANGE_LOG_DATASET,replicaDataset=$REPLICA_DATASET,updateFrequencySecs=$UPDATE_FREQUENCY_SECS,useSingleTopic=$USE_SINGLE_TOPIC,useStorageWriteApi=$USE_STORAGE_WRITE_API,useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE,numStorageWriteApiStreams=$NUM_STORAGE_WRITE_API_STREAMS,storageWriteApiTriggeringFrequencySec=$STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC" \
-f v2/cdc-change-applier
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
cd v2/cdc-change-applier/terraform/Cdc_To_BigQuery_Template
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

resource "google_dataflow_flex_template_job" "cdc_to_bigquery_template" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Cdc_To_BigQuery_Template"
  name              = "cdc-to-bigquery-template"
  region            = var.region
  parameters        = {
    inputSubscriptions = "<inputSubscriptions>"
    changeLogDataset = "<changeLogDataset>"
    replicaDataset = "<replicaDataset>"
    # inputTopics = "<inputTopics>"
    # updateFrequencySecs = "<updateFrequencySecs>"
    # useSingleTopic = "false"
    # useStorageWriteApi = "false"
    # useStorageWriteApiAtLeastOnce = "false"
    # numStorageWriteApiStreams = "0"
    # storageWriteApiTriggeringFrequencySec = "<storageWriteApiTriggeringFrequencySec>"
  }
}
```
