
Ordered change stream buffer to Source DB template
---
Streaming pipeline. Reads ordered Spanner change stream message from Pub/Sub to
Kafka, transforms them, and writes them to a Source Database like MySQL.



:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required Parameters

* **sourceShardsFilePath** (Source shard details file path in Cloud Storage): Source shard details file path in Cloud Storage that contains connection profile of source shards.
* **sessionFilePath** (Session File Path in Cloud Storage): Session file path in Cloud Storage that contains mapping information from HarbourBridge.

### Optional Parameters

* **sourceType** (Destination source type): This is the type of source database. Currently only mysql is supported. Defaults to: mysql.
* **bufferType** (Input buffer type): This is the type of input buffer read from. Supported values - PubSub/Kafka. Defaults to: pubsub.
* **pubSubProjectId** (Project id for the PubSub subscriber): This is the project containing the pubsub subscribers. Required when the buffer is PubSub.
* **pubSubMaxReadCount** (Max messages to read from PubSub subscriber): Tuning parameter, to control the throughput. Defaults to: 2000.
* **kafkaClusterFilePath** (File location for Kafka cluster details file in Cloud Storage.): This is the file location for Kafka cluster details file in Cloud Storage.Required when the buffer is Kafka.
* **sourceDbTimezoneOffset** (SourceDB timezone offset): This is the timezone offset from UTC for the source database. Example value: +10:00. Defaults to: +00:00.
* **timerInterval** (Duration in seconds between calls to stateful timer processing. ): Controls the time between successive polls to buffer and processing of the resultant records. Defaults to: 1.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!
[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/ordered-changestream-buffer-to-sourcedb/src/main/java/com/google/cloud/teleport/v2/templates/OrderedChangestreamBufferToSourceDb.java)

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
-DtemplateName="Ordered_Changestream_Buffer_to_Sourcedb" \
-pl v2/ordered-changestream-buffer-to-sourcedb \
-am
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Ordered_Changestream_Buffer_to_Sourcedb
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Ordered_Changestream_Buffer_to_Sourcedb"

### Required
export SOURCE_SHARDS_FILE_PATH=<sourceShardsFilePath>
export SESSION_FILE_PATH=<sessionFilePath>

### Optional
export SOURCE_TYPE=mysql
export BUFFER_TYPE=pubsub
export PUB_SUB_PROJECT_ID=<pubSubProjectId>
export PUB_SUB_MAX_READ_COUNT=2000
export KAFKA_CLUSTER_FILE_PATH=<kafkaClusterFilePath>
export SOURCE_DB_TIMEZONE_OFFSET=+00:00
export TIMER_INTERVAL=1

gcloud dataflow flex-template run "ordered-changestream-buffer-to-sourcedb-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "sourceShardsFilePath=$SOURCE_SHARDS_FILE_PATH" \
  --parameters "sessionFilePath=$SESSION_FILE_PATH" \
  --parameters "sourceType=$SOURCE_TYPE" \
  --parameters "bufferType=$BUFFER_TYPE" \
  --parameters "pubSubProjectId=$PUB_SUB_PROJECT_ID" \
  --parameters "pubSubMaxReadCount=$PUB_SUB_MAX_READ_COUNT" \
  --parameters "kafkaClusterFilePath=$KAFKA_CLUSTER_FILE_PATH" \
  --parameters "sourceDbTimezoneOffset=$SOURCE_DB_TIMEZONE_OFFSET" \
  --parameters "timerInterval=$TIMER_INTERVAL"
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
export SOURCE_SHARDS_FILE_PATH=<sourceShardsFilePath>
export SESSION_FILE_PATH=<sessionFilePath>

### Optional
export SOURCE_TYPE=mysql
export BUFFER_TYPE=pubsub
export PUB_SUB_PROJECT_ID=<pubSubProjectId>
export PUB_SUB_MAX_READ_COUNT=2000
export KAFKA_CLUSTER_FILE_PATH=<kafkaClusterFilePath>
export SOURCE_DB_TIMEZONE_OFFSET=+00:00
export TIMER_INTERVAL=1

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="ordered-changestream-buffer-to-sourcedb-job" \
-DtemplateName="Ordered_Changestream_Buffer_to_Sourcedb" \
-Dparameters="sourceShardsFilePath=$SOURCE_SHARDS_FILE_PATH,sessionFilePath=$SESSION_FILE_PATH,sourceType=$SOURCE_TYPE,bufferType=$BUFFER_TYPE,pubSubProjectId=$PUB_SUB_PROJECT_ID,pubSubMaxReadCount=$PUB_SUB_MAX_READ_COUNT,kafkaClusterFilePath=$KAFKA_CLUSTER_FILE_PATH,sourceDbTimezoneOffset=$SOURCE_DB_TIMEZONE_OFFSET,timerInterval=$TIMER_INTERVAL" \
-pl v2/ordered-changestream-buffer-to-sourcedb \
-am
```

## Terraform

Dataflow supports the utilization of Terraform to manage template jobs,
see [dataflow_flex_template_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_flex_template_job).

Here is an example of Terraform configuration:


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

resource "google_dataflow_flex_template_job" "ordered_changestream_buffer_to_sourcedb" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Ordered_Changestream_Buffer_to_Sourcedb"
  name              = "ordered-changestream-buffer-to-sourcedb"
  region            = var.region
  parameters        = {
    sourceShardsFilePath = "<sourceShardsFilePath>"
    sessionFilePath = "<sessionFilePath>"
    # sourceType = "mysql"
    # bufferType = "pubsub"
    # pubSubProjectId = "<pubSubProjectId>"
    # pubSubMaxReadCount = "2000"
    # kafkaClusterFilePath = "<kafkaClusterFilePath>"
    # sourceDbTimezoneOffset = "+00:00"
    # timerInterval = "1"
  }
}
```
