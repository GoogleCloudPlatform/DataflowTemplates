
Cloud Bigtable Change Streams to PubSub template
---
Streaming pipeline. Streams Bigtable data change records and writes them into
PubSub using Dataflow Runner V2.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/cloud-bigtable-change-streams-to-pubsub)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Bigtable_Change_Streams_to_PubSub).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **pubSubTopic**: The name of the destination Pub/Sub topic.
* **bigtableChangeStreamAppProfile**: The Bigtable application profile ID. The application profile must use single-cluster routing and allow single-row transactions.
* **bigtableReadInstanceId**: The source Bigtable instance ID.
* **bigtableReadTableId**: The source Bigtable table ID.

### Optional parameters

* **messageEncoding**: The encoding of the messages to be published to the Pub/Sub topic. When the schema of the destination topic is configured, the message encoding is determined by the topic settings. The following values are supported: `BINARY` and `JSON`. Defaults to `JSON`.
* **messageFormat**: The encoding of the messages to publish to the Pub/Sub topic. When the schema of the destination topic is configured, the message encoding is determined by the topic settings. The following values are supported: `AVRO`, `PROTOCOL_BUFFERS`, and `JSON`. The default value is `JSON`. When the `JSON` format is used, the rowKey, column, and value fields of the message are strings, the contents of which are determined by the pipeline options `useBase64Rowkeys`, `useBase64ColumnQualifiers`, `useBase64Values`, and `bigtableChangeStreamCharset`.
* **stripValues**: When set to `true`, the `SET_CELL` mutations are returned without new values set. Defaults to `false`. This parameter is useful when you don't need a new value to be present, also known as cache invalidation, or when values are extremely large and exceed Pub/Sub message size limits.
* **dlqDirectory**: The directory for the dead-letter queue. Records that fail to be processed are stored in this directory. Defaults to a directory under the Dataflow job temp location. In most cases, you can use the default path.
* **dlqRetryMinutes**: The number of minutes between dead-letter queue retries. Defaults to `10`.
* **dlqMaxRetries**: The dead letter maximum retries. Defaults to `5`.
* **useBase64Rowkeys**: Used with JSON message encoding. When set to `true`, the `rowKey` field is a Base64-encoded string. Otherwise, the `rowKey` is produced by using `bigtableChangeStreamCharset` to decode bytes into a string. Defaults to `false`.
* **pubSubProjectId**: The Bigtable project ID. The default is the project of the Dataflow job.
* **useBase64ColumnQualifiers**: Used with JSON message encoding. When set to `true`, the `column` field is a Base64-encoded string. Otherwise, the column is produced by using `bigtableChangeStreamCharset` to decode bytes into a string. Defaults to `false`.
* **useBase64Values**: Used with JSON message encoding. When set to `true`, the value field is a Base64-encoded string. Otherwise, the value isproduced by using `bigtableChangeStreamCharset` to decode bytes into a string. Defaults to `false`.
* **disableDlqRetries**: Whether or not to disable retries for the DLQ. Defaults to: false.
* **bigtableChangeStreamMetadataInstanceId**: The Bigtable change streams metadata instance ID. Defaults to empty.
* **bigtableChangeStreamMetadataTableTableId**: The ID of the Bigtable change streams connector metadata table. If not provided, a Bigtable change streams connector metadata table is automatically created during pipeline execution. Defaults to empty.
* **bigtableChangeStreamCharset**: The Bigtable change streams charset name. Defaults to: UTF-8.
* **bigtableChangeStreamStartTimestamp**: The starting timestamp (https://tools.ietf.org/html/rfc3339), inclusive, to use for reading change streams. For example, `2022-05-05T07:59:59Z`. Defaults to the timestamp of the pipeline start time.
* **bigtableChangeStreamIgnoreColumnFamilies**: A comma-separated list of column family name changes to ignore. Defaults to empty.
* **bigtableChangeStreamIgnoreColumns**: A comma-separated list of column name changes to ignore. Defaults to empty.
* **bigtableChangeStreamName**: A unique name for the client pipeline. Lets you resume processing from the point at which a previously running pipeline stopped. Defaults to an automatically generated name. See the Dataflow job logs for the value used.
* **bigtableChangeStreamResume**: When set to `true`, a new pipeline resumes processing from the point at which a previously running pipeline with the same `bigtableChangeStreamName` value stopped. If the pipeline with the given `bigtableChangeStreamName` value has never run, a new pipeline doesn't start. When set to `false`, a new pipeline starts. If a pipeline with the same `bigtableChangeStreamName` value has already run for the given source, a new pipeline doesn't start. Defaults to `false`.
* **bigtableReadProjectId**: The Bigtable project ID. The default is the project for the Dataflow job.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/googlecloud-to-googlecloud/src/main/java/com/google/cloud/teleport/v2/templates/bigtablechangestreamstopubsub/BigtableChangeStreamsToPubSub.java)

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
-DtemplateName="Bigtable_Change_Streams_to_PubSub" \
-f v2/googlecloud-to-googlecloud
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Bigtable_Change_Streams_to_PubSub
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Bigtable_Change_Streams_to_PubSub"

### Required
export PUB_SUB_TOPIC=<pubSubTopic>
export BIGTABLE_CHANGE_STREAM_APP_PROFILE=<bigtableChangeStreamAppProfile>
export BIGTABLE_READ_INSTANCE_ID=<bigtableReadInstanceId>
export BIGTABLE_READ_TABLE_ID=<bigtableReadTableId>

### Optional
export MESSAGE_ENCODING=JSON
export MESSAGE_FORMAT=JSON
export STRIP_VALUES=false
export DLQ_DIRECTORY=""
export DLQ_RETRY_MINUTES=10
export DLQ_MAX_RETRIES=5
export USE_BASE64ROWKEYS=false
export PUB_SUB_PROJECT_ID=""
export USE_BASE64COLUMN_QUALIFIERS=false
export USE_BASE64VALUES=false
export DISABLE_DLQ_RETRIES=false
export BIGTABLE_CHANGE_STREAM_METADATA_INSTANCE_ID=""
export BIGTABLE_CHANGE_STREAM_METADATA_TABLE_TABLE_ID=""
export BIGTABLE_CHANGE_STREAM_CHARSET=UTF-8
export BIGTABLE_CHANGE_STREAM_START_TIMESTAMP=""
export BIGTABLE_CHANGE_STREAM_IGNORE_COLUMN_FAMILIES=""
export BIGTABLE_CHANGE_STREAM_IGNORE_COLUMNS=""
export BIGTABLE_CHANGE_STREAM_NAME=<bigtableChangeStreamName>
export BIGTABLE_CHANGE_STREAM_RESUME=false
export BIGTABLE_READ_PROJECT_ID=""

gcloud dataflow flex-template run "bigtable-change-streams-to-pubsub-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "pubSubTopic=$PUB_SUB_TOPIC" \
  --parameters "messageEncoding=$MESSAGE_ENCODING" \
  --parameters "messageFormat=$MESSAGE_FORMAT" \
  --parameters "stripValues=$STRIP_VALUES" \
  --parameters "dlqDirectory=$DLQ_DIRECTORY" \
  --parameters "dlqRetryMinutes=$DLQ_RETRY_MINUTES" \
  --parameters "dlqMaxRetries=$DLQ_MAX_RETRIES" \
  --parameters "useBase64Rowkeys=$USE_BASE64ROWKEYS" \
  --parameters "pubSubProjectId=$PUB_SUB_PROJECT_ID" \
  --parameters "useBase64ColumnQualifiers=$USE_BASE64COLUMN_QUALIFIERS" \
  --parameters "useBase64Values=$USE_BASE64VALUES" \
  --parameters "disableDlqRetries=$DISABLE_DLQ_RETRIES" \
  --parameters "bigtableChangeStreamMetadataInstanceId=$BIGTABLE_CHANGE_STREAM_METADATA_INSTANCE_ID" \
  --parameters "bigtableChangeStreamMetadataTableTableId=$BIGTABLE_CHANGE_STREAM_METADATA_TABLE_TABLE_ID" \
  --parameters "bigtableChangeStreamAppProfile=$BIGTABLE_CHANGE_STREAM_APP_PROFILE" \
  --parameters "bigtableChangeStreamCharset=$BIGTABLE_CHANGE_STREAM_CHARSET" \
  --parameters "bigtableChangeStreamStartTimestamp=$BIGTABLE_CHANGE_STREAM_START_TIMESTAMP" \
  --parameters "bigtableChangeStreamIgnoreColumnFamilies=$BIGTABLE_CHANGE_STREAM_IGNORE_COLUMN_FAMILIES" \
  --parameters "bigtableChangeStreamIgnoreColumns=$BIGTABLE_CHANGE_STREAM_IGNORE_COLUMNS" \
  --parameters "bigtableChangeStreamName=$BIGTABLE_CHANGE_STREAM_NAME" \
  --parameters "bigtableChangeStreamResume=$BIGTABLE_CHANGE_STREAM_RESUME" \
  --parameters "bigtableReadInstanceId=$BIGTABLE_READ_INSTANCE_ID" \
  --parameters "bigtableReadTableId=$BIGTABLE_READ_TABLE_ID" \
  --parameters "bigtableReadProjectId=$BIGTABLE_READ_PROJECT_ID"
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
export PUB_SUB_TOPIC=<pubSubTopic>
export BIGTABLE_CHANGE_STREAM_APP_PROFILE=<bigtableChangeStreamAppProfile>
export BIGTABLE_READ_INSTANCE_ID=<bigtableReadInstanceId>
export BIGTABLE_READ_TABLE_ID=<bigtableReadTableId>

### Optional
export MESSAGE_ENCODING=JSON
export MESSAGE_FORMAT=JSON
export STRIP_VALUES=false
export DLQ_DIRECTORY=""
export DLQ_RETRY_MINUTES=10
export DLQ_MAX_RETRIES=5
export USE_BASE64ROWKEYS=false
export PUB_SUB_PROJECT_ID=""
export USE_BASE64COLUMN_QUALIFIERS=false
export USE_BASE64VALUES=false
export DISABLE_DLQ_RETRIES=false
export BIGTABLE_CHANGE_STREAM_METADATA_INSTANCE_ID=""
export BIGTABLE_CHANGE_STREAM_METADATA_TABLE_TABLE_ID=""
export BIGTABLE_CHANGE_STREAM_CHARSET=UTF-8
export BIGTABLE_CHANGE_STREAM_START_TIMESTAMP=""
export BIGTABLE_CHANGE_STREAM_IGNORE_COLUMN_FAMILIES=""
export BIGTABLE_CHANGE_STREAM_IGNORE_COLUMNS=""
export BIGTABLE_CHANGE_STREAM_NAME=<bigtableChangeStreamName>
export BIGTABLE_CHANGE_STREAM_RESUME=false
export BIGTABLE_READ_PROJECT_ID=""

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="bigtable-change-streams-to-pubsub-job" \
-DtemplateName="Bigtable_Change_Streams_to_PubSub" \
-Dparameters="pubSubTopic=$PUB_SUB_TOPIC,messageEncoding=$MESSAGE_ENCODING,messageFormat=$MESSAGE_FORMAT,stripValues=$STRIP_VALUES,dlqDirectory=$DLQ_DIRECTORY,dlqRetryMinutes=$DLQ_RETRY_MINUTES,dlqMaxRetries=$DLQ_MAX_RETRIES,useBase64Rowkeys=$USE_BASE64ROWKEYS,pubSubProjectId=$PUB_SUB_PROJECT_ID,useBase64ColumnQualifiers=$USE_BASE64COLUMN_QUALIFIERS,useBase64Values=$USE_BASE64VALUES,disableDlqRetries=$DISABLE_DLQ_RETRIES,bigtableChangeStreamMetadataInstanceId=$BIGTABLE_CHANGE_STREAM_METADATA_INSTANCE_ID,bigtableChangeStreamMetadataTableTableId=$BIGTABLE_CHANGE_STREAM_METADATA_TABLE_TABLE_ID,bigtableChangeStreamAppProfile=$BIGTABLE_CHANGE_STREAM_APP_PROFILE,bigtableChangeStreamCharset=$BIGTABLE_CHANGE_STREAM_CHARSET,bigtableChangeStreamStartTimestamp=$BIGTABLE_CHANGE_STREAM_START_TIMESTAMP,bigtableChangeStreamIgnoreColumnFamilies=$BIGTABLE_CHANGE_STREAM_IGNORE_COLUMN_FAMILIES,bigtableChangeStreamIgnoreColumns=$BIGTABLE_CHANGE_STREAM_IGNORE_COLUMNS,bigtableChangeStreamName=$BIGTABLE_CHANGE_STREAM_NAME,bigtableChangeStreamResume=$BIGTABLE_CHANGE_STREAM_RESUME,bigtableReadInstanceId=$BIGTABLE_READ_INSTANCE_ID,bigtableReadTableId=$BIGTABLE_READ_TABLE_ID,bigtableReadProjectId=$BIGTABLE_READ_PROJECT_ID" \
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
cd v2/googlecloud-to-googlecloud/terraform/Bigtable_Change_Streams_to_PubSub
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

resource "google_dataflow_flex_template_job" "bigtable_change_streams_to_pubsub" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Bigtable_Change_Streams_to_PubSub"
  name              = "bigtable-change-streams-to-pubsub"
  region            = var.region
  parameters        = {
    pubSubTopic = "<pubSubTopic>"
    bigtableChangeStreamAppProfile = "<bigtableChangeStreamAppProfile>"
    bigtableReadInstanceId = "<bigtableReadInstanceId>"
    bigtableReadTableId = "<bigtableReadTableId>"
    # messageEncoding = "JSON"
    # messageFormat = "JSON"
    # stripValues = "false"
    # dlqDirectory = ""
    # dlqRetryMinutes = "10"
    # dlqMaxRetries = "5"
    # useBase64Rowkeys = "false"
    # pubSubProjectId = ""
    # useBase64ColumnQualifiers = "false"
    # useBase64Values = "false"
    # disableDlqRetries = "false"
    # bigtableChangeStreamMetadataInstanceId = ""
    # bigtableChangeStreamMetadataTableTableId = ""
    # bigtableChangeStreamCharset = "UTF-8"
    # bigtableChangeStreamStartTimestamp = ""
    # bigtableChangeStreamIgnoreColumnFamilies = ""
    # bigtableChangeStreamIgnoreColumns = ""
    # bigtableChangeStreamName = "<bigtableChangeStreamName>"
    # bigtableChangeStreamResume = "false"
    # bigtableReadProjectId = ""
  }
}
```
