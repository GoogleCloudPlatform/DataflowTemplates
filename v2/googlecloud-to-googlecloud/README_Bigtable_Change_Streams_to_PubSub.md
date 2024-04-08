
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

* **pubSubTopic** : The Pub/Sub topic to publish changelog entry messages.
* **bigtableChangeStreamAppProfile** : The application profile is used to distinguish workload in Cloud Bigtable.
* **bigtableReadInstanceId** : The ID of the Cloud Bigtable instance that contains the table.
* **bigtableReadTableId** : The Cloud Bigtable table to read from.

### Optional parameters

* **messageEncoding** : The format of the message to be written into PubSub. Allowed formats are BINARY and JSON. If topic has no encoding configured, default value is JSON.
* **messageFormat** : The message format chosen for outputting data to PubSub. Allowed formats are AVRO, PROTOCOL_BUFFERS and JSON. If topic has no schema configured, defaults to JSON.
* **stripValues** : Strip values for SetCell mutation. If true the SetCell mutation message won’t include the values written. Defaults to: false.
* **dlqDirectory** : The file path to store any unprocessed records with the reason they failed to be processed. Default is a directory under the Dataflow job's temp location. The default value is enough under most conditions.
* **dlqRetryMinutes** : The number of minutes between dead letter queue retries. Defaults to 10.
* **dlqMaxRetries** : The number of attempts to process change stream mutations. Defaults to 5.
* **useBase64Rowkeys** : Only supported for the JSON messageFormat. When set to true, row keys will be written as Base64-encoded strings. Otherwise bigtableChangeStreamCharset charset will be used to decode binary values into String row keysDefaults to false.
* **pubSubProjectId** : The PubSub Project. Default is the project for the Dataflow job.
* **useBase64ColumnQualifiers** : Only supported for the JSON messageFormat. When set to true, column qualifiers will be written as Base64-encoded strings. Otherwise bigtableChangeStreamCharset charset will be used to decode binary values into String column qualifiersDefaults to false.
* **useBase64Values** : Only supported for the JSON messageFormat. When set to true, values will be written as Base64-encoded strings. Otherwise bigtableChangeStreamCharset charset will be used to decode binary values into String valuesDefaults to false.
* **disableDlqRetries** : Whether or not to disable retries for the DLQ. Defaults to: false.
* **bigtableChangeStreamMetadataInstanceId** : The Cloud Bigtable instance to use for the change streams connector metadata table. Defaults to empty.
* **bigtableChangeStreamMetadataTableTableId** : The Cloud Bigtable change streams connector metadata table ID to use. If not provided, a Cloud Bigtable change streams connector metadata table will automatically be created during the pipeline flow. Defaults to empty.
* **bigtableChangeStreamCharset** : Bigtable change streams charset name when reading values and column qualifiers. Default is UTF-8.
* **bigtableChangeStreamStartTimestamp** : The starting DateTime, inclusive, to use for reading change streams (https://tools.ietf.org/html/rfc3339). For example, 2022-05-05T07:59:59Z. Defaults to the timestamp when the pipeline starts.
* **bigtableChangeStreamIgnoreColumnFamilies** : A comma-separated list of column family names changes to which won't be captured. Defaults to empty.
* **bigtableChangeStreamIgnoreColumns** : A comma-separated list of column names changes to which won't be captured. Defaults to empty.
* **bigtableChangeStreamName** : Allows to resume processing from the point where a previously running pipeline stopped.
* **bigtableChangeStreamResume** : When set to true< a new pipeline will resume processing from the point at which a previously running pipeline with the same bigtableChangeStreamName stopped. If pipeline with the given bigtableChangeStreamName never ran in the past, a new pipeline will fail to start. When set to false a new pipeline will be started. If pipeline with the same bigtableChangeStreamName already ran in the past for the given source, a new pipeline will fail to start. Defaults to false.
* **bigtableReadProjectId** : Project to read Cloud Bigtable data from. The default for this parameter is the project where the Dataflow pipeline is running.



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
