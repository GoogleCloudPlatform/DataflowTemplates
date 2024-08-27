
Cloud Spanner change streams to Pub/Sub template
---
The Cloud Spanner change streams to the Pub/Sub template is a streaming pipeline
that streams Cloud Spanner data change records and writes them into Pub/Sub
topics using Dataflow Runner V2.

To output your data to a new Pub/Sub topic, you need to first create the topic.
After creation, Pub/Sub automatically generates and attaches a subscription to
the new topic. If you try to output data to a Pub/Sub topic that doesn't exist,
the dataflow pipeline throws an exception, and the pipeline gets stuck as it
continuously tries to make a connection.

If the necessary Pub/Sub topic already exists, you can output data to that topic.

Learn more about <a
href="https://cloud.google.com/spanner/docs/change-streams">change streams</a>,
<a href="https://cloud.google.com/spanner/docs/change-streams/use-dataflow">how
to build change streams Dataflow pipelines</a>, and <a
href="https://cloud.google.com/spanner/docs/change-streams/use-dataflow#best_practices">best
practices</a>.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/cloud-spanner-change-streams-to-pubsub)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Spanner_Change_Streams_to_PubSub).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **spannerInstanceId** : The Spanner instance to read change streams from.
* **spannerDatabase** : The Spanner database to read change streams from.
* **spannerMetadataInstanceId** : The Spanner instance to use for the change streams connector metadata table.
* **spannerMetadataDatabase** : The Spanner database to use for the change streams connector metadata table.
* **spannerChangeStreamName** : The name of the Spanner change stream to read from.
* **pubsubTopic** : The Pub/Sub topic for change streams output.

### Optional parameters

* **spannerProjectId** : The project to read change streams from. This project is also where the change streams connector metadata table is created. The default for this parameter is the project where the Dataflow pipeline is running.
* **spannerDatabaseRole** : The Spanner database role to use when running the template. This parameter is required only when the IAM principal who is running the template is a fine-grained access control user. The database role must have the `SELECT` privilege on the change stream and the `EXECUTE` privilege on the change stream's read function. For more information, see Fine-grained access control for change streams (https://cloud.google.com/spanner/docs/fgac-change-streams).
* **spannerMetadataTableName** : The Spanner change streams connector metadata table name to use. If not provided, Spanner automatically creates the streams connector metadata table during the pipeline flow change. You must provide this parameter when updating an existing pipeline. Don't use this parameter for other cases.
* **startTimestamp** : The starting DateTime (https://tools.ietf.org/html/rfc3339), inclusive, to use for reading change streams. For example, ex- 2021-10-12T07:20:50.52Z. Defaults to the timestamp when the pipeline starts, that is, the current time.
* **endTimestamp** : The ending DateTime (https://tools.ietf.org/html/rfc3339), inclusive, to use for reading change streams. For example, ex- 2021-10-12T07:20:50.52Z. Defaults to an infinite time in the future.
* **spannerHost** : The Cloud Spanner endpoint to call in the template. Only used for testing. (Example: https://spanner.googleapis.com). Defaults to: https://spanner.googleapis.com.
* **outputDataFormat** : The format of the output. Output is wrapped in many PubsubMessages and sent to a Pub/Sub topic. Allowed formats are JSON and AVRO. Default is JSON.
* **pubsubAPI** : The Pub/Sub API used to implement the pipeline. Allowed APIs are `pubsubio` and `native_client`. For a small number of queries per second (QPS), `native_client` has less latency. For a large number of QPS, `pubsubio` provides better and more stable performance. The default is `pubsubio`.
* **pubsubProjectId** : Project of Pub/Sub topic. The default for this parameter is the project where the Dataflow pipeline is running.
* **rpcPriority** : The request priority for Spanner calls. Allowed values are HIGH, MEDIUM, and LOW. Defaults to: HIGH).



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/googlecloud-to-googlecloud/src/main/java/com/google/cloud/teleport/v2/templates/SpannerChangeStreamsToPubSub.java)

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
-DtemplateName="Spanner_Change_Streams_to_PubSub" \
-f v2/googlecloud-to-googlecloud
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Spanner_Change_Streams_to_PubSub
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Spanner_Change_Streams_to_PubSub"

### Required
export SPANNER_INSTANCE_ID=<spannerInstanceId>
export SPANNER_DATABASE=<spannerDatabase>
export SPANNER_METADATA_INSTANCE_ID=<spannerMetadataInstanceId>
export SPANNER_METADATA_DATABASE=<spannerMetadataDatabase>
export SPANNER_CHANGE_STREAM_NAME=<spannerChangeStreamName>
export PUBSUB_TOPIC=<pubsubTopic>

### Optional
export SPANNER_PROJECT_ID=""
export SPANNER_DATABASE_ROLE=<spannerDatabaseRole>
export SPANNER_METADATA_TABLE_NAME=<spannerMetadataTableName>
export START_TIMESTAMP=""
export END_TIMESTAMP=""
export SPANNER_HOST=https://spanner.googleapis.com
export OUTPUT_DATA_FORMAT=JSON
export PUBSUB_API=pubsubio
export PUBSUB_PROJECT_ID=""
export RPC_PRIORITY=HIGH

gcloud dataflow flex-template run "spanner-change-streams-to-pubsub-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "spannerProjectId=$SPANNER_PROJECT_ID" \
  --parameters "spannerInstanceId=$SPANNER_INSTANCE_ID" \
  --parameters "spannerDatabase=$SPANNER_DATABASE" \
  --parameters "spannerDatabaseRole=$SPANNER_DATABASE_ROLE" \
  --parameters "spannerMetadataInstanceId=$SPANNER_METADATA_INSTANCE_ID" \
  --parameters "spannerMetadataDatabase=$SPANNER_METADATA_DATABASE" \
  --parameters "spannerMetadataTableName=$SPANNER_METADATA_TABLE_NAME" \
  --parameters "spannerChangeStreamName=$SPANNER_CHANGE_STREAM_NAME" \
  --parameters "startTimestamp=$START_TIMESTAMP" \
  --parameters "endTimestamp=$END_TIMESTAMP" \
  --parameters "spannerHost=$SPANNER_HOST" \
  --parameters "outputDataFormat=$OUTPUT_DATA_FORMAT" \
  --parameters "pubsubAPI=$PUBSUB_API" \
  --parameters "pubsubProjectId=$PUBSUB_PROJECT_ID" \
  --parameters "pubsubTopic=$PUBSUB_TOPIC" \
  --parameters "rpcPriority=$RPC_PRIORITY"
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
export SPANNER_INSTANCE_ID=<spannerInstanceId>
export SPANNER_DATABASE=<spannerDatabase>
export SPANNER_METADATA_INSTANCE_ID=<spannerMetadataInstanceId>
export SPANNER_METADATA_DATABASE=<spannerMetadataDatabase>
export SPANNER_CHANGE_STREAM_NAME=<spannerChangeStreamName>
export PUBSUB_TOPIC=<pubsubTopic>

### Optional
export SPANNER_PROJECT_ID=""
export SPANNER_DATABASE_ROLE=<spannerDatabaseRole>
export SPANNER_METADATA_TABLE_NAME=<spannerMetadataTableName>
export START_TIMESTAMP=""
export END_TIMESTAMP=""
export SPANNER_HOST=https://spanner.googleapis.com
export OUTPUT_DATA_FORMAT=JSON
export PUBSUB_API=pubsubio
export PUBSUB_PROJECT_ID=""
export RPC_PRIORITY=HIGH

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="spanner-change-streams-to-pubsub-job" \
-DtemplateName="Spanner_Change_Streams_to_PubSub" \
-Dparameters="spannerProjectId=$SPANNER_PROJECT_ID,spannerInstanceId=$SPANNER_INSTANCE_ID,spannerDatabase=$SPANNER_DATABASE,spannerDatabaseRole=$SPANNER_DATABASE_ROLE,spannerMetadataInstanceId=$SPANNER_METADATA_INSTANCE_ID,spannerMetadataDatabase=$SPANNER_METADATA_DATABASE,spannerMetadataTableName=$SPANNER_METADATA_TABLE_NAME,spannerChangeStreamName=$SPANNER_CHANGE_STREAM_NAME,startTimestamp=$START_TIMESTAMP,endTimestamp=$END_TIMESTAMP,spannerHost=$SPANNER_HOST,outputDataFormat=$OUTPUT_DATA_FORMAT,pubsubAPI=$PUBSUB_API,pubsubProjectId=$PUBSUB_PROJECT_ID,pubsubTopic=$PUBSUB_TOPIC,rpcPriority=$RPC_PRIORITY" \
-f v2/googlecloud-to-googlecloud
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
cd v2/googlecloud-to-googlecloud/terraform/Spanner_Change_Streams_to_PubSub
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

resource "google_dataflow_flex_template_job" "spanner_change_streams_to_pubsub" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Spanner_Change_Streams_to_PubSub"
  name              = "spanner-change-streams-to-pubsub"
  region            = var.region
  parameters        = {
    spannerInstanceId = "<spannerInstanceId>"
    spannerDatabase = "<spannerDatabase>"
    spannerMetadataInstanceId = "<spannerMetadataInstanceId>"
    spannerMetadataDatabase = "<spannerMetadataDatabase>"
    spannerChangeStreamName = "<spannerChangeStreamName>"
    pubsubTopic = "<pubsubTopic>"
    # spannerProjectId = ""
    # spannerDatabaseRole = "<spannerDatabaseRole>"
    # spannerMetadataTableName = "<spannerMetadataTableName>"
    # startTimestamp = ""
    # endTimestamp = ""
    # spannerHost = "https://spanner.googleapis.com"
    # outputDataFormat = "JSON"
    # pubsubAPI = "pubsubio"
    # pubsubProjectId = ""
    # rpcPriority = "HIGH"
  }
}
```
