
Cloud Spanner change streams to Cloud Storage template
---
The Cloud Spanner change streams to Cloud Storage template is a streaming
pipeline that streams Spanner data change records and writes them into a Cloud
Storage bucket using Dataflow Runner V2.

The pipeline groups Spanner change stream records into windows based on their
timestamp, with each window representing a time duration whose length you can
configure with this template. All records with timestamps belonging to the window
are guaranteed to be in the window; there can be no late arrivals. You can also
define a number of output shards; the pipeline creates one Cloud Storage output
file per window per shard. Within an output file, records are unordered. Output
files can be written in either JSON or AVRO format, depending on the user
configuration.

Note that you can minimize network latency and network transport costs by running
the Dataflow job from the same region as your Cloud Spanner instance or Cloud
Storage bucket. If you use sources, sinks, staging file locations, or temporary
file locations that are located outside of your job's region, your data might be
sent across regions. See more about <a
href="https://cloud.google.com/dataflow/docs/concepts/regional-endpoints">Dataflow
regional endpoints</a>.

Learn more about <a
href="https://cloud.google.com/spanner/docs/change-streams">change streams</a>,
<a href="https://cloud.google.com/spanner/docs/change-streams/use-dataflow">how
to build change streams Dataflow pipelines</a>, and <a
href="https://cloud.google.com/spanner/docs/change-streams/use-dataflow#best_practices">best
practices</a>.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/cloud-spanner-change-streams-to-cloud-storage)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Spanner_Change_Streams_to_Google_Cloud_Storage).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **spannerInstanceId**: The Spanner instance ID to read change streams data from.
* **spannerDatabase**: The Spanner database to read change streams data from.
* **spannerMetadataInstanceId**: The Spanner instance ID to use for the change streams connector metadata table.
* **spannerMetadataDatabase**: The Spanner database to use for the change streams connector metadata table.
* **spannerChangeStreamName**: The name of the Spanner change stream to read from.
* **gcsOutputDirectory**: The path and filename prefix for writing output files. Must end with a slash. DateTime formatting is used to parse directory path for date & time formatters. For example, `gs://your-bucket/your-path`.

### Optional parameters

* **spannerProjectId**: The ID of the Google Cloud project that contains the Spanner database to read change streams from. This project is also where the change streams connector metadata table is created. The default for this parameter is the project where the Dataflow pipeline is running.
* **spannerDatabaseRole**: The Spanner database role to use when running the template. This parameter is required only when the IAM principal who is running the template is a fine-grained access control user. The database role must have the `SELECT` privilege on the change stream and the `EXECUTE` privilege on the change stream's read function. For more information, see Fine-grained access control for change streams (https://cloud.google.com/spanner/docs/fgac-change-streams).
* **spannerMetadataTableName**: The Spanner change streams connector metadata table name to use. If not provided, a Spanner change streams metadata table is automatically created during pipeline execution. You must provide a value for this parameter when updating an existing pipeline. Otherwise, don't use this parameter.
* **startTimestamp**: The starting DateTime, inclusive, to use for reading change streams, in the format `Ex-2021-10-12T07:20:50.52Z`. Defaults to the timestamp when the pipeline starts, that is, the current time.
* **endTimestamp**: The ending DateTime, inclusive, to use for reading change streams. For example, `Ex-2021-10-12T07:20:50.52Z`. Defaults to an infinite time in the future.
* **spannerHost**: The Cloud Spanner endpoint to call in the template. Only used for testing. For example, `https://spanner.googleapis.com`. Defaults to: https://spanner.googleapis.com.
* **outputFileFormat**: The format of the output Cloud Storage file. Allowed formats are `TEXT` and `AVRO`. Defaults to `AVRO`.
* **windowDuration**: The window duration is the interval in which data is written to the output directory. Configure the duration based on the pipeline's throughput. For example, a higher throughput might require smaller window sizes so that the data fits into memory. Defaults to 5m (five minutes), with a minimum of 1s (one second). Allowed formats are: [int]s (for seconds, example: 5s), [int]m (for minutes, example: 12m), [int]h (for hours, example: 2h). For example, `5m`.
* **rpcPriority**: The request priority for Spanner calls. The value must be `HIGH`, `MEDIUM`, or `LOW`. Defaults to `HIGH`.
* **outputFilenamePrefix**: The prefix to place on each windowed file. For example, `output-`. Defaults to: output.
* **numShards**: The maximum number of output shards produced when writing. A higher number of shards means higher throughput for writing to Cloud Storage, but potentially higher data aggregation cost across shards when processing output Cloud Storage files. Defaults to: 20.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/googlecloud-to-googlecloud/src/main/java/com/google/cloud/teleport/v2/templates/SpannerChangeStreamsToGcs.java)

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
-DtemplateName="Spanner_Change_Streams_to_Google_Cloud_Storage" \
-f v2/googlecloud-to-googlecloud
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Spanner_Change_Streams_to_Google_Cloud_Storage
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Spanner_Change_Streams_to_Google_Cloud_Storage"

### Required
export SPANNER_INSTANCE_ID=<spannerInstanceId>
export SPANNER_DATABASE=<spannerDatabase>
export SPANNER_METADATA_INSTANCE_ID=<spannerMetadataInstanceId>
export SPANNER_METADATA_DATABASE=<spannerMetadataDatabase>
export SPANNER_CHANGE_STREAM_NAME=<spannerChangeStreamName>
export GCS_OUTPUT_DIRECTORY=<gcsOutputDirectory>

### Optional
export SPANNER_PROJECT_ID=""
export SPANNER_DATABASE_ROLE=<spannerDatabaseRole>
export SPANNER_METADATA_TABLE_NAME=<spannerMetadataTableName>
export START_TIMESTAMP=""
export END_TIMESTAMP=""
export SPANNER_HOST=https://spanner.googleapis.com
export OUTPUT_FILE_FORMAT=AVRO
export WINDOW_DURATION=5m
export RPC_PRIORITY=HIGH
export OUTPUT_FILENAME_PREFIX=output
export NUM_SHARDS=20

gcloud dataflow flex-template run "spanner-change-streams-to-google-cloud-storage-job" \
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
  --parameters "outputFileFormat=$OUTPUT_FILE_FORMAT" \
  --parameters "windowDuration=$WINDOW_DURATION" \
  --parameters "rpcPriority=$RPC_PRIORITY" \
  --parameters "gcsOutputDirectory=$GCS_OUTPUT_DIRECTORY" \
  --parameters "outputFilenamePrefix=$OUTPUT_FILENAME_PREFIX" \
  --parameters "numShards=$NUM_SHARDS"
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
export GCS_OUTPUT_DIRECTORY=<gcsOutputDirectory>

### Optional
export SPANNER_PROJECT_ID=""
export SPANNER_DATABASE_ROLE=<spannerDatabaseRole>
export SPANNER_METADATA_TABLE_NAME=<spannerMetadataTableName>
export START_TIMESTAMP=""
export END_TIMESTAMP=""
export SPANNER_HOST=https://spanner.googleapis.com
export OUTPUT_FILE_FORMAT=AVRO
export WINDOW_DURATION=5m
export RPC_PRIORITY=HIGH
export OUTPUT_FILENAME_PREFIX=output
export NUM_SHARDS=20

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="spanner-change-streams-to-google-cloud-storage-job" \
-DtemplateName="Spanner_Change_Streams_to_Google_Cloud_Storage" \
-Dparameters="spannerProjectId=$SPANNER_PROJECT_ID,spannerInstanceId=$SPANNER_INSTANCE_ID,spannerDatabase=$SPANNER_DATABASE,spannerDatabaseRole=$SPANNER_DATABASE_ROLE,spannerMetadataInstanceId=$SPANNER_METADATA_INSTANCE_ID,spannerMetadataDatabase=$SPANNER_METADATA_DATABASE,spannerMetadataTableName=$SPANNER_METADATA_TABLE_NAME,spannerChangeStreamName=$SPANNER_CHANGE_STREAM_NAME,startTimestamp=$START_TIMESTAMP,endTimestamp=$END_TIMESTAMP,spannerHost=$SPANNER_HOST,outputFileFormat=$OUTPUT_FILE_FORMAT,windowDuration=$WINDOW_DURATION,rpcPriority=$RPC_PRIORITY,gcsOutputDirectory=$GCS_OUTPUT_DIRECTORY,outputFilenamePrefix=$OUTPUT_FILENAME_PREFIX,numShards=$NUM_SHARDS" \
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
cd v2/googlecloud-to-googlecloud/terraform/Spanner_Change_Streams_to_Google_Cloud_Storage
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

resource "google_dataflow_flex_template_job" "spanner_change_streams_to_google_cloud_storage" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Spanner_Change_Streams_to_Google_Cloud_Storage"
  name              = "spanner-change-streams-to-google-cloud-storage"
  region            = var.region
  parameters        = {
    spannerInstanceId = "<spannerInstanceId>"
    spannerDatabase = "<spannerDatabase>"
    spannerMetadataInstanceId = "<spannerMetadataInstanceId>"
    spannerMetadataDatabase = "<spannerMetadataDatabase>"
    spannerChangeStreamName = "<spannerChangeStreamName>"
    gcsOutputDirectory = "<gcsOutputDirectory>"
    # spannerProjectId = ""
    # spannerDatabaseRole = "<spannerDatabaseRole>"
    # spannerMetadataTableName = "<spannerMetadataTableName>"
    # startTimestamp = ""
    # endTimestamp = ""
    # spannerHost = "https://spanner.googleapis.com"
    # outputFileFormat = "AVRO"
    # windowDuration = "5m"
    # rpcPriority = "HIGH"
    # outputFilenamePrefix = "output"
    # numShards = "20"
  }
}
```
