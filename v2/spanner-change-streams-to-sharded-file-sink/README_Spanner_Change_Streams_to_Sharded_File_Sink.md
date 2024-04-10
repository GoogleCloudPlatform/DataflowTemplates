
Spanner Change Streams to Sharded File Sink template
---
Streaming pipeline. Ingests data from Spanner Change Streams, splits them into
shards and intervals , and writes them to a file sink.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/spanner-change-streams-to-sharded-file-sink)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Spanner_Change_Streams_to_Sharded_File_Sink).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **changeStreamName** : This is the name of the Spanner change stream that the pipeline will read from.
* **instanceId** : This is the name of the Cloud Spanner instance where the changestream is present.
* **databaseId** : This is the name of the Cloud Spanner database that the changestream is monitoring.
* **spannerProjectId** : This is the name of the Cloud Spanner project.
* **metadataInstance** : This is the instance to store the metadata used by the connector to control the consumption of the change stream API data.
* **metadataDatabase** : This is the database to store the metadata used by the connector to control the consumption of the change stream API data.
* **gcsOutputDirectory** : The path and filename prefix for writing output files. Must end with a slash. DateTime formatting is used to parse directory path for date & time formatters. (Example: gs://your-bucket/your-path/).
* **sourceShardsFilePath** : Source shard details file path in Cloud Storage that contains connection profile of source shards. Atleast one shard information is expected.
* **runIdentifier** : The identifier to distinguish between different runs of reverse replication flows.

### Optional parameters

* **startTimestamp** : Read changes from the given timestamp. Defaults to empty.
* **endTimestamp** : Read changes until the given timestamp. If no timestamp provided, reads indefinitely. Defaults to empty.
* **sessionFilePath** : Session file path in Cloud Storage that contains mapping information from HarbourBridge. Needed when doing sharded reverse replication.
* **windowDuration** : The window duration/size in which data will be written to Cloud Storage. Allowed formats are: Ns (for seconds, example: 5s), Nm (for minutes, example: 12m), Nh (for hours, example: 2h). (Example: 5m). Defaults to: 10s.
* **filtrationMode** : Mode of Filtration, decides how to drop certain records based on a criteria. Currently supported modes are: none (filter nothing), forward_migration (filter records written via the forward migration pipeline). Defaults to forward_migration.
* **metadataTableSuffix** : Suffix appended to the spanner_to_gcs_metadata and shard_file_create_progress metadata tables.Useful when doing multiple runs.Only alpha numeric and underscores are allowed. Defaults to empty.
* **skipDirectoryName** : Records skipped from reverse replication are written to this directory. Default directory name is skip.
* **runMode** : Regular starts from input start time, resume start from last processed time. Defaults to: regular.
* **shardingCustomJarPath** : Custom jar location in Cloud Storage that contains the customization logic for fetching shard id. Defaults to empty.
* **shardingCustomClassName** : Fully qualified class name having the custom shard id implementation.  It is a mandatory field in case shardingCustomJarPath is specified. Defaults to empty.
* **shardingCustomParameters** : String containing any custom parameters to be passed to the custom sharding class. Defaults to empty.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/spanner-change-streams-to-sharded-file-sink/src/main/java/com/google/cloud/teleport/v2/templates/SpannerChangeStreamsToShardedFileSink.java)

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
-DtemplateName="Spanner_Change_Streams_to_Sharded_File_Sink" \
-f v2/spanner-change-streams-to-sharded-file-sink
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Spanner_Change_Streams_to_Sharded_File_Sink
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Spanner_Change_Streams_to_Sharded_File_Sink"

### Required
export CHANGE_STREAM_NAME=<changeStreamName>
export INSTANCE_ID=<instanceId>
export DATABASE_ID=<databaseId>
export SPANNER_PROJECT_ID=<spannerProjectId>
export METADATA_INSTANCE=<metadataInstance>
export METADATA_DATABASE=<metadataDatabase>
export GCS_OUTPUT_DIRECTORY=<gcsOutputDirectory>
export SOURCE_SHARDS_FILE_PATH=<sourceShardsFilePath>
export RUN_IDENTIFIER=<runIdentifier>

### Optional
export START_TIMESTAMP=""
export END_TIMESTAMP=""
export SESSION_FILE_PATH=<sessionFilePath>
export WINDOW_DURATION=10s
export FILTRATION_MODE=forward_migration
export METADATA_TABLE_SUFFIX=""
export SKIP_DIRECTORY_NAME=skip
export RUN_MODE=regular
export SHARDING_CUSTOM_JAR_PATH=""
export SHARDING_CUSTOM_CLASS_NAME=""
export SHARDING_CUSTOM_PARAMETERS=""

gcloud dataflow flex-template run "spanner-change-streams-to-sharded-file-sink-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "changeStreamName=$CHANGE_STREAM_NAME" \
  --parameters "instanceId=$INSTANCE_ID" \
  --parameters "databaseId=$DATABASE_ID" \
  --parameters "spannerProjectId=$SPANNER_PROJECT_ID" \
  --parameters "metadataInstance=$METADATA_INSTANCE" \
  --parameters "metadataDatabase=$METADATA_DATABASE" \
  --parameters "startTimestamp=$START_TIMESTAMP" \
  --parameters "endTimestamp=$END_TIMESTAMP" \
  --parameters "sessionFilePath=$SESSION_FILE_PATH" \
  --parameters "windowDuration=$WINDOW_DURATION" \
  --parameters "gcsOutputDirectory=$GCS_OUTPUT_DIRECTORY" \
  --parameters "filtrationMode=$FILTRATION_MODE" \
  --parameters "sourceShardsFilePath=$SOURCE_SHARDS_FILE_PATH" \
  --parameters "metadataTableSuffix=$METADATA_TABLE_SUFFIX" \
  --parameters "skipDirectoryName=$SKIP_DIRECTORY_NAME" \
  --parameters "runIdentifier=$RUN_IDENTIFIER" \
  --parameters "runMode=$RUN_MODE" \
  --parameters "shardingCustomJarPath=$SHARDING_CUSTOM_JAR_PATH" \
  --parameters "shardingCustomClassName=$SHARDING_CUSTOM_CLASS_NAME" \
  --parameters "shardingCustomParameters=$SHARDING_CUSTOM_PARAMETERS"
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
export CHANGE_STREAM_NAME=<changeStreamName>
export INSTANCE_ID=<instanceId>
export DATABASE_ID=<databaseId>
export SPANNER_PROJECT_ID=<spannerProjectId>
export METADATA_INSTANCE=<metadataInstance>
export METADATA_DATABASE=<metadataDatabase>
export GCS_OUTPUT_DIRECTORY=<gcsOutputDirectory>
export SOURCE_SHARDS_FILE_PATH=<sourceShardsFilePath>
export RUN_IDENTIFIER=<runIdentifier>

### Optional
export START_TIMESTAMP=""
export END_TIMESTAMP=""
export SESSION_FILE_PATH=<sessionFilePath>
export WINDOW_DURATION=10s
export FILTRATION_MODE=forward_migration
export METADATA_TABLE_SUFFIX=""
export SKIP_DIRECTORY_NAME=skip
export RUN_MODE=regular
export SHARDING_CUSTOM_JAR_PATH=""
export SHARDING_CUSTOM_CLASS_NAME=""
export SHARDING_CUSTOM_PARAMETERS=""

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="spanner-change-streams-to-sharded-file-sink-job" \
-DtemplateName="Spanner_Change_Streams_to_Sharded_File_Sink" \
-Dparameters="changeStreamName=$CHANGE_STREAM_NAME,instanceId=$INSTANCE_ID,databaseId=$DATABASE_ID,spannerProjectId=$SPANNER_PROJECT_ID,metadataInstance=$METADATA_INSTANCE,metadataDatabase=$METADATA_DATABASE,startTimestamp=$START_TIMESTAMP,endTimestamp=$END_TIMESTAMP,sessionFilePath=$SESSION_FILE_PATH,windowDuration=$WINDOW_DURATION,gcsOutputDirectory=$GCS_OUTPUT_DIRECTORY,filtrationMode=$FILTRATION_MODE,sourceShardsFilePath=$SOURCE_SHARDS_FILE_PATH,metadataTableSuffix=$METADATA_TABLE_SUFFIX,skipDirectoryName=$SKIP_DIRECTORY_NAME,runIdentifier=$RUN_IDENTIFIER,runMode=$RUN_MODE,shardingCustomJarPath=$SHARDING_CUSTOM_JAR_PATH,shardingCustomClassName=$SHARDING_CUSTOM_CLASS_NAME,shardingCustomParameters=$SHARDING_CUSTOM_PARAMETERS" \
-f v2/spanner-change-streams-to-sharded-file-sink
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
cd v2/spanner-change-streams-to-sharded-file-sink/terraform/Spanner_Change_Streams_to_Sharded_File_Sink
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

resource "google_dataflow_flex_template_job" "spanner_change_streams_to_sharded_file_sink" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Spanner_Change_Streams_to_Sharded_File_Sink"
  name              = "spanner-change-streams-to-sharded-file-sink"
  region            = var.region
  parameters        = {
    changeStreamName = "<changeStreamName>"
    instanceId = "<instanceId>"
    databaseId = "<databaseId>"
    spannerProjectId = "<spannerProjectId>"
    metadataInstance = "<metadataInstance>"
    metadataDatabase = "<metadataDatabase>"
    gcsOutputDirectory = "gs://your-bucket/your-path/"
    sourceShardsFilePath = "<sourceShardsFilePath>"
    runIdentifier = "<runIdentifier>"
    # startTimestamp = ""
    # endTimestamp = ""
    # sessionFilePath = "<sessionFilePath>"
    # windowDuration = "5m"
    # filtrationMode = "forward_migration"
    # metadataTableSuffix = ""
    # skipDirectoryName = "skip"
    # runMode = "regular"
    # shardingCustomJarPath = ""
    # shardingCustomClassName = ""
    # shardingCustomParameters = ""
  }
}
```
