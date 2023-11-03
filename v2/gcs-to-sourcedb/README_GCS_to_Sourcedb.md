
GCS to Source DB template
---
Streaming pipeline. Reads Spanner change stream messages from GCS, orders them,
transforms them, and writes them to a Source Database like MySQL.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/gcs-to-sourcedb)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=GCS_to_Sourcedb).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required Parameters

* **sourceShardsFilePath** (Source shard details file path in Cloud Storage): Source shard details file path in Cloud Storage that contains connection profile of source shards.
* **sessionFilePath** (Session File Path in Cloud Storage): Session file path in Cloud Storage that contains mapping information from HarbourBridge.
* **GCSInputDirectoryPath** (GCS input directory path): Path from where to read the change stream files.
* **spannerProjectId** (Cloud Spanner Project Id.): This is the name of the Cloud Spanner project.
* **metadataInstance** (Cloud Spanner Instance to store the shard progress when reading from gcs): This is the instance to store the shard progress of the files processed.
* **metadataDatabase** (Cloud Spanner Database to store the shard progress when reading from gcs): This is the database to store  the shard progress of the files processed..

### Optional Parameters

* **sourceType** (Destination source type): This is the type of source databse.Currently only mysql is supported. Defaults to: mysql.
* **sourceDbTimezoneOffset** (SourceDB timezone offset): This is the timezone offset from UTC for the source database. Example value: +10:00. Defaults to: +00:00.
* **timerInterval** (Duration in seconds between calls to stateful timer processing. ): Controls the time between successive polls to buffer and processing of the resultant records. Defaults to: 1.
* **startTimestamp** (File start timestamp, takes precedence if provided, else value from spanner_to_gcs_metadata is considered, for regular mode.): Start time of file for all shards. If not provided, the value is taken from spanner_to_gcs_metadata. If provided, this takes precedence. To be given when running in regular run mode.
* **windowDuration** (File increment window duration,takes precedence if provided, else value from spanner_to_gcs_metadata is considered, for regular mode.): The window duration/size in which data is written to Cloud Storage. Allowed formats are: Ns (for seconds, example: 5s), Nm (for minutes, example: 12m), Nh (for hours, example: 2h). If not provided, the value is taken from spanner_to_gcs_metadata. If provided, this takes precedence. To be given when running in regular run mode. (Example: 5m).
* **runMode** (This type of run mode. Supported values - regular/reprocess.): Regular writes to source db, reprocess erred shards. Defaults to: regular.
* **metadataTableSuffix** (Metadata table suffix): Suffix appended to the spanner_to_gcs_metadata and shard_file_create_progress metadata tables.Useful when doing multiple runs.Only alpha numeric and underscores are allowed. Defaults to empty.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!
[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/gcs-to-sourcedb/src/main/java/com/google/cloud/teleport/v2/templates/GCSToSourceDb.java)

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
-DtemplateName="GCS_to_Sourcedb" \
-pl v2/gcs-to-sourcedb \
-am
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/GCS_to_Sourcedb
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/GCS_to_Sourcedb"

### Required
export SOURCE_SHARDS_FILE_PATH=<sourceShardsFilePath>
export SESSION_FILE_PATH=<sessionFilePath>
export GCSINPUT_DIRECTORY_PATH=<GCSInputDirectoryPath>
export SPANNER_PROJECT_ID=<spannerProjectId>
export METADATA_INSTANCE=<metadataInstance>
export METADATA_DATABASE=<metadataDatabase>

### Optional
export SOURCE_TYPE=mysql
export SOURCE_DB_TIMEZONE_OFFSET=+00:00
export TIMER_INTERVAL=1
export START_TIMESTAMP=<startTimestamp>
export WINDOW_DURATION=<windowDuration>
export RUN_MODE=regular
export METADATA_TABLE_SUFFIX=""

gcloud dataflow flex-template run "gcs-to-sourcedb-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "sourceShardsFilePath=$SOURCE_SHARDS_FILE_PATH" \
  --parameters "sessionFilePath=$SESSION_FILE_PATH" \
  --parameters "sourceType=$SOURCE_TYPE" \
  --parameters "sourceDbTimezoneOffset=$SOURCE_DB_TIMEZONE_OFFSET" \
  --parameters "timerInterval=$TIMER_INTERVAL" \
  --parameters "startTimestamp=$START_TIMESTAMP" \
  --parameters "windowDuration=$WINDOW_DURATION" \
  --parameters "GCSInputDirectoryPath=$GCSINPUT_DIRECTORY_PATH" \
  --parameters "spannerProjectId=$SPANNER_PROJECT_ID" \
  --parameters "metadataInstance=$METADATA_INSTANCE" \
  --parameters "metadataDatabase=$METADATA_DATABASE" \
  --parameters "runMode=$RUN_MODE" \
  --parameters "metadataTableSuffix=$METADATA_TABLE_SUFFIX"
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
export GCSINPUT_DIRECTORY_PATH=<GCSInputDirectoryPath>
export SPANNER_PROJECT_ID=<spannerProjectId>
export METADATA_INSTANCE=<metadataInstance>
export METADATA_DATABASE=<metadataDatabase>

### Optional
export SOURCE_TYPE=mysql
export SOURCE_DB_TIMEZONE_OFFSET=+00:00
export TIMER_INTERVAL=1
export START_TIMESTAMP=<startTimestamp>
export WINDOW_DURATION=<windowDuration>
export RUN_MODE=regular
export METADATA_TABLE_SUFFIX=""

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="gcs-to-sourcedb-job" \
-DtemplateName="GCS_to_Sourcedb" \
-Dparameters="sourceShardsFilePath=$SOURCE_SHARDS_FILE_PATH,sessionFilePath=$SESSION_FILE_PATH,sourceType=$SOURCE_TYPE,sourceDbTimezoneOffset=$SOURCE_DB_TIMEZONE_OFFSET,timerInterval=$TIMER_INTERVAL,startTimestamp=$START_TIMESTAMP,windowDuration=$WINDOW_DURATION,GCSInputDirectoryPath=$GCSINPUT_DIRECTORY_PATH,spannerProjectId=$SPANNER_PROJECT_ID,metadataInstance=$METADATA_INSTANCE,metadataDatabase=$METADATA_DATABASE,runMode=$RUN_MODE,metadataTableSuffix=$METADATA_TABLE_SUFFIX" \
-pl v2/gcs-to-sourcedb \
-am
```
