
Datastream to Cloud Spanner template
---
The Datastream to Cloud Spanner template is a streaming pipeline that reads <a
href="https://cloud.google.com/datastream/docs">Datastream</a> events from a
Cloud Storage bucket and writes them to a Cloud Spanner database. It is intended
for data migration from Datastream sources to Cloud Spanner.

All tables required for migration must exist in the destination Cloud Spanner
database prior to template execution. Hence schema migration from a source
database to destination Cloud Spanner must be completed prior to data migration.
Data can exist in the tables prior to migration. This template does not propagate
Datastream schema changes to the Cloud Spanner database.

Data consistency is guaranteed only at the end of migration when all data has
been written to Cloud Spanner. To store ordering information for each record
written to Cloud Spanner, this template creates an additional table (called a
shadow table) for each table in the Cloud Spanner database. This is used to
ensure consistency at the end of migration. The shadow tables are not deleted
after migration and can be used for validation purposes at the end of migration.

Any errors that occur during operation, such as schema mismatches, malformed JSON
files, or errors resulting from executing transforms, are recorded in an error
queue. The error queue is a Cloud Storage folder which stores all the Datastream
events that had encountered errors along with the error reason in text format.
The errors can be transient or permanent and are stored in appropriate Cloud
Storage folders in the error queue. The transient errors are retried
automatically while the permanent errors are not. In case of permanent errors,
you have the option of making corrections to the change events and moving them to
the retriable bucket while the template is running.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/datastream-to-cloud-spanner)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Cloud_Datastream_to_Spanner).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **instanceId**: The Spanner instance where the changes are replicated.
* **databaseId**: The Spanner database where the changes are replicated.

### Optional parameters

* **inputFilePattern**: The Cloud Storage file location that contains the Datastream files to replicate. Typically, this is the root path for a stream. Support for this feature has been disabled.
* **inputFileFormat**: The format of the output file produced by Datastream. For example `avro,json`. Defaults to `avro`.
* **sessionFilePath**: Session file path in Cloud Storage that contains mapping information from HarbourBridge.
* **projectId**: The Spanner project ID.
* **spannerHost**: The Cloud Spanner endpoint to call in the template. For example, `https://batch-spanner.googleapis.com`. Defaults to: https://batch-spanner.googleapis.com.
* **gcsPubSubSubscription**: The Pub/Sub subscription being used in a Cloud Storage notification policy. For the name, use the format `projects/<PROJECT_ID>/subscriptions/<SUBSCRIPTION_NAME>`.
* **streamName**: The name or template for the stream to poll for schema information and source type.
* **shadowTablePrefix**: The prefix used to name shadow tables. Default: `shadow_`.
* **shouldCreateShadowTables**: This flag indicates whether shadow tables must be created in Cloud Spanner database. Defaults to: true.
* **rfcStartDateTime**: The starting DateTime used to fetch from Cloud Storage (https://tools.ietf.org/html/rfc3339). Defaults to: 1970-01-01T00:00:00.00Z.
* **fileReadConcurrency**: The number of concurrent DataStream files to read. Defaults to: 30.
* **deadLetterQueueDirectory**: The file path used when storing the error queue output. The default file path is a directory under the Dataflow job's temp location.
* **dlqRetryMinutes**: The number of minutes between dead letter queue retries. Defaults to `10`.
* **dlqMaxRetryCount**: The max number of times temporary errors can be retried through DLQ. Defaults to `500`.
* **dataStreamRootUrl**: Datastream API Root URL. Defaults to: https://datastream.googleapis.com/.
* **datastreamSourceType**: This is the type of source database that Datastream connects to. Example - mysql/oracle. Need to be set when testing without an actual running Datastream.
* **roundJsonDecimals**: This flag if set, rounds the decimal values in json columns to a number that can be stored without loss of precision. Defaults to: false.
* **runMode**: This is the run mode type, whether regular or with retryDLQ. Defaults to: regular.
* **transformationContextFilePath**: Transformation context file path in cloud storage used to populate data used in transformations performed during migrations   Eg: The shard id to db name to identify the db from which a row was migrated.
* **directoryWatchDurationInMinutes**: The Duration for which the pipeline should keep polling a directory in GCS. Datastreamoutput files are arranged in a directory structure which depicts the timestamp of the event grouped by minutes. This parameter should be approximately equal tomaximum delay which could occur between event occurring in source database and the same event being written to GCS by Datastream. 99.9 percentile = 10 minutes. Defaults to: 10.
* **spannerPriority**: The request priority for Cloud Spanner calls. The value must be one of: [`HIGH`,`MEDIUM`,`LOW`]. Defaults to `HIGH`.
* **dlqGcsPubSubSubscription**: The Pub/Sub subscription being used in a Cloud Storage notification policy for DLQ retry directory when running in regular mode. For the name, use the format `projects/<PROJECT_ID>/subscriptions/<SUBSCRIPTION_NAME>`. When set, the deadLetterQueueDirectory and dlqRetryMinutes are ignored.
* **transformationJarPath**: Custom JAR file location in Cloud Storage for the file that contains the custom transformation logic for processing records in forward migration. Defaults to empty.
* **transformationClassName**: Fully qualified class name having the custom transformation logic.  It is a mandatory field in case transformationJarPath is specified. Defaults to empty.
* **transformationCustomParameters**: String containing any custom parameters to be passed to the custom transformation class. Defaults to empty.
* **filteredEventsDirectory**: This is the file path to store the events filtered via custom transformation. Default is a directory under the Dataflow job's temp location. The default value is enough under most conditions.
* **shardingContextFilePath**: Sharding context file path in cloud storage is used to populate the shard id in spanner database for each source shard.It is of the format Map<stream_name, Map<db_name, shard_id>>.
* **tableOverrides**: These are the table name overrides from source to spanner. They are written in thefollowing format: [{SourceTableName1, SpannerTableName1}, {SourceTableName2, SpannerTableName2}]This example shows mapping Singers table to Vocalists and Albums table to Records. For example, `[{Singers, Vocalists}, {Albums, Records}]`. Defaults to empty.
* **columnOverrides**: These are the column name overrides from source to spanner. They are written in thefollowing format: [{SourceTableName1.SourceColumnName1, SourceTableName1.SpannerColumnName1}, {SourceTableName2.SourceColumnName1, SourceTableName2.SpannerColumnName1}]Note that the SourceTableName should remain the same in both the source and spanner pair. To override table names, use tableOverrides.The example shows mapping SingerName to TalentName and AlbumName to RecordName in Singers and Albums table respectively. For example, `[{Singers.SingerName, Singers.TalentName}, {Albums.AlbumName, Albums.RecordName}]`. Defaults to empty.
* **schemaOverridesFilePath**: A file which specifies the table and the column name overrides from source to spanner. Defaults to empty.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/sourcedb-to-spanner/src/main/java/com/google/cloud/teleport/v2/templates/DataStreamToSpanner.java)

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
-DtemplateName="Cloud_Datastream_to_Spanner" \
-f v2/datastream-to-spanner
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Cloud_Datastream_to_Spanner
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Cloud_Datastream_to_Spanner"

### Required
export INSTANCE_ID=<instanceId>
export DATABASE_ID=<databaseId>

### Optional
export INPUT_FILE_PATTERN=<inputFilePattern>
export INPUT_FILE_FORMAT=avro
export SESSION_FILE_PATH=<sessionFilePath>
export PROJECT_ID=<projectId>
export SPANNER_HOST=https://batch-spanner.googleapis.com
export GCS_PUB_SUB_SUBSCRIPTION=<gcsPubSubSubscription>
export STREAM_NAME=<streamName>
export SHADOW_TABLE_PREFIX=shadow_
export SHOULD_CREATE_SHADOW_TABLES=true
export RFC_START_DATE_TIME=1970-01-01T00:00:00.00Z
export FILE_READ_CONCURRENCY=30
export DEAD_LETTER_QUEUE_DIRECTORY=""
export DLQ_RETRY_MINUTES=10
export DLQ_MAX_RETRY_COUNT=500
export DATA_STREAM_ROOT_URL=https://datastream.googleapis.com/
export DATASTREAM_SOURCE_TYPE=<datastreamSourceType>
export ROUND_JSON_DECIMALS=false
export RUN_MODE=regular
export TRANSFORMATION_CONTEXT_FILE_PATH=<transformationContextFilePath>
export DIRECTORY_WATCH_DURATION_IN_MINUTES=10
export SPANNER_PRIORITY=HIGH
export DLQ_GCS_PUB_SUB_SUBSCRIPTION=<dlqGcsPubSubSubscription>
export TRANSFORMATION_JAR_PATH=""
export TRANSFORMATION_CLASS_NAME=""
export TRANSFORMATION_CUSTOM_PARAMETERS=""
export FILTERED_EVENTS_DIRECTORY=""
export SHARDING_CONTEXT_FILE_PATH=<shardingContextFilePath>
export TABLE_OVERRIDES=""
export COLUMN_OVERRIDES=""
export SCHEMA_OVERRIDES_FILE_PATH=""

gcloud dataflow flex-template run "cloud-datastream-to-spanner-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "inputFilePattern=$INPUT_FILE_PATTERN" \
  --parameters "inputFileFormat=$INPUT_FILE_FORMAT" \
  --parameters "sessionFilePath=$SESSION_FILE_PATH" \
  --parameters "instanceId=$INSTANCE_ID" \
  --parameters "databaseId=$DATABASE_ID" \
  --parameters "projectId=$PROJECT_ID" \
  --parameters "spannerHost=$SPANNER_HOST" \
  --parameters "gcsPubSubSubscription=$GCS_PUB_SUB_SUBSCRIPTION" \
  --parameters "streamName=$STREAM_NAME" \
  --parameters "shadowTablePrefix=$SHADOW_TABLE_PREFIX" \
  --parameters "shouldCreateShadowTables=$SHOULD_CREATE_SHADOW_TABLES" \
  --parameters "rfcStartDateTime=$RFC_START_DATE_TIME" \
  --parameters "fileReadConcurrency=$FILE_READ_CONCURRENCY" \
  --parameters "deadLetterQueueDirectory=$DEAD_LETTER_QUEUE_DIRECTORY" \
  --parameters "dlqRetryMinutes=$DLQ_RETRY_MINUTES" \
  --parameters "dlqMaxRetryCount=$DLQ_MAX_RETRY_COUNT" \
  --parameters "dataStreamRootUrl=$DATA_STREAM_ROOT_URL" \
  --parameters "datastreamSourceType=$DATASTREAM_SOURCE_TYPE" \
  --parameters "roundJsonDecimals=$ROUND_JSON_DECIMALS" \
  --parameters "runMode=$RUN_MODE" \
  --parameters "transformationContextFilePath=$TRANSFORMATION_CONTEXT_FILE_PATH" \
  --parameters "directoryWatchDurationInMinutes=$DIRECTORY_WATCH_DURATION_IN_MINUTES" \
  --parameters "spannerPriority=$SPANNER_PRIORITY" \
  --parameters "dlqGcsPubSubSubscription=$DLQ_GCS_PUB_SUB_SUBSCRIPTION" \
  --parameters "transformationJarPath=$TRANSFORMATION_JAR_PATH" \
  --parameters "transformationClassName=$TRANSFORMATION_CLASS_NAME" \
  --parameters "transformationCustomParameters=$TRANSFORMATION_CUSTOM_PARAMETERS" \
  --parameters "filteredEventsDirectory=$FILTERED_EVENTS_DIRECTORY" \
  --parameters "shardingContextFilePath=$SHARDING_CONTEXT_FILE_PATH" \
  --parameters "tableOverrides=$TABLE_OVERRIDES" \
  --parameters "columnOverrides=$COLUMN_OVERRIDES" \
  --parameters "schemaOverridesFilePath=$SCHEMA_OVERRIDES_FILE_PATH"
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
export INSTANCE_ID=<instanceId>
export DATABASE_ID=<databaseId>

### Optional
export INPUT_FILE_PATTERN=<inputFilePattern>
export INPUT_FILE_FORMAT=avro
export SESSION_FILE_PATH=<sessionFilePath>
export PROJECT_ID=<projectId>
export SPANNER_HOST=https://batch-spanner.googleapis.com
export GCS_PUB_SUB_SUBSCRIPTION=<gcsPubSubSubscription>
export STREAM_NAME=<streamName>
export SHADOW_TABLE_PREFIX=shadow_
export SHOULD_CREATE_SHADOW_TABLES=true
export RFC_START_DATE_TIME=1970-01-01T00:00:00.00Z
export FILE_READ_CONCURRENCY=30
export DEAD_LETTER_QUEUE_DIRECTORY=""
export DLQ_RETRY_MINUTES=10
export DLQ_MAX_RETRY_COUNT=500
export DATA_STREAM_ROOT_URL=https://datastream.googleapis.com/
export DATASTREAM_SOURCE_TYPE=<datastreamSourceType>
export ROUND_JSON_DECIMALS=false
export RUN_MODE=regular
export TRANSFORMATION_CONTEXT_FILE_PATH=<transformationContextFilePath>
export DIRECTORY_WATCH_DURATION_IN_MINUTES=10
export SPANNER_PRIORITY=HIGH
export DLQ_GCS_PUB_SUB_SUBSCRIPTION=<dlqGcsPubSubSubscription>
export TRANSFORMATION_JAR_PATH=""
export TRANSFORMATION_CLASS_NAME=""
export TRANSFORMATION_CUSTOM_PARAMETERS=""
export FILTERED_EVENTS_DIRECTORY=""
export SHARDING_CONTEXT_FILE_PATH=<shardingContextFilePath>
export TABLE_OVERRIDES=""
export COLUMN_OVERRIDES=""
export SCHEMA_OVERRIDES_FILE_PATH=""

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="cloud-datastream-to-spanner-job" \
-DtemplateName="Cloud_Datastream_to_Spanner" \
-Dparameters="inputFilePattern=$INPUT_FILE_PATTERN,inputFileFormat=$INPUT_FILE_FORMAT,sessionFilePath=$SESSION_FILE_PATH,instanceId=$INSTANCE_ID,databaseId=$DATABASE_ID,projectId=$PROJECT_ID,spannerHost=$SPANNER_HOST,gcsPubSubSubscription=$GCS_PUB_SUB_SUBSCRIPTION,streamName=$STREAM_NAME,shadowTablePrefix=$SHADOW_TABLE_PREFIX,shouldCreateShadowTables=$SHOULD_CREATE_SHADOW_TABLES,rfcStartDateTime=$RFC_START_DATE_TIME,fileReadConcurrency=$FILE_READ_CONCURRENCY,deadLetterQueueDirectory=$DEAD_LETTER_QUEUE_DIRECTORY,dlqRetryMinutes=$DLQ_RETRY_MINUTES,dlqMaxRetryCount=$DLQ_MAX_RETRY_COUNT,dataStreamRootUrl=$DATA_STREAM_ROOT_URL,datastreamSourceType=$DATASTREAM_SOURCE_TYPE,roundJsonDecimals=$ROUND_JSON_DECIMALS,runMode=$RUN_MODE,transformationContextFilePath=$TRANSFORMATION_CONTEXT_FILE_PATH,directoryWatchDurationInMinutes=$DIRECTORY_WATCH_DURATION_IN_MINUTES,spannerPriority=$SPANNER_PRIORITY,dlqGcsPubSubSubscription=$DLQ_GCS_PUB_SUB_SUBSCRIPTION,transformationJarPath=$TRANSFORMATION_JAR_PATH,transformationClassName=$TRANSFORMATION_CLASS_NAME,transformationCustomParameters=$TRANSFORMATION_CUSTOM_PARAMETERS,filteredEventsDirectory=$FILTERED_EVENTS_DIRECTORY,shardingContextFilePath=$SHARDING_CONTEXT_FILE_PATH,tableOverrides=$TABLE_OVERRIDES,columnOverrides=$COLUMN_OVERRIDES,schemaOverridesFilePath=$SCHEMA_OVERRIDES_FILE_PATH" \
-f v2/datastream-to-spanner
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
cd v2/datastream-to-spanner/terraform/Cloud_Datastream_to_Spanner
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

resource "google_dataflow_flex_template_job" "cloud_datastream_to_spanner" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Cloud_Datastream_to_Spanner"
  name              = "cloud-datastream-to-spanner"
  region            = var.region
  parameters        = {
    instanceId = "<instanceId>"
    databaseId = "<databaseId>"
    # inputFilePattern = "<inputFilePattern>"
    # inputFileFormat = "avro"
    # sessionFilePath = "<sessionFilePath>"
    # projectId = "<projectId>"
    # spannerHost = "https://batch-spanner.googleapis.com"
    # gcsPubSubSubscription = "<gcsPubSubSubscription>"
    # streamName = "<streamName>"
    # shadowTablePrefix = "shadow_"
    # shouldCreateShadowTables = "true"
    # rfcStartDateTime = "1970-01-01T00:00:00.00Z"
    # fileReadConcurrency = "30"
    # deadLetterQueueDirectory = ""
    # dlqRetryMinutes = "10"
    # dlqMaxRetryCount = "500"
    # dataStreamRootUrl = "https://datastream.googleapis.com/"
    # datastreamSourceType = "<datastreamSourceType>"
    # roundJsonDecimals = "false"
    # runMode = "regular"
    # transformationContextFilePath = "<transformationContextFilePath>"
    # directoryWatchDurationInMinutes = "10"
    # spannerPriority = "HIGH"
    # dlqGcsPubSubSubscription = "<dlqGcsPubSubSubscription>"
    # transformationJarPath = ""
    # transformationClassName = ""
    # transformationCustomParameters = ""
    # filteredEventsDirectory = ""
    # shardingContextFilePath = "<shardingContextFilePath>"
    # tableOverrides = ""
    # columnOverrides = ""
    # schemaOverridesFilePath = ""
  }
}
```
