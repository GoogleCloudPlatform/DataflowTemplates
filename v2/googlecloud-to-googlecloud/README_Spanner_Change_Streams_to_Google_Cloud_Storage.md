Cloud Spanner change streams to Cloud Storage Template
---
Streaming pipeline. Streams Spanner change stream data records and writes them into a Cloud Storage bucket using Dataflow Runner V2.

:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided-templates)
on how to use it without having to build from sources.

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Mandatory Parameters

* **spannerInstanceId** (Spanner instance ID): The Spanner instance to read change streams from.
* **spannerDatabase** (Spanner database): The Spanner database to read change streams from.
* **spannerMetadataInstanceId** (Spanner metadata instance ID): The Spanner instance to use for the change streams connector metadata table.
* **spannerMetadataDatabase** (Spanner metadata database): The Spanner database to use for the change streams connector metadata table. For change streams tracking all tables in a database, we recommend putting the metadata table in a separate database.
* **spannerChangeStreamName** (Spanner change stream): The name of the Spanner change stream to read from.
* **gcsOutputDirectory** (Output file directory in Cloud Storage): The path and filename prefix for writing output files. Must end with a slash. DateTime formatting is used to parse directory path for date & time formatters. (Example: gs://your-bucket/your-path).

### Optional Parameters

* **spannerProjectId** (Spanner Project ID): Project to read change streams from. The default for this parameter is the project where the Dataflow pipeline is running.
* **spannerMetadataTableName** (Cloud Spanner metadata table name): The Cloud Spanner change streams connector metadata table name to use. If not provided, a Cloud Spanner change streams connector metadata table will automatically be created during the pipeline flow. This parameter must be provided when updating an existing pipeline and should not be provided otherwise.
* **startTimestamp** (The timestamp to read change streams from): The starting DateTime, inclusive, to use for reading change streams (https://tools.ietf.org/html/rfc3339). For example, 2022-05-05T07:59:59Z. Defaults to the timestamp when the pipeline starts.
* **endTimestamp** (The timestamp to read change streams to): The ending DateTime, inclusive, to use for reading change streams (https://tools.ietf.org/html/rfc3339). Ex-2022-05-05T07:59:59Z. Defaults to an infinite time in the future.
* **spannerHost** (Cloud Spanner Endpoint to call): The Cloud Spanner endpoint to call in the template. Only used for testing. (Example: https://spanner.googleapis.com). Defaults to: https://spanner.googleapis.com.
* **outputFileFormat** (Output file format): The format of the output Cloud Storage file. Allowed formats are TEXT, AVRO. Default is AVRO.
* **windowDuration** (Window duration): The window duration/size in which data will be written to Cloud Storage. Allowed formats are: Ns (for seconds, example: 5s), Nm (for minutes, example: 12m), Nh (for hours, example: 2h). (Example: 5m). Defaults to: 5m.
* **rpcPriority** (Priority for Spanner RPC invocations): The request priority for Cloud Spanner calls. The value must be one of: [HIGH,MEDIUM,LOW]. Defaults to: HIGH.
* **outputFilenamePrefix** (Output filename prefix of the files to write): The prefix to place on each windowed file. (Example: output-). Defaults to: output.
* **numShards** (Maximum output shards): The maximum number of output shards produced when writing. A higher number of shards means higher throughput for writing to Cloud Storage, but potentially higher data aggregation cost across shards when processing output Cloud Storage files. Defaults to: 20.

## Getting Started

### Requirements

* Java 11
* Maven
* Valid resources for mandatory parameters.
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following command:
    * `gcloud auth login`

This README uses
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates#templates-plugin)
. Install the plugin with the following command to proceed:

```shell
mvn clean install -pl plugins/templates-maven-plugin -am
```

### Building Template

This template is a Flex Template, meaning that the pipeline code will be
containerized and the container will be executed on Dataflow. Please
check [Use Flex Templates](https://cloud.google.com/dataflow/docs/guides/templates/using-flex-templates)
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
-pl v2/googlecloud-to-googlecloud -am
```

The command should print what is the template location on Cloud Storage:

```
Flex Template was staged! gs://{BUCKET}/{PATH}
```


#### Running the Template

**Using the staged template**:

You can use the path above to share or run the template.

To start a job with the template at any time using `gcloud`, you can use:

```shell
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Spanner_Change_Streams_to_Google_Cloud_Storage"
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>
export REGION=us-central1

### Mandatory
export SPANNER_INSTANCE_ID=<spannerInstanceId>
export SPANNER_DATABASE=<spannerDatabase>
export SPANNER_METADATA_INSTANCE_ID=<spannerMetadataInstanceId>
export SPANNER_METADATA_DATABASE=<spannerMetadataDatabase>
export SPANNER_CHANGE_STREAM_NAME=<spannerChangeStreamName>
export GCS_OUTPUT_DIRECTORY=<gcsOutputDirectory>

### Optional
export SPANNER_PROJECT_ID=""
export SPANNER_METADATA_TABLE_NAME=<spannerMetadataTableName>
export START_TIMESTAMP=""
export END_TIMESTAMP=""
export SPANNER_HOST="https://spanner.googleapis.com"
export OUTPUT_FILE_FORMAT="AVRO"
export WINDOW_DURATION="5m"
export RPC_PRIORITY="HIGH"
export OUTPUT_FILENAME_PREFIX="output"
export NUM_SHARDS=20

gcloud dataflow flex-template run "spanner-change-streams-to-google-cloud-storage-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "spannerProjectId=$SPANNER_PROJECT_ID" \
  --parameters "spannerInstanceId=$SPANNER_INSTANCE_ID" \
  --parameters "spannerDatabase=$SPANNER_DATABASE" \
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


**Using the plugin**:

Instead of just generating the template in the folder, it is possible to stage
and run the template in a single command. This may be useful for testing when
changing the templates.

```shell
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>
export REGION=us-central1

### Mandatory
export SPANNER_INSTANCE_ID=<spannerInstanceId>
export SPANNER_DATABASE=<spannerDatabase>
export SPANNER_METADATA_INSTANCE_ID=<spannerMetadataInstanceId>
export SPANNER_METADATA_DATABASE=<spannerMetadataDatabase>
export SPANNER_CHANGE_STREAM_NAME=<spannerChangeStreamName>
export GCS_OUTPUT_DIRECTORY=<gcsOutputDirectory>

### Optional
export SPANNER_PROJECT_ID=""
export SPANNER_METADATA_TABLE_NAME=<spannerMetadataTableName>
export START_TIMESTAMP=""
export END_TIMESTAMP=""
export SPANNER_HOST="https://spanner.googleapis.com"
export OUTPUT_FILE_FORMAT="AVRO"
export WINDOW_DURATION="5m"
export RPC_PRIORITY="HIGH"
export OUTPUT_FILENAME_PREFIX="output"
export NUM_SHARDS=20

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="spanner-change-streams-to-google-cloud-storage-job" \
-DtemplateName="Spanner_Change_Streams_to_Google_Cloud_Storage" \
-Dparameters="spannerProjectId=$SPANNER_PROJECT_ID,spannerInstanceId=$SPANNER_INSTANCE_ID,spannerDatabase=$SPANNER_DATABASE,spannerMetadataInstanceId=$SPANNER_METADATA_INSTANCE_ID,spannerMetadataDatabase=$SPANNER_METADATA_DATABASE,spannerMetadataTableName=$SPANNER_METADATA_TABLE_NAME,spannerChangeStreamName=$SPANNER_CHANGE_STREAM_NAME,startTimestamp=$START_TIMESTAMP,endTimestamp=$END_TIMESTAMP,spannerHost=$SPANNER_HOST,outputFileFormat=$OUTPUT_FILE_FORMAT,windowDuration=$WINDOW_DURATION,rpcPriority=$RPC_PRIORITY,gcsOutputDirectory=$GCS_OUTPUT_DIRECTORY,outputFilenamePrefix=$OUTPUT_FILENAME_PREFIX,numShards=$NUM_SHARDS" \
-pl v2/googlecloud-to-googlecloud -am
```
