
MongoDB to MongoDB template
---
Copy or stream data from one MongoDB database to another.



:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **sourceUri**: URI to connect to the source MongoDB cluster.
* **targetUri**: URI to connect to the target MongoDB cluster.
* **sourceDatabase**: Database in the source MongoDB to read from.
* **targetDatabase**: Database in the target MongoDB to write to.

### Optional parameters

* **sourceCollection**: Collection in the source MongoDB to read from. If not provided, all collections in the database will be migrated.
* **targetCollection**: Collection in the target MongoDB to write to. If not provided, source collection names will be used.
* **writeBatchSize**: Number of documents in a bulk write. Defaults to: 5000.
* **maxConcurrentAsyncWrites**: Maximum number of concurrent asynchronous batch writes per worker. Defaults to: 10.
* **maxWriteRetries**: Maximum number of retry attempts for transient failures during write. Defaults to: 3.
* **initialWriteRatePerWorker**: Initial maximum documents/second written per worker thread during linear write rate ramp-up. Set to <= 0 to disable throttling. Defaults to: 5000.
* **writeRateRampUpMinutes**: Number of minutes between linear rate limit increases during write rate ramp-up. Defaults to: 5.
* **maxWriteRatePerWorker**: Maximum target documents/second per worker after completing ramp-up. Default is 25000.
* **writeRateRampUpSteps**: Number of discrete linear step increases over the ramp-up period. Defaults to: 5.
* **dlqDirectory**: Base path to store failed events. Events will be grouped by date and time, and separated into 'retryable' and 'permanent' subdirectories.
* **dlqMaxRetries**: Maximum number of times to retry events from DLQ. Defaults to: 3.
* **reconsumeDlqPath**: Path to read files from DLQ for reprocessing. If not provided, write DLQ path will be used.
* **readFromDlq**: If true, reads only from DLQ for retry. If false, reads from MongoDB. Defaults to: false.
* **migrationMode**: Migration mode: 'BACKFILL_AND_STREAMING' (historical backfill and streaming CDC in parallel with stateful deduplication), 'STREAMING_CDC' (stream CDC only), or 'BACKFILL' (historical backfill only). Defaults to: BACKFILL_AND_STREAMING.
* **numChangeStreamSplits**: Number of parallel change stream cursors per collection for high-throughput CDC streaming (e.g. 1, 4, 8, 16). Defaults to: 1.
* **changeStreamFullDocument**: Strategy for fetching full document in change streams: 'updateLookup' (lookup full doc from collection on updates, compatible with MongoDB 4+), 'whenAvailable' (MongoDB 6+ post-image oplog extraction without extra lookup), 'required', or 'default'.
* **startAtOperationTime**: Optional starting clusterTime (epoch seconds or ISO-8601 timestamp) to start change stream cursors from. If omitted in BACKFILL_AND_STREAMING mode, captures the current clusterTime before backfill starts.
* **numWriteShards**: Number of parallel shards per collection for batched writes to prevent Windmill single-key hotspots and maximize write parallelism. Default is 64.
* **maxBufferingDurationMs**: Maximum duration in milliseconds to buffer documents before flushing a batch to target MongoDB. Default is 200ms.
* **targetBackfillChunkSize**: Target number of documents per backfill split. Adaptive volume-based splitting uses this to calculate the number of splits per collection based on estimated count. Default is 200000.
* **maxBackfillSplits**: Maximum number of backfill read splits allowed per collection during adaptive splitting. Default is 256.
* **maxConcurrentBackfillReads**: Maximum number of concurrent in-flight backfill cursors allowed across the cluster. Partitions are distributed across virtual concurrency slots to strictly bound source database connections and cursor memory. Default is 128.
* **dedupStateRetentionHours**: How long per-document deduplication state is retained after the most recent accepted event, in hours. Only applies when both backfill and change streams are enabled. Defaults to 0, meaning state is kept for the life of the job; this is recommended. Per-document state is only about a hundred bytes, so bounding it saves little, and if state expires while backfill is still running then a stale backfill record for that document is treated as unseen and re-applied over newer change stream data. Only set this if deduplication state size is a demonstrated problem, and set it comfortably above your longest expected backfill.
* **javascriptTextTransformGcsPath**: The Cloud Storage URI of the .js file that defines the JavaScript user-defined function (UDF) to use. For example, `gs://my-bucket/my-udfs/my_file.js`.
* **javascriptTextTransformFunctionName**: The name of the JavaScript user-defined function (UDF) to use. For example, if your JavaScript function code is `myTransform(inJson) { /*...do stuff...*/ }`, then the function name is `myTransform`. For sample JavaScript UDFs, see UDF Examples (https://github.com/GoogleCloudPlatform/DataflowTemplates#udf-examples).
* **javascriptTextTransformReloadIntervalMinutes**: Specifies how frequently to reload the UDF, in minutes. If the value is greater than 0, Dataflow periodically checks the UDF file in Cloud Storage, and reloads the UDF if the file is modified. This parameter allows you to update the UDF while the pipeline is running, without needing to restart the job. If the value is `0`, UDF reloading is disabled. The default value is `0`.


## User-Defined functions (UDFs)

The MongoDB to MongoDB Template supports User-Defined functions (UDFs).
UDFs allow you to customize functionality by providing a JavaScript function
without having to maintain or build the entire template code.

Check [Create user-defined functions for Dataflow templates](https://cloud.google.com/dataflow/docs/guides/templates/create-template-udf)
and [Using UDFs](https://github.com/GoogleCloudPlatform/DataflowTemplates#using-udfs)
for more information about how to create and test those functions.


## Getting Started

### Requirements

* Java 17
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/mongodb-to-mongodb/src/main/java/com/google/cloud/teleport/v2/templates/MongoDbToMongoDb.java)

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#templates-plugin).

#### Validating the Template

This template has a validation command that is used to check code quality.

```shell
mvn clean install -PtemplatesValidate \
-DskipTests -am \
-pl v2/mongodb-to-mongodb
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
export ARTIFACT_REGISTRY_REPO=<region>-docker.pkg.dev/$PROJECT/<repo>

mvn clean package -PtemplatesStage  \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-DartifactRegistry="$ARTIFACT_REGISTRY_REPO" \
-DstagePrefix="templates" \
-DtemplateName="Mongodb_To_Mongodb" \
-pl v2/mongodb-to-mongodb -am
```

The `-DartifactRegistry` parameter can be specified to set the artifact registry repository of the Flex Templates image.
If not provided, it defaults to `gcr.io/<project>`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Mongodb_To_Mongodb
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Mongodb_To_Mongodb"

### Required
export SOURCE_URI=<sourceUri>
export TARGET_URI=<targetUri>
export SOURCE_DATABASE=<sourceDatabase>
export TARGET_DATABASE=<targetDatabase>

### Optional
export SOURCE_COLLECTION=<sourceCollection>
export TARGET_COLLECTION=<targetCollection>
export WRITE_BATCH_SIZE=5000
export MAX_CONCURRENT_ASYNC_WRITES=10
export MAX_WRITE_RETRIES=3
export INITIAL_WRITE_RATE_PER_WORKER=5000
export WRITE_RATE_RAMP_UP_MINUTES=5
export MAX_WRITE_RATE_PER_WORKER=25000
export WRITE_RATE_RAMP_UP_STEPS=5
export DLQ_DIRECTORY=<dlqDirectory>
export DLQ_MAX_RETRIES=3
export RECONSUME_DLQ_PATH=<reconsumeDlqPath>
export READ_FROM_DLQ=false
export MIGRATION_MODE=BACKFILL_AND_STREAMING
export NUM_CHANGE_STREAM_SPLITS=1
export CHANGE_STREAM_FULL_DOCUMENT=updateLookup
export START_AT_OPERATION_TIME=<startAtOperationTime>
export NUM_WRITE_SHARDS=64
export MAX_BUFFERING_DURATION_MS=200
export TARGET_BACKFILL_CHUNK_SIZE=200000
export MAX_BACKFILL_SPLITS=256
export MAX_CONCURRENT_BACKFILL_READS=128
export DEDUP_STATE_RETENTION_HOURS=0
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>
export JAVASCRIPT_TEXT_TRANSFORM_RELOAD_INTERVAL_MINUTES=0

gcloud dataflow flex-template run "mongodb-to-mongodb-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "sourceUri=$SOURCE_URI" \
  --parameters "targetUri=$TARGET_URI" \
  --parameters "sourceDatabase=$SOURCE_DATABASE" \
  --parameters "targetDatabase=$TARGET_DATABASE" \
  --parameters "sourceCollection=$SOURCE_COLLECTION" \
  --parameters "targetCollection=$TARGET_COLLECTION" \
  --parameters "writeBatchSize=$WRITE_BATCH_SIZE" \
  --parameters "maxConcurrentAsyncWrites=$MAX_CONCURRENT_ASYNC_WRITES" \
  --parameters "maxWriteRetries=$MAX_WRITE_RETRIES" \
  --parameters "initialWriteRatePerWorker=$INITIAL_WRITE_RATE_PER_WORKER" \
  --parameters "writeRateRampUpMinutes=$WRITE_RATE_RAMP_UP_MINUTES" \
  --parameters "maxWriteRatePerWorker=$MAX_WRITE_RATE_PER_WORKER" \
  --parameters "writeRateRampUpSteps=$WRITE_RATE_RAMP_UP_STEPS" \
  --parameters "dlqDirectory=$DLQ_DIRECTORY" \
  --parameters "dlqMaxRetries=$DLQ_MAX_RETRIES" \
  --parameters "reconsumeDlqPath=$RECONSUME_DLQ_PATH" \
  --parameters "readFromDlq=$READ_FROM_DLQ" \
  --parameters "migrationMode=$MIGRATION_MODE" \
  --parameters "numChangeStreamSplits=$NUM_CHANGE_STREAM_SPLITS" \
  --parameters "changeStreamFullDocument=$CHANGE_STREAM_FULL_DOCUMENT" \
  --parameters "startAtOperationTime=$START_AT_OPERATION_TIME" \
  --parameters "numWriteShards=$NUM_WRITE_SHARDS" \
  --parameters "maxBufferingDurationMs=$MAX_BUFFERING_DURATION_MS" \
  --parameters "targetBackfillChunkSize=$TARGET_BACKFILL_CHUNK_SIZE" \
  --parameters "maxBackfillSplits=$MAX_BACKFILL_SPLITS" \
  --parameters "maxConcurrentBackfillReads=$MAX_CONCURRENT_BACKFILL_READS" \
  --parameters "dedupStateRetentionHours=$DEDUP_STATE_RETENTION_HOURS" \
  --parameters "javascriptTextTransformGcsPath=$JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH" \
  --parameters "javascriptTextTransformFunctionName=$JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME" \
  --parameters "javascriptTextTransformReloadIntervalMinutes=$JAVASCRIPT_TEXT_TRANSFORM_RELOAD_INTERVAL_MINUTES"
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
export SOURCE_URI=<sourceUri>
export TARGET_URI=<targetUri>
export SOURCE_DATABASE=<sourceDatabase>
export TARGET_DATABASE=<targetDatabase>

### Optional
export SOURCE_COLLECTION=<sourceCollection>
export TARGET_COLLECTION=<targetCollection>
export WRITE_BATCH_SIZE=5000
export MAX_CONCURRENT_ASYNC_WRITES=10
export MAX_WRITE_RETRIES=3
export INITIAL_WRITE_RATE_PER_WORKER=5000
export WRITE_RATE_RAMP_UP_MINUTES=5
export MAX_WRITE_RATE_PER_WORKER=25000
export WRITE_RATE_RAMP_UP_STEPS=5
export DLQ_DIRECTORY=<dlqDirectory>
export DLQ_MAX_RETRIES=3
export RECONSUME_DLQ_PATH=<reconsumeDlqPath>
export READ_FROM_DLQ=false
export MIGRATION_MODE=BACKFILL_AND_STREAMING
export NUM_CHANGE_STREAM_SPLITS=1
export CHANGE_STREAM_FULL_DOCUMENT=updateLookup
export START_AT_OPERATION_TIME=<startAtOperationTime>
export NUM_WRITE_SHARDS=64
export MAX_BUFFERING_DURATION_MS=200
export TARGET_BACKFILL_CHUNK_SIZE=200000
export MAX_BACKFILL_SPLITS=256
export MAX_CONCURRENT_BACKFILL_READS=128
export DEDUP_STATE_RETENTION_HOURS=0
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>
export JAVASCRIPT_TEXT_TRANSFORM_RELOAD_INTERVAL_MINUTES=0

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="mongodb-to-mongodb-job" \
-DtemplateName="Mongodb_To_Mongodb" \
-Dparameters="sourceUri=$SOURCE_URI,targetUri=$TARGET_URI,sourceDatabase=$SOURCE_DATABASE,targetDatabase=$TARGET_DATABASE,sourceCollection=$SOURCE_COLLECTION,targetCollection=$TARGET_COLLECTION,writeBatchSize=$WRITE_BATCH_SIZE,maxConcurrentAsyncWrites=$MAX_CONCURRENT_ASYNC_WRITES,maxWriteRetries=$MAX_WRITE_RETRIES,initialWriteRatePerWorker=$INITIAL_WRITE_RATE_PER_WORKER,writeRateRampUpMinutes=$WRITE_RATE_RAMP_UP_MINUTES,maxWriteRatePerWorker=$MAX_WRITE_RATE_PER_WORKER,writeRateRampUpSteps=$WRITE_RATE_RAMP_UP_STEPS,dlqDirectory=$DLQ_DIRECTORY,dlqMaxRetries=$DLQ_MAX_RETRIES,reconsumeDlqPath=$RECONSUME_DLQ_PATH,readFromDlq=$READ_FROM_DLQ,migrationMode=$MIGRATION_MODE,numChangeStreamSplits=$NUM_CHANGE_STREAM_SPLITS,changeStreamFullDocument=$CHANGE_STREAM_FULL_DOCUMENT,startAtOperationTime=$START_AT_OPERATION_TIME,numWriteShards=$NUM_WRITE_SHARDS,maxBufferingDurationMs=$MAX_BUFFERING_DURATION_MS,targetBackfillChunkSize=$TARGET_BACKFILL_CHUNK_SIZE,maxBackfillSplits=$MAX_BACKFILL_SPLITS,maxConcurrentBackfillReads=$MAX_CONCURRENT_BACKFILL_READS,dedupStateRetentionHours=$DEDUP_STATE_RETENTION_HOURS,javascriptTextTransformGcsPath=$JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH,javascriptTextTransformFunctionName=$JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME,javascriptTextTransformReloadIntervalMinutes=$JAVASCRIPT_TEXT_TRANSFORM_RELOAD_INTERVAL_MINUTES" \
-f v2/mongodb-to-mongodb
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
cd v2/mongodb-to-mongodb/terraform/Mongodb_To_Mongodb
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

resource "google_dataflow_flex_template_job" "mongodb_to_mongodb" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Mongodb_To_Mongodb"
  name              = "mongodb-to-mongodb"
  region            = var.region
  parameters        = {
    sourceUri = "<sourceUri>"
    targetUri = "<targetUri>"
    sourceDatabase = "<sourceDatabase>"
    targetDatabase = "<targetDatabase>"
    # sourceCollection = "<sourceCollection>"
    # targetCollection = "<targetCollection>"
    # writeBatchSize = "5000"
    # maxConcurrentAsyncWrites = "10"
    # maxWriteRetries = "3"
    # initialWriteRatePerWorker = "5000"
    # writeRateRampUpMinutes = "5"
    # maxWriteRatePerWorker = "25000"
    # writeRateRampUpSteps = "5"
    # dlqDirectory = "<dlqDirectory>"
    # dlqMaxRetries = "3"
    # reconsumeDlqPath = "<reconsumeDlqPath>"
    # readFromDlq = "false"
    # migrationMode = "BACKFILL_AND_STREAMING"
    # numChangeStreamSplits = "1"
    # changeStreamFullDocument = "updateLookup"
    # startAtOperationTime = "<startAtOperationTime>"
    # numWriteShards = "64"
    # maxBufferingDurationMs = "200"
    # targetBackfillChunkSize = "200000"
    # maxBackfillSplits = "256"
    # maxConcurrentBackfillReads = "128"
    # dedupStateRetentionHours = "0"
    # javascriptTextTransformGcsPath = "<javascriptTextTransformGcsPath>"
    # javascriptTextTransformFunctionName = "<javascriptTextTransformFunctionName>"
    # javascriptTextTransformReloadIntervalMinutes = "0"
  }
}
```
