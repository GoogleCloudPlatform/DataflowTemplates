
Datastream to Firestore template
---
The Datastream MongoDB to Firestore template is a streaming pipeline that reads
<a href="https://cloud.google.com/datastream/docs">Datastream</a> events from a
Cloud Storage bucket and writes them to a Firestore with MongoDB compatibility
database. It is intended for data migration from Datastream sources to Firestore
with MongoDB compatibility.

Data consistency is guaranteed only at the end of migration when all data has
been written to the destination database. To store ordering information for each
record written to the destination database, this template creates an additional
collection (called a shadow collection) for each collection in the source
database. This is used to ensure consistency at the end of migration. By default
the shadow collection is used only on cdc events, it is configurable to be used
on backfill events via setting `useShadowTablesForBackfill` to true. The shadow
collections by default uses prefix `shadow_`, if it can cause collection name
collision with the source database, please configure that by setting
`shadowCollectionPrefix`. The shadow collections are not deleted after migration
and can be used for validation purposes at the end of the migration.

The pipeline by default processes backfill events first with batch write, which
is optimized for performance, followed by cdc events. This is configurable via
setting `processBackfillFirst` to false to process backfill and cdc events
together.

Any errors that occur during operation are recorded in error queues. The error
queue is a Cloud Storage folder which stores all the Datastream events that had
encountered errors.



:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **inputFilePattern**: Path of the file pattern glob to read from. For example, `gs://your-bucket/path/`.
* **inputFileFormat**: The file format of the desired input files. Can be avro or json. Defaults to: avro.
* **connectionUri**: URI to connect to the target project. It should start with either 'mongodb://' or 'mongodb+srv://'. If OIDC authentication mechanism is used and no TOKEN_RESOURCE is provided, it will automatically use FIRESTORE.
* **databaseName**: The database to write to. For example, `(default)`.

### Optional parameters

* **rfcStartDateTime**: The starting DateTime used to fetch from Cloud Storage (https://tools.ietf.org/html/rfc3339). Defaults to: 1970-01-01T00:00:00.00Z.
* **fileReadConcurrency**: The number of concurrent DataStream files to read. Defaults to: 10.
* **gcsPubSubSubscription**: The Pub/Sub subscription being used in a Cloud Storage notification policy. For the name, use the format `projects/<PROJECT_ID>/subscriptions/<SUBSCRIPTION_NAME>`.
* **databaseCollection**: If specified, only replicate this collection. If not specified, replicate all collections. For example, `my-collection`.
* **shadowCollectionPrefix**: The prefix used to name shadow collections. Default: `shadow_`.
* **batchSize**: The batch size for writing to Database. Defaults to: 500.
* **deadLetterQueueDirectory**: The file path used when storing the error queue output. The default file path is a directory under the Dataflow job's temp location.
* **dlqRetryMinutes**: The number of minutes between dead letter queue retries. Defaults to `10`.
* **dlqMaxRetryCount**: The max number of times temporary errors can be retried through DLQ. Defaults to `500`.
* **processBackfillFirst**: When true, all backfill events are processed before any CDC events, otherwise the backfill and cdc events are processed together. Default: false.
* **useShadowTablesForBackfill**: When false, backfill events are processed without shadow tables. This only takes effect when processBackfillFirst is set to true. Default: false.
* **runMode**: This is the run mode type, whether regular or with retryDLQ. Defaults to: regular.
* **directoryWatchDurationInMinutes**: The Duration for which the pipeline should keep polling a directory in GCS. Datastreamoutput files are arranged in a directory structure which depicts the timestamp of the event grouped by minutes. This parameter should be approximately equal tomaximum delay which could occur between event occurring in source database and the same event being written to GCS by Datastream. 99.9 percentile = 10 minutes. Defaults to: 10.
* **streamName**: The name or template for the stream to poll for schema information and source type.
* **dlqGcsPubSubSubscription**: The Pub/Sub subscription being used in a Cloud Storage notification policy for DLQ retry directory when running in regular mode. For the name, use the format `projects/<PROJECT_ID>/subscriptions/<SUBSCRIPTION_NAME>`. When set, the deadLetterQueueDirectory and dlqRetryMinutes are ignored.



## Getting Started

### Requirements

* Java 17
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/datastream-mongodb-to-firestore/src/main/java/com/google/cloud/teleport/v2/templates/DataStreamMongoDBToFirestore.java)

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
-DtemplateName="Cloud_Datastream_MongoDB_to_Firestore" \
-f v2/datastream-mongodb-to-firestore
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Cloud_Datastream_MongoDB_to_Firestore
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Cloud_Datastream_MongoDB_to_Firestore"

### Required
export INPUT_FILE_PATTERN=<inputFilePattern>
export INPUT_FILE_FORMAT=avro
export CONNECTION_URI=<connectionUri>
export DATABASE_NAME=(default)

### Optional
export RFC_START_DATE_TIME=1970-01-01T00:00:00.00Z
export FILE_READ_CONCURRENCY=10
export GCS_PUB_SUB_SUBSCRIPTION=<gcsPubSubSubscription>
export DATABASE_COLLECTION=<databaseCollection>
export SHADOW_COLLECTION_PREFIX=shadow_
export BATCH_SIZE=500
export DEAD_LETTER_QUEUE_DIRECTORY=""
export DLQ_RETRY_MINUTES=10
export DLQ_MAX_RETRY_COUNT=500
export PROCESS_BACKFILL_FIRST=false
export USE_SHADOW_TABLES_FOR_BACKFILL=false
export RUN_MODE=regular
export DIRECTORY_WATCH_DURATION_IN_MINUTES=10
export STREAM_NAME=<streamName>
export DLQ_GCS_PUB_SUB_SUBSCRIPTION=<dlqGcsPubSubSubscription>

gcloud dataflow flex-template run "cloud-datastream-mongodb-to-firestore-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "inputFilePattern=$INPUT_FILE_PATTERN" \
  --parameters "inputFileFormat=$INPUT_FILE_FORMAT" \
  --parameters "rfcStartDateTime=$RFC_START_DATE_TIME" \
  --parameters "fileReadConcurrency=$FILE_READ_CONCURRENCY" \
  --parameters "connectionUri=$CONNECTION_URI" \
  --parameters "databaseName=$DATABASE_NAME" \
  --parameters "gcsPubSubSubscription=$GCS_PUB_SUB_SUBSCRIPTION" \
  --parameters "databaseCollection=$DATABASE_COLLECTION" \
  --parameters "shadowCollectionPrefix=$SHADOW_COLLECTION_PREFIX" \
  --parameters "batchSize=$BATCH_SIZE" \
  --parameters "deadLetterQueueDirectory=$DEAD_LETTER_QUEUE_DIRECTORY" \
  --parameters "dlqRetryMinutes=$DLQ_RETRY_MINUTES" \
  --parameters "dlqMaxRetryCount=$DLQ_MAX_RETRY_COUNT" \
  --parameters "processBackfillFirst=$PROCESS_BACKFILL_FIRST" \
  --parameters "useShadowTablesForBackfill=$USE_SHADOW_TABLES_FOR_BACKFILL" \
  --parameters "runMode=$RUN_MODE" \
  --parameters "directoryWatchDurationInMinutes=$DIRECTORY_WATCH_DURATION_IN_MINUTES" \
  --parameters "streamName=$STREAM_NAME" \
  --parameters "dlqGcsPubSubSubscription=$DLQ_GCS_PUB_SUB_SUBSCRIPTION"
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
export INPUT_FILE_PATTERN=<inputFilePattern>
export INPUT_FILE_FORMAT=avro
export CONNECTION_URI=<connectionUri>
export DATABASE_NAME=(default)

### Optional
export RFC_START_DATE_TIME=1970-01-01T00:00:00.00Z
export FILE_READ_CONCURRENCY=10
export GCS_PUB_SUB_SUBSCRIPTION=<gcsPubSubSubscription>
export DATABASE_COLLECTION=<databaseCollection>
export SHADOW_COLLECTION_PREFIX=shadow_
export BATCH_SIZE=500
export DEAD_LETTER_QUEUE_DIRECTORY=""
export DLQ_RETRY_MINUTES=10
export DLQ_MAX_RETRY_COUNT=500
export PROCESS_BACKFILL_FIRST=false
export USE_SHADOW_TABLES_FOR_BACKFILL=false
export RUN_MODE=regular
export DIRECTORY_WATCH_DURATION_IN_MINUTES=10
export STREAM_NAME=<streamName>
export DLQ_GCS_PUB_SUB_SUBSCRIPTION=<dlqGcsPubSubSubscription>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="cloud-datastream-mongodb-to-firestore-job" \
-DtemplateName="Cloud_Datastream_MongoDB_to_Firestore" \
-Dparameters="inputFilePattern=$INPUT_FILE_PATTERN,inputFileFormat=$INPUT_FILE_FORMAT,rfcStartDateTime=$RFC_START_DATE_TIME,fileReadConcurrency=$FILE_READ_CONCURRENCY,connectionUri=$CONNECTION_URI,databaseName=$DATABASE_NAME,gcsPubSubSubscription=$GCS_PUB_SUB_SUBSCRIPTION,databaseCollection=$DATABASE_COLLECTION,shadowCollectionPrefix=$SHADOW_COLLECTION_PREFIX,batchSize=$BATCH_SIZE,deadLetterQueueDirectory=$DEAD_LETTER_QUEUE_DIRECTORY,dlqRetryMinutes=$DLQ_RETRY_MINUTES,dlqMaxRetryCount=$DLQ_MAX_RETRY_COUNT,processBackfillFirst=$PROCESS_BACKFILL_FIRST,useShadowTablesForBackfill=$USE_SHADOW_TABLES_FOR_BACKFILL,runMode=$RUN_MODE,directoryWatchDurationInMinutes=$DIRECTORY_WATCH_DURATION_IN_MINUTES,streamName=$STREAM_NAME,dlqGcsPubSubSubscription=$DLQ_GCS_PUB_SUB_SUBSCRIPTION" \
-f v2/datastream-mongodb-to-firestore
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
cd v2/datastream-mongodb-to-firestore/terraform/Cloud_Datastream_MongoDB_to_Firestore
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

resource "google_dataflow_flex_template_job" "cloud_datastream_mongodb_to_firestore" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Cloud_Datastream_MongoDB_to_Firestore"
  name              = "cloud-datastream-mongodb-to-firestore"
  region            = var.region
  parameters        = {
    inputFilePattern = "<inputFilePattern>"
    inputFileFormat = "avro"
    connectionUri = "<connectionUri>"
    databaseName = "(default)"
    # rfcStartDateTime = "1970-01-01T00:00:00.00Z"
    # fileReadConcurrency = "10"
    # gcsPubSubSubscription = "<gcsPubSubSubscription>"
    # databaseCollection = "<databaseCollection>"
    # shadowCollectionPrefix = "shadow_"
    # batchSize = "500"
    # deadLetterQueueDirectory = ""
    # dlqRetryMinutes = "10"
    # dlqMaxRetryCount = "500"
    # processBackfillFirst = "false"
    # useShadowTablesForBackfill = "false"
    # runMode = "regular"
    # directoryWatchDurationInMinutes = "10"
    # streamName = "<streamName>"
    # dlqGcsPubSubSubscription = "<dlqGcsPubSubSubscription>"
  }
}
```
