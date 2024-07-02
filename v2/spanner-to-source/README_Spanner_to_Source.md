
Spanner Change Streams to Source template
---
Streaming pipeline. Ingests data from Spanner Change Streams, orders them, and
writes them to a source.



:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required Parameters

* **changeStreamName** (Name of the change stream to read from): This is the name of the Spanner change stream that the pipeline will read from.
* **instanceId** (Cloud Spanner Instance Id.): This is the name of the Cloud Spanner instance where the changestream is present.
* **databaseId** (Cloud Spanner Database Id.): This is the name of the Cloud Spanner database that the changestream is monitoring.
* **spannerProjectId** (Cloud Spanner Project Id.): This is the name of the Cloud Spanner project.
* **metadataInstance** (Cloud Spanner Instance to store metadata when reading from changestreams): This is the instance to store the metadata used by the connector to control the consumption of the change stream API data.
* **metadataDatabase** (Cloud Spanner Database to store metadata when reading from changestreams): This is the database to store the metadata used by the connector to control the consumption of the change stream API data.

### Optional Parameters

* **startTimestamp** (Changes are read from the given timestamp): Read changes from the given timestamp. Defaults to empty.
* **endTimestamp** (Changes are read until the given timestamp): Read changes until the given timestamp. If no timestamp provided, reads indefinitely. Defaults to empty.
* **incrementInterval** (Time interval to increment the Stateful timer.): The timer gets incremented by the specified time interval in seconds. By default, the next timer is set to the current real time.
* **sinkType** (Type of sink to write the data to): The type of sink where the data will get written to.
* **pubSubDataTopicId** (PubSub topic where records will get written to, in the format of 'projects/your-project-id/topics/your-topic-name'): PubSub topic where records will get written to, in the format of 'projects/your-project-id/topics/your-topic-name'. Must be provided if sink is pubsub.
* **pubSubErrorTopicId** (PubSub topic where error records will get written to , in the format of 'projects/your-project-id/topics/your-topic-name'): PubSub topic where error records will get written to, in the format of 'projects/your-project-id/topics/your-topic-name'. Must be provided if sink is pubsub.
* **pubSubEndpoint** (Endpoint for pubsub): Endpoint for pubsub. Must be provided if sink is pubsub. Defaults to empty.
* **kafkaClusterFilePath** (Path to GCS file containing Kafka cluster details): This is the path to GCS file containing Kafka cluster details. Must be provided if sink is kafka.
* **sourceShardsFilePath** (Path to GCS file containing the the Source shard details): Path to GCS file containing connection profile info for source shards. Must be provided if sink is kafka.
* **sessionFilePath** (Session File Path in Cloud Storage): Session file path in Cloud Storage that contains mapping information from HarbourBridge.
* **filtrationMode** (Filtration mode): Mode of Filtration, decides how to drop certain records based on a criteria. Currently supported modes are: none (filter nothing), forward_migration (filter records written via the forward migration pipeline). Defaults to forward_migration.
* **shardingCustomJarPath** (Custom jar location in Cloud Storage): Custom jar location in Cloud Storage that contains the customization logic for fetching shard id. Defaults to empty.
* **shardingCustomClassName** (Custom class name): Fully qualified class name having the custom shard id implementation.  It is a mandatory field in case shardingCustomJarPath is specified. Defaults to empty.
* **shardingCustomParameters** (Custom sharding logic parameters): String containing any custom parameters to be passed to the custom sharding class. Defaults to empty.
* **sourceDbTimezoneOffset** (SourceDB timezone offset): This is the timezone offset from UTC for the source database. Example value: +10:00. Defaults to: +00:00.
* **dlqGcsPubSubSubscription** (The Pub/Sub subscription being used in a Cloud Storage notification policy for DLQ retry directory when running in regular mode.): The Pub/Sub subscription being used in a Cloud Storage notification policy for DLQ retry directory when running in regular mode. The name should be in the format of projects/<project-id>/subscriptions/<subscription-name>. When set, the deadLetterQueueDirectory and dlqRetryMinutes are ignored.
* **skipDirectoryName** (Directory name for holding skipped records): Records skipped from reverse replication are written to this directory. Default directory name is skip.
* **maxShardConnections** (Maximum connections per shard.): This will come from shard file eventually. Defaults to: 10000.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/spanner-to-source/src/main/java/com/google/cloud/teleport/v2/templates/SpannerToSource.java)

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
-DtemplateName="Spanner_to_Source" \
-f v2/spanner-to-source
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Spanner_to_Source
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Spanner_to_Source"

### Required
export CHANGE_STREAM_NAME=<changeStreamName>
export INSTANCE_ID=<instanceId>
export DATABASE_ID=<databaseId>
export SPANNER_PROJECT_ID=<spannerProjectId>
export METADATA_INSTANCE=<metadataInstance>
export METADATA_DATABASE=<metadataDatabase>

### Optional
export START_TIMESTAMP=""
export END_TIMESTAMP=""
export INCREMENT_INTERVAL=0
export SINK_TYPE=<sinkType>
export PUB_SUB_DATA_TOPIC_ID=<pubSubDataTopicId>
export PUB_SUB_ERROR_TOPIC_ID=<pubSubErrorTopicId>
export PUB_SUB_ENDPOINT=""
export KAFKA_CLUSTER_FILE_PATH=<kafkaClusterFilePath>
export SOURCE_SHARDS_FILE_PATH=<sourceShardsFilePath>
export SESSION_FILE_PATH=<sessionFilePath>
export FILTRATION_MODE=forward_migration
export SHARDING_CUSTOM_JAR_PATH=""
export SHARDING_CUSTOM_CLASS_NAME=""
export SHARDING_CUSTOM_PARAMETERS=""
export SOURCE_DB_TIMEZONE_OFFSET=+00:00
export DLQ_GCS_PUB_SUB_SUBSCRIPTION=<dlqGcsPubSubSubscription>
export SKIP_DIRECTORY_NAME=skip
export MAX_SHARD_CONNECTIONS=10000

gcloud dataflow flex-template run "spanner-to-source-job" \
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
  --parameters "incrementInterval=$INCREMENT_INTERVAL" \
  --parameters "sinkType=$SINK_TYPE" \
  --parameters "pubSubDataTopicId=$PUB_SUB_DATA_TOPIC_ID" \
  --parameters "pubSubErrorTopicId=$PUB_SUB_ERROR_TOPIC_ID" \
  --parameters "pubSubEndpoint=$PUB_SUB_ENDPOINT" \
  --parameters "kafkaClusterFilePath=$KAFKA_CLUSTER_FILE_PATH" \
  --parameters "sourceShardsFilePath=$SOURCE_SHARDS_FILE_PATH" \
  --parameters "sessionFilePath=$SESSION_FILE_PATH" \
  --parameters "filtrationMode=$FILTRATION_MODE" \
  --parameters "shardingCustomJarPath=$SHARDING_CUSTOM_JAR_PATH" \
  --parameters "shardingCustomClassName=$SHARDING_CUSTOM_CLASS_NAME" \
  --parameters "shardingCustomParameters=$SHARDING_CUSTOM_PARAMETERS" \
  --parameters "sourceDbTimezoneOffset=$SOURCE_DB_TIMEZONE_OFFSET" \
  --parameters "dlqGcsPubSubSubscription=$DLQ_GCS_PUB_SUB_SUBSCRIPTION" \
  --parameters "skipDirectoryName=$SKIP_DIRECTORY_NAME" \
  --parameters "maxShardConnections=$MAX_SHARD_CONNECTIONS"
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

### Optional
export START_TIMESTAMP=""
export END_TIMESTAMP=""
export INCREMENT_INTERVAL=0
export SINK_TYPE=<sinkType>
export PUB_SUB_DATA_TOPIC_ID=<pubSubDataTopicId>
export PUB_SUB_ERROR_TOPIC_ID=<pubSubErrorTopicId>
export PUB_SUB_ENDPOINT=""
export KAFKA_CLUSTER_FILE_PATH=<kafkaClusterFilePath>
export SOURCE_SHARDS_FILE_PATH=<sourceShardsFilePath>
export SESSION_FILE_PATH=<sessionFilePath>
export FILTRATION_MODE=forward_migration
export SHARDING_CUSTOM_JAR_PATH=""
export SHARDING_CUSTOM_CLASS_NAME=""
export SHARDING_CUSTOM_PARAMETERS=""
export SOURCE_DB_TIMEZONE_OFFSET=+00:00
export DLQ_GCS_PUB_SUB_SUBSCRIPTION=<dlqGcsPubSubSubscription>
export SKIP_DIRECTORY_NAME=skip
export MAX_SHARD_CONNECTIONS=10000

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="spanner-to-source-job" \
-DtemplateName="Spanner_to_Source" \
-Dparameters="changeStreamName=$CHANGE_STREAM_NAME,instanceId=$INSTANCE_ID,databaseId=$DATABASE_ID,spannerProjectId=$SPANNER_PROJECT_ID,metadataInstance=$METADATA_INSTANCE,metadataDatabase=$METADATA_DATABASE,startTimestamp=$START_TIMESTAMP,endTimestamp=$END_TIMESTAMP,incrementInterval=$INCREMENT_INTERVAL,sinkType=$SINK_TYPE,pubSubDataTopicId=$PUB_SUB_DATA_TOPIC_ID,pubSubErrorTopicId=$PUB_SUB_ERROR_TOPIC_ID,pubSubEndpoint=$PUB_SUB_ENDPOINT,kafkaClusterFilePath=$KAFKA_CLUSTER_FILE_PATH,sourceShardsFilePath=$SOURCE_SHARDS_FILE_PATH,sessionFilePath=$SESSION_FILE_PATH,filtrationMode=$FILTRATION_MODE,shardingCustomJarPath=$SHARDING_CUSTOM_JAR_PATH,shardingCustomClassName=$SHARDING_CUSTOM_CLASS_NAME,shardingCustomParameters=$SHARDING_CUSTOM_PARAMETERS,sourceDbTimezoneOffset=$SOURCE_DB_TIMEZONE_OFFSET,dlqGcsPubSubSubscription=$DLQ_GCS_PUB_SUB_SUBSCRIPTION,skipDirectoryName=$SKIP_DIRECTORY_NAME,maxShardConnections=$MAX_SHARD_CONNECTIONS" \
-f v2/spanner-to-source
```

## Terraform

Dataflow supports the utilization of Terraform to manage template jobs,
see [dataflow_flex_template_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_flex_template_job).

Here is an example of Terraform configuration:


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

resource "google_dataflow_flex_template_job" "spanner_to_source" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Spanner_to_Source"
  name              = "spanner-to-source"
  region            = var.region
  parameters        = {
    changeStreamName = "<changeStreamName>"
    instanceId = "<instanceId>"
    databaseId = "<databaseId>"
    spannerProjectId = "<spannerProjectId>"
    metadataInstance = "<metadataInstance>"
    metadataDatabase = "<metadataDatabase>"
    # startTimestamp = ""
    # endTimestamp = ""
    # incrementInterval = "0"
    # sinkType = "<sinkType>"
    # pubSubDataTopicId = "<pubSubDataTopicId>"
    # pubSubErrorTopicId = "<pubSubErrorTopicId>"
    # pubSubEndpoint = ""
    # kafkaClusterFilePath = "<kafkaClusterFilePath>"
    # sourceShardsFilePath = "<sourceShardsFilePath>"
    # sessionFilePath = "<sessionFilePath>"
    # filtrationMode = "forward_migration"
    # shardingCustomJarPath = ""
    # shardingCustomClassName = ""
    # shardingCustomParameters = ""
    # sourceDbTimezoneOffset = "+00:00"
    # dlqGcsPubSubSubscription = "<dlqGcsPubSubSubscription>"
    # skipDirectoryName = "skip"
    # maxShardConnections = "10000"
  }
}
```
