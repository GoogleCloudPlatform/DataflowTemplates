Spanner Change Streams to Sink Template
---
Streaming pipeline. Ingests data from Spanner Change Streams, orders them, and writes them to a sink.

:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/spanner-change-streams-to-sink)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Spanner_Change_Streams_to_Sink).


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
* **sinkType** (Type of sink to write the data to): The type of sink where the data will get written to.

### Optional Parameters

* **startTimestamp** (Changes are read from the given timestamp): Read changes from the given timestamp. Defaults to empty.
* **endTimestamp** (Changes are read until the given timestamp): Read changes until the given timestamp. If no timestamp provided, reads indefinitely. Defaults to empty.
* **incrementInterval** (Time interval to increment the Stateful timer.): The timer gets incremented by the specified time interval in seconds. By default, the next timer is set to the current real time.
* **pubSubProjectId** (Project ID for pubsub): Project Id for pubsub. By default, takes the same project as Spanner.
* **pubSubDataTopicId** (PubSub topic where records will get written to.): PubSub topic where records will get written to. Must be provided if sink is pubsub. Defaults to empty.
* **pubSubErrorTopicId** (PubSub topic where error records will get written to.): PubSub topic where error records will get written to. Must be provided if sink is pubsub. Defaults to empty.
* **pubSubEndpoint** (Endpoint for pubsub): Endpoint for pubsub. Must be provided if sink is pubsub. Defaults to empty.
* **kafkaClusterFilePath** (Path to GCS file containing Kafka cluster details): This is the path to GCS file containing Kafka cluster details. Must be provided if sink is kafka.
* **sourceShardsFilePath** (Path to GCS file containing the the Source shard details): Path to GCS file containing connection profile info for source shards. Must be provided if sink is kafka.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell! 
[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=/v2/spanner-change-streams-to-sink/src/main/java/com/google/cloud/teleport/v2/templates/SpannerChangeStreamsToSink.java)

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
-DtemplateName="Spanner_Change_Streams_to_Sink" \
-pl v2/spanner-change-streams-to-sink \
-am
```

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Spanner_Change_Streams_to_Sink
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Spanner_Change_Streams_to_Sink"

### Required
export CHANGE_STREAM_NAME=<changeStreamName>
export INSTANCE_ID=<instanceId>
export DATABASE_ID=<databaseId>
export SPANNER_PROJECT_ID=<spannerProjectId>
export METADATA_INSTANCE=<metadataInstance>
export METADATA_DATABASE=<metadataDatabase>
export SINK_TYPE=<sinkType>

### Optional
export START_TIMESTAMP=""
export END_TIMESTAMP=""
export INCREMENT_INTERVAL=0
export PUB_SUB_PROJECT_ID=""
export PUB_SUB_DATA_TOPIC_ID=""
export PUB_SUB_ERROR_TOPIC_ID=""
export PUB_SUB_ENDPOINT=""
export KAFKA_CLUSTER_FILE_PATH=<kafkaClusterFilePath>
export SOURCE_SHARDS_FILE_PATH=<sourceShardsFilePath>

gcloud dataflow flex-template run "spanner-change-streams-to-sink-job" \
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
  --parameters "pubSubProjectId=$PUB_SUB_PROJECT_ID" \
  --parameters "pubSubDataTopicId=$PUB_SUB_DATA_TOPIC_ID" \
  --parameters "pubSubErrorTopicId=$PUB_SUB_ERROR_TOPIC_ID" \
  --parameters "pubSubEndpoint=$PUB_SUB_ENDPOINT" \
  --parameters "kafkaClusterFilePath=$KAFKA_CLUSTER_FILE_PATH" \
  --parameters "sourceShardsFilePath=$SOURCE_SHARDS_FILE_PATH" 
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
export SINK_TYPE=<sinkType>

### Optional
export START_TIMESTAMP=""
export END_TIMESTAMP=""
export INCREMENT_INTERVAL=0
export PUB_SUB_PROJECT_ID=""
export PUB_SUB_DATA_TOPIC_ID=""
export PUB_SUB_ERROR_TOPIC_ID=""
export PUB_SUB_ENDPOINT=""
export KAFKA_CLUSTER_FILE_PATH=<kafkaClusterFilePath>
export SOURCE_SHARDS_FILE_PATH=<sourceShardsFilePath>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="spanner-change-streams-to-sink-job" \
-DtemplateName="Spanner_Change_Streams_to_Sink" \
-Dparameters="changeStreamName=$CHANGE_STREAM_NAME,instanceId=$INSTANCE_ID,databaseId=$DATABASE_ID,spannerProjectId=$SPANNER_PROJECT_ID,metadataInstance=$METADATA_INSTANCE,metadataDatabase=$METADATA_DATABASE,startTimestamp=$START_TIMESTAMP,endTimestamp=$END_TIMESTAMP,incrementInterval=$INCREMENT_INTERVAL,sinkType=$SINK_TYPE,pubSubProjectId=$PUB_SUB_PROJECT_ID,pubSubDataTopicId=$PUB_SUB_DATA_TOPIC_ID,pubSubErrorTopicId=$PUB_SUB_ERROR_TOPIC_ID,pubSubEndpoint=$PUB_SUB_ENDPOINT,kafkaClusterFilePath=$KAFKA_CLUSTER_FILE_PATH,sourceShardsFilePath=$SOURCE_SHARDS_FILE_PATH" \
-pl v2/spanner-change-streams-to-sink \
-am
```


