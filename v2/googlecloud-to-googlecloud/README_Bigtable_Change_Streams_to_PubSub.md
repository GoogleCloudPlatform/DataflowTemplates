Cloud Bigtable change streams to PubSub Template
---
Streaming pipeline. Streams Bigtable data change records and writes them into PubSub using Dataflow Runner V2.

:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided-templates)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Bigtable_Change_Streams_to_PubSub).


:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required Parameters

* **pubSubTopic** (The output Pub/Sub topic): The Pub/Sub topic to publish PubSubMessage.
* **bigtableChangeStreamAppProfile** (Cloud Bigtable application profile ID): The application profile is used to distinguish workload in Cloud Bigtable.
* **bigtableReadInstanceId** (Source Bigtable Instance ID): The ID of the Cloud Bigtable instance that contains the table.
* **bigtableReadTableId** (Source Cloud Bigtable table ID): The Cloud Bigtable table to read from.

### Optional Parameters

* **messageEncoding** (The encoding of the message written into PubSub): The format of the message to be written into PubSub. Allowed formats are Binary and JSON Text. Defaults to: JSON.
* **messageFormat** (The format of the message written into PubSub): The message format chosen for outputting data to PubSub. Allowed formats are AVRO, Protocol Buffer and JSON Text. Defaults to: JSON.
* **stripValue** (Strip value for SetCell mutation):  If true the SetCell mutation message won’t include the value written. Defaults to: false.
* **dlqDirectory** (Dead letter queue directory): The file path to store any unprocessed records with the reason they failed to be processed. Default is a directory under the Dataflow job's temp location. The default value is enough under most conditions.
* **dlqRetryMinutes** (Dead letter queue retry minutes): The number of minutes between dead letter queue retries. Defaults to 10.
* **dlqMaxRetries** (Dead letter maximum retries): The number of attempts to process change stream mutations. Defaults to 5.
* **pubSubAPI** (Pub/Sub API): Pub/Sub API used to implement the pipeline. Allowed APIs are pubsubio and native_client. Default is pubsubio. For a small QPS, native_client can achieve a smaller latency than pubsubio. For a large QPS, pubsubio has better and more stable performance.
* **pubSubProjectId** (PubSub project ID): The PubSub Project. Default is the project for the Dataflow job.
* **bigtableChangeStreamMetadataInstanceId** (Cloud Bigtable change streams metadata instance ID): The Cloud Bigtable instance to use for the change streams connector metadata table. Defaults to empty.
* **bigtableChangeStreamMetadataTableTableId** (Cloud Bigtable change streams metadata table ID): The Cloud Bigtable change streams connector metadata table ID to use. If not provided, a Cloud Bigtable change streams connector metadata table will automatically be created during the pipeline flow. Defaults to empty.
* **bigtableChangeStreamCharset** (Bigtable change streams charset name when reading values and column qualifiers): Bigtable change streams charset name when reading values and column qualifiers. Default is UTF-8.
* **bigtableChangeStreamStartTimestamp** (The timestamp to read change streams from): The starting DateTime, inclusive, to use for reading change streams (https://tools.ietf.org/html/rfc3339). For example, 2022-05-05T07:59:59Z. Defaults to the timestamp when the pipeline starts.
* **bigtableChangeStreamIgnoreColumnFamilies** (Cloud Bigtable change streams column families to ignore): A comma-separated list of column family names changes to which won't be captured. Defaults to empty.
* **bigtableChangeStreamIgnoreColumns** (Cloud Bigtable change streams columns to ignore): A comma-separated list of column names changes to which won't be captured. Defaults to empty.
* **bigtableChangeStreamName** (A unique name of the client pipeline): Allows to resume processing from the point where a previously running pipeline stopped.
* **bigtableReadProjectId** (Source Cloud Bigtable Project ID): Project to read Cloud Bigtable data from. The default for this parameter is the project where the Dataflow pipeline is running.
* **bigtableReadAppProfile** (Bigtable App Profile): Bigtable App Profile to use for reads. The default for this parameter is the Bigtable instance's default app profile.
* **bigtableRpcAttemptTimeoutMs** (The timeout for an RPC attempt in milliseconds): This sets the timeout for an RPC attempt in milliseconds.
* **bigtableRpcTimeoutMs** (The total timeout for an RPC operation in milliseconds): This sets the total timeout for an RPC operation in milliseconds.
* **bigtableAdditionalRetryCodes** (The additional retry codes): This sets the additional retry codes, separated by ',' (Example: RESOURCE_EXHAUSTED,DEADLINE_EXCEEDED).



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!
[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=/v2/googlecloud-to-googlecloud/src/main/java/com/google/cloud/teleport/v2/templates/bigtablechangestreamstopubsub/BigtableChangeStreamsToPubSub.java)

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
-DtemplateName="Bigtable_Change_Streams_to_PubSub" \
-pl v2/googlecloud-to-googlecloud \
-am
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Bigtable_Change_Streams_to_PubSub
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Bigtable_Change_Streams_to_PubSub"

### Required
export PUB_SUB_TOPIC=<pubSubTopic>
export BIGTABLE_CHANGE_STREAM_APP_PROFILE=<bigtableChangeStreamAppProfile>
export BIGTABLE_READ_INSTANCE_ID=<bigtableReadInstanceId>
export BIGTABLE_READ_TABLE_ID=<bigtableReadTableId>

### Optional
export MESSAGE_ENCODING="JSON"
export MESSAGE_FORMAT="JSON"
export STRIP_VALUE=false
export DLQ_DIRECTORY=""
export DLQ_RETRY_MINUTES=10
export DLQ_MAX_RETRIES=5
export PUB_SUB_API="pubsubio"
export PUB_SUB_PROJECT_ID=""
export BIGTABLE_CHANGE_STREAM_METADATA_INSTANCE_ID=""
export BIGTABLE_CHANGE_STREAM_METADATA_TABLE_TABLE_ID=""
export BIGTABLE_CHANGE_STREAM_CHARSET="UTF-8"
export BIGTABLE_CHANGE_STREAM_START_TIMESTAMP=""
export BIGTABLE_CHANGE_STREAM_IGNORE_COLUMN_FAMILIES=""
export BIGTABLE_CHANGE_STREAM_IGNORE_COLUMNS=""
export BIGTABLE_CHANGE_STREAM_NAME=<bigtableChangeStreamName>
export BIGTABLE_READ_PROJECT_ID=""
export BIGTABLE_READ_APP_PROFILE="default"
export BIGTABLE_RPC_ATTEMPT_TIMEOUT_MS=<bigtableRpcAttemptTimeoutMs>
export BIGTABLE_RPC_TIMEOUT_MS=<bigtableRpcTimeoutMs>
export BIGTABLE_ADDITIONAL_RETRY_CODES=<bigtableAdditionalRetryCodes>

gcloud dataflow flex-template run "bigtable-change-streams-to-pubsub-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "pubSubTopic=$PUB_SUB_TOPIC" \
  --parameters "messageEncoding=$MESSAGE_ENCODING" \
  --parameters "messageFormat=$MESSAGE_FORMAT" \
  --parameters "stripValue=$STRIP_VALUE" \
  --parameters "dlqDirectory=$DLQ_DIRECTORY" \
  --parameters "dlqRetryMinutes=$DLQ_RETRY_MINUTES" \
  --parameters "dlqMaxRetries=$DLQ_MAX_RETRIES" \
  --parameters "pubSubAPI=$PUB_SUB_API" \
  --parameters "pubSubProjectId=$PUB_SUB_PROJECT_ID" \
  --parameters "bigtableChangeStreamMetadataInstanceId=$BIGTABLE_CHANGE_STREAM_METADATA_INSTANCE_ID" \
  --parameters "bigtableChangeStreamMetadataTableTableId=$BIGTABLE_CHANGE_STREAM_METADATA_TABLE_TABLE_ID" \
  --parameters "bigtableChangeStreamAppProfile=$BIGTABLE_CHANGE_STREAM_APP_PROFILE" \
  --parameters "bigtableChangeStreamCharset=$BIGTABLE_CHANGE_STREAM_CHARSET" \
  --parameters "bigtableChangeStreamStartTimestamp=$BIGTABLE_CHANGE_STREAM_START_TIMESTAMP" \
  --parameters "bigtableChangeStreamIgnoreColumnFamilies=$BIGTABLE_CHANGE_STREAM_IGNORE_COLUMN_FAMILIES" \
  --parameters "bigtableChangeStreamIgnoreColumns=$BIGTABLE_CHANGE_STREAM_IGNORE_COLUMNS" \
  --parameters "bigtableChangeStreamName=$BIGTABLE_CHANGE_STREAM_NAME" \
  --parameters "bigtableReadInstanceId=$BIGTABLE_READ_INSTANCE_ID" \
  --parameters "bigtableReadTableId=$BIGTABLE_READ_TABLE_ID" \
  --parameters "bigtableReadProjectId=$BIGTABLE_READ_PROJECT_ID" \
  --parameters "bigtableReadAppProfile=$BIGTABLE_READ_APP_PROFILE" \
  --parameters "bigtableRpcAttemptTimeoutMs=$BIGTABLE_RPC_ATTEMPT_TIMEOUT_MS" \
  --parameters "bigtableRpcTimeoutMs=$BIGTABLE_RPC_TIMEOUT_MS" \
  --parameters "bigtableAdditionalRetryCodes=$BIGTABLE_ADDITIONAL_RETRY_CODES"
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
export PUB_SUB_TOPIC=<pubSubTopic>
export BIGTABLE_CHANGE_STREAM_APP_PROFILE=<bigtableChangeStreamAppProfile>
export BIGTABLE_READ_INSTANCE_ID=<bigtableReadInstanceId>
export BIGTABLE_READ_TABLE_ID=<bigtableReadTableId>

### Optional
export MESSAGE_ENCODING="JSON"
export MESSAGE_FORMAT="JSON"
export STRIP_VALUE=false
export DLQ_DIRECTORY=""
export DLQ_RETRY_MINUTES=10
export DLQ_MAX_RETRIES=5
export PUB_SUB_API="pubsubio"
export PUB_SUB_PROJECT_ID=""
export BIGTABLE_CHANGE_STREAM_METADATA_INSTANCE_ID=""
export BIGTABLE_CHANGE_STREAM_METADATA_TABLE_TABLE_ID=""
export BIGTABLE_CHANGE_STREAM_CHARSET="UTF-8"
export BIGTABLE_CHANGE_STREAM_START_TIMESTAMP=""
export BIGTABLE_CHANGE_STREAM_IGNORE_COLUMN_FAMILIES=""
export BIGTABLE_CHANGE_STREAM_IGNORE_COLUMNS=""
export BIGTABLE_CHANGE_STREAM_NAME=<bigtableChangeStreamName>
export BIGTABLE_READ_PROJECT_ID=""
export BIGTABLE_READ_APP_PROFILE="default"
export BIGTABLE_RPC_ATTEMPT_TIMEOUT_MS=<bigtableRpcAttemptTimeoutMs>
export BIGTABLE_RPC_TIMEOUT_MS=<bigtableRpcTimeoutMs>
export BIGTABLE_ADDITIONAL_RETRY_CODES=<bigtableAdditionalRetryCodes>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="bigtable-change-streams-to-pubsub-job" \
-DtemplateName="Bigtable_Change_Streams_to_PubSub" \
-Dparameters="pubSubTopic=$PUB_SUB_TOPIC,messageEncoding=$MESSAGE_ENCODING,messageFormat=$MESSAGE_FORMAT,stripValue=$STRIP_VALUE,dlqDirectory=$DLQ_DIRECTORY,dlqRetryMinutes=$DLQ_RETRY_MINUTES,dlqMaxRetries=$DLQ_MAX_RETRIES,pubSubAPI=$PUB_SUB_API,pubSubProjectId=$PUB_SUB_PROJECT_ID,bigtableChangeStreamMetadataInstanceId=$BIGTABLE_CHANGE_STREAM_METADATA_INSTANCE_ID,bigtableChangeStreamMetadataTableTableId=$BIGTABLE_CHANGE_STREAM_METADATA_TABLE_TABLE_ID,bigtableChangeStreamAppProfile=$BIGTABLE_CHANGE_STREAM_APP_PROFILE,bigtableChangeStreamCharset=$BIGTABLE_CHANGE_STREAM_CHARSET,bigtableChangeStreamStartTimestamp=$BIGTABLE_CHANGE_STREAM_START_TIMESTAMP,bigtableChangeStreamIgnoreColumnFamilies=$BIGTABLE_CHANGE_STREAM_IGNORE_COLUMN_FAMILIES,bigtableChangeStreamIgnoreColumns=$BIGTABLE_CHANGE_STREAM_IGNORE_COLUMNS,bigtableChangeStreamName=$BIGTABLE_CHANGE_STREAM_NAME,bigtableReadInstanceId=$BIGTABLE_READ_INSTANCE_ID,bigtableReadTableId=$BIGTABLE_READ_TABLE_ID,bigtableReadProjectId=$BIGTABLE_READ_PROJECT_ID,bigtableReadAppProfile=$BIGTABLE_READ_APP_PROFILE,bigtableRpcAttemptTimeoutMs=$BIGTABLE_RPC_ATTEMPT_TIMEOUT_MS,bigtableRpcTimeoutMs=$BIGTABLE_RPC_TIMEOUT_MS,bigtableAdditionalRetryCodes=$BIGTABLE_ADDITIONAL_RETRY_CODES" \
-pl v2/googlecloud-to-googlecloud \
-am
```
