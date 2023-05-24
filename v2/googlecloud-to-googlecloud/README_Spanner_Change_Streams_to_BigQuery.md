Cloud Spanner change streams to BigQuery Template
---
Streaming pipeline. Streams Spanner data change records and writes them into BigQuery using Dataflow Runner V2.

:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/cloud-spanner-change-streams-to-bigquery)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Spanner_Change_Streams_to_BigQuery).


:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required Parameters

* **spannerInstanceId** (Spanner instance ID): The Spanner instance to read change streams from.
* **spannerDatabase** (Spanner database): The Spanner database to read change streams from.
* **spannerMetadataInstanceId** (Spanner metadata instance ID): The Spanner instance to use for the change streams connector metadata table.
* **spannerMetadataDatabase** (Spanner metadata database): The Spanner database to use for the change streams connector metadata table. For change streams tracking all tables in a database, we recommend putting the metadata table in a separate database.
* **spannerChangeStreamName** (Spanner change stream): The name of the Spanner change stream to read from.
* **bigQueryDataset** (BigQuery dataset): The BigQuery dataset for change streams output.

### Optional Parameters

* **spannerProjectId** (Spanner Project ID): Project to read change streams from. The default for this parameter is the project where the Dataflow pipeline is running.
* **spannerDatabaseRole** (Spanner database role): Database role user assumes while reading from the change stream. The database role should have required privileges to read from change stream. If a database role is not specified, the user should have required IAM permissions to read from the database.
* **spannerMetadataTableName** (Cloud Spanner metadata table name): The Cloud Spanner change streams connector metadata table name to use. If not provided, a Cloud Spanner change streams connector metadata table will automatically be created during the pipeline flow. This parameter must be provided when updating an existing pipeline and should not be provided otherwise.
* **rpcPriority** (Priority for Spanner RPC invocations): The request priority for Cloud Spanner calls. The value must be one of: [HIGH,MEDIUM,LOW]. Defaults to: HIGH.
* **spannerHost** (Cloud Spanner Endpoint to call): The Cloud Spanner endpoint to call in the template. Only used for testing. (Example: https://batch-spanner.googleapis.com).
* **startTimestamp** (The timestamp to read change streams from): The starting DateTime, inclusive, to use for reading change streams (https://tools.ietf.org/html/rfc3339). For example, 2022-05-05T07:59:59Z. Defaults to the timestamp when the pipeline starts.
* **endTimestamp** (The timestamp to read change streams to): The ending DateTime, inclusive, to use for reading change streams (https://tools.ietf.org/html/rfc3339). Ex-2022-05-05T07:59:59Z. Defaults to an infinite time in the future.
* **bigQueryProjectId** (BigQuery project ID): The BigQuery Project. Default is the project for the Dataflow job.
* **bigQueryChangelogTableNameTemplate** (BigQuery table name Template): The Template for the BigQuery table name that contains the change log. Defaults to: {_metadata_spanner_table_name}_changelog.
* **deadLetterQueueDirectory** (Dead letter queue directory): The file path to store any unprocessed records with the reason they failed to be processed. Default is a directory under the Dataflow job's temp location. The default value is enough under most conditions.
* **dlqRetryMinutes** (Dead letter queue retry minutes): The number of minutes between dead letter queue retries. Defaults to 10.
* **ignoreFields** (Fields to be ignored): Comma separated list of fields to be ignored, these could be fields of tracked tables, or metadata fields which are _metadata_spanner_mod_type, _metadata_spanner_table_name, _metadata_spanner_commit_timestamp, _metadata_spanner_server_transaction_id, _metadata_spanner_record_sequence, _metadata_spanner_is_last_record_in_transaction_in_partition, _metadata_spanner_number_of_records_in_transaction, _metadata_spanner_number_of_partitions_in_transaction, _metadata_big_query_commit_timestamp. Defaults to empty.
* **useStorageWriteApi** (Use BigQuery Storage Write API): If enabled (set to true) the pipeline will use Storage Write API when writing the data to BigQuery (see https://cloud.google.com/blog/products/data-analytics/streaming-data-into-bigquery-using-storage-write-api). If this is enabled and at-least-once semantics (useStorageWriteApiAtLeastOnce) option is off then "Number of streams for BigQuery Storage Write API" and "Triggering frequency in seconds for BigQuery Storage Write API" must be provided. Defaults to: false.
* **useStorageWriteApiAtLeastOnce** (Use at at-least-once semantics in BigQuery Storage Write API): This parameter takes effect only if "Use BigQuery Storage Write API" is enabled. If enabled the at-least-once semantics will be used for Storage Write API, otherwise exactly-once semantics will be used. Defaults to: false.
* **numStorageWriteApiStreams** (Number of streams for BigQuery Storage Write API): Number of streams defines the parallelism of the BigQueryIO’s Write transform and roughly corresponds to the number of Storage Write API’s streams which will be used by the pipeline. See https://cloud.google.com/blog/products/data-analytics/streaming-data-into-bigquery-using-storage-write-api for the recommended values. Defaults to: 0.
* **storageWriteApiTriggeringFrequencySec** (Triggering frequency in seconds for BigQuery Storage Write API): Triggering frequency will determine how soon the data will be visible for querying in BigQuery. See https://cloud.google.com/blog/products/data-analytics/streaming-data-into-bigquery-using-storage-write-api for the recommended values.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!
[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=/v2/googlecloud-to-googlecloud/src/main/java/com/google/cloud/teleport/v2/templates/spannerchangestreamstobigquery/SpannerChangeStreamsToBigQuery.java)

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
-DtemplateName="Spanner_Change_Streams_to_BigQuery" \
-pl v2/googlecloud-to-googlecloud \
-am
```

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Spanner_Change_Streams_to_BigQuery
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Spanner_Change_Streams_to_BigQuery"

### Required
export SPANNER_INSTANCE_ID=<spannerInstanceId>
export SPANNER_DATABASE=<spannerDatabase>
export SPANNER_METADATA_INSTANCE_ID=<spannerMetadataInstanceId>
export SPANNER_METADATA_DATABASE=<spannerMetadataDatabase>
export SPANNER_CHANGE_STREAM_NAME=<spannerChangeStreamName>
export BIG_QUERY_DATASET=<bigQueryDataset>

### Optional
export SPANNER_PROJECT_ID=""
export SPANNER_DATABASE_ROLE=<spannerDatabaseRole>
export SPANNER_METADATA_TABLE_NAME=<spannerMetadataTableName>
export RPC_PRIORITY="HIGH"
export SPANNER_HOST=<spannerHost>
export START_TIMESTAMP=""
export END_TIMESTAMP=""
export BIG_QUERY_PROJECT_ID=""
export BIG_QUERY_CHANGELOG_TABLE_NAME_TEMPLATE="{_metadata_spanner_table_name}_changelog"
export DEAD_LETTER_QUEUE_DIRECTORY=""
export DLQ_RETRY_MINUTES=10
export IGNORE_FIELDS=""
export USE_STORAGE_WRITE_API=false
export USE_STORAGE_WRITE_API_AT_LEAST_ONCE=false
export NUM_STORAGE_WRITE_API_STREAMS=0
export STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC=<storageWriteApiTriggeringFrequencySec>

gcloud dataflow flex-template run "spanner-change-streams-to-bigquery-job" \
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
  --parameters "rpcPriority=$RPC_PRIORITY" \
  --parameters "spannerHost=$SPANNER_HOST" \
  --parameters "startTimestamp=$START_TIMESTAMP" \
  --parameters "endTimestamp=$END_TIMESTAMP" \
  --parameters "bigQueryDataset=$BIG_QUERY_DATASET" \
  --parameters "bigQueryProjectId=$BIG_QUERY_PROJECT_ID" \
  --parameters "bigQueryChangelogTableNameTemplate=$BIG_QUERY_CHANGELOG_TABLE_NAME_TEMPLATE" \
  --parameters "deadLetterQueueDirectory=$DEAD_LETTER_QUEUE_DIRECTORY" \
  --parameters "dlqRetryMinutes=$DLQ_RETRY_MINUTES" \
  --parameters "ignoreFields=$IGNORE_FIELDS" \
  --parameters "useStorageWriteApi=$USE_STORAGE_WRITE_API" \
  --parameters "useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE" \
  --parameters "numStorageWriteApiStreams=$NUM_STORAGE_WRITE_API_STREAMS" \
  --parameters "storageWriteApiTriggeringFrequencySec=$STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC"
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
export BIG_QUERY_DATASET=<bigQueryDataset>

### Optional
export SPANNER_PROJECT_ID=""
export SPANNER_DATABASE_ROLE=<spannerDatabaseRole>
export SPANNER_METADATA_TABLE_NAME=<spannerMetadataTableName>
export RPC_PRIORITY="HIGH"
export SPANNER_HOST=<spannerHost>
export START_TIMESTAMP=""
export END_TIMESTAMP=""
export BIG_QUERY_PROJECT_ID=""
export BIG_QUERY_CHANGELOG_TABLE_NAME_TEMPLATE="{_metadata_spanner_table_name}_changelog"
export DEAD_LETTER_QUEUE_DIRECTORY=""
export DLQ_RETRY_MINUTES=10
export IGNORE_FIELDS=""
export USE_STORAGE_WRITE_API=false
export USE_STORAGE_WRITE_API_AT_LEAST_ONCE=false
export NUM_STORAGE_WRITE_API_STREAMS=0
export STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC=<storageWriteApiTriggeringFrequencySec>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="spanner-change-streams-to-bigquery-job" \
-DtemplateName="Spanner_Change_Streams_to_BigQuery" \
-Dparameters="spannerProjectId=$SPANNER_PROJECT_ID,spannerInstanceId=$SPANNER_INSTANCE_ID,spannerDatabase=$SPANNER_DATABASE,spannerDatabaseRole=$SPANNER_DATABASE_ROLE,spannerMetadataInstanceId=$SPANNER_METADATA_INSTANCE_ID,spannerMetadataDatabase=$SPANNER_METADATA_DATABASE,spannerMetadataTableName=$SPANNER_METADATA_TABLE_NAME,spannerChangeStreamName=$SPANNER_CHANGE_STREAM_NAME,rpcPriority=$RPC_PRIORITY,spannerHost=$SPANNER_HOST,startTimestamp=$START_TIMESTAMP,endTimestamp=$END_TIMESTAMP,bigQueryDataset=$BIG_QUERY_DATASET,bigQueryProjectId=$BIG_QUERY_PROJECT_ID,bigQueryChangelogTableNameTemplate=$BIG_QUERY_CHANGELOG_TABLE_NAME_TEMPLATE,deadLetterQueueDirectory=$DEAD_LETTER_QUEUE_DIRECTORY,dlqRetryMinutes=$DLQ_RETRY_MINUTES,ignoreFields=$IGNORE_FIELDS,useStorageWriteApi=$USE_STORAGE_WRITE_API,useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE,numStorageWriteApiStreams=$NUM_STORAGE_WRITE_API_STREAMS,storageWriteApiTriggeringFrequencySec=$STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC" \
-pl v2/googlecloud-to-googlecloud \
-am
```
