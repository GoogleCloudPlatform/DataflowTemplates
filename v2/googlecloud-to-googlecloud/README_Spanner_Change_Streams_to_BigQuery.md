
Cloud Spanner change streams to BigQuery template
---
The Cloud Spanner change streams to BigQuery template is a streaming pipeline
that streams Cloud Spanner data change records and writes them into BigQuery
tables using Dataflow Runner V2.

All change stream watched columns are included in each BigQuery table row,
regardless of whether they are modified by a Cloud Spanner transaction. Columns
not watched are not included in the BigQuery row. Any Cloud Spanner change less
than the Dataflow watermark are either successfully applied to the BigQuery
tables or are stored in the dead-letter queue for retry. BigQuery rows are
inserted out of order compared to the original Cloud Spanner commit timestamp
ordering.

If the necessary BigQuery tables don't exist, the pipeline creates them.
Otherwise, existing BigQuery tables are used. The schema of existing BigQuery
tables must contain the corresponding tracked columns of the Cloud Spanner tables
and any additional metadata columns that are not ignored explicitly by the
ignoreFields option. See the description of the metadata fields in the following
list. Each new BigQuery row includes all columns watched by the change stream
from its corresponding row in your Cloud Spanner table at the change record's
timestamp.

The following metadata fields are added to BigQuery tables. For more details
about these fields, see Data change records in "Change streams partitions,
records, and queries."
- _metadata_spanner_mod_type: The modification type (insert, update, or delete)
of the Cloud Spanner transaction. Extracted from change stream data change
record.
- _metadata_spanner_table_name: The Cloud Spanner table name. Note this field is
not the metadata table name of the connector.
- _metadata_spanner_commit_timestamp: The Spanner commit timestamp, which is the
time when a change is committed. Extracted from change stream data change record.
- _metadata_spanner_server_transaction_id: A globally unique string that
represents the Spanner transaction in which the change was committed. Only use
this value in the context of processing change stream records. It isn't
correlated with the transaction ID in Spanner's API. Extracted from change stream
data change record.
- _metadata_spanner_record_sequence: The sequence number for the record within
the Spanner transaction. Sequence numbers are guaranteed to be unique and
monotonically increasing (but not necessarily contiguous) within a transaction.
Extracted from change stream data change record.
- _metadata_spanner_is_last_record_in_transaction_in_partition: Indicates whether
the record is the last record for a Spanner transaction in the current partition.
Extracted from change stream data change record.
- _metadata_spanner_number_of_records_in_transaction: The number of data change
records that are part of the Spanner transaction across all change stream
partitions. Extracted from change stream data change record.
- _metadata_spanner_number_of_partitions_in_transaction: The number of partitions
that return data change records for the Spanner transaction. Extracted from
change stream data change record.
- _metadata_big_query_commit_timestamp: The commit timestamp of when the row is
inserted into BigQuery.

Notes:
- This template does not propagate schema changes from Cloud Spanner to BigQuery.
Because performing a schema change in Cloud Spanner is likely going to break the
pipeline, you might need to recreate the pipeline after the schema change.
- For OLD_AND_NEW_VALUES and NEW_VALUES value capture types, when the data change
record contains an UPDATE change, the template needs to do a stale read to Cloud
Spanner at the commit timestamp of the data change record to retrieve the
unchanged but watched columns. Make sure to configure your database
'version_retention_period' properly for the stale read. For the NEW_ROW value
capture type, the template is more efficient, because the data change record
captures the full new row including columns that are not updated in UPDATEs, and
the template does not need to do a stale read.
- You can minimize network latency and network transport costs by running the
Dataflow job from the same region as your Cloud Spanner instance or BigQuery
tables. If you use sources, sinks, staging file locations, or temporary file
locations that are located outside of your job's region, your data might be sent
across regions. See more about Dataflow regional endpoints.
- This template supports all valid Cloud Spanner data types, but if the BigQuery
type is more precise than the Cloud Spanner type, precision loss might occur
during the transformation. Specifically:
- For Cloud Spanner JSON type, the order of the members of an object is
lexicographically ordered, but there is no such guarantee for BigQuery JSON type.
- Cloud Spanner supports nanoseconds TIMESTAMP type, BigQuery only supports
microseconds TIMESTAMP type.

Learn more about <a
href="https://cloud.google.com/spanner/docs/change-streams">change streams</a>,
<a href="https://cloud.google.com/spanner/docs/change-streams/use-dataflow">how
to build change streams Dataflow pipelines</a>, and <a
href="https://cloud.google.com/spanner/docs/change-streams/use-dataflow#best_practices">best
practices</a>.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/cloud-spanner-change-streams-to-bigquery)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Spanner_Change_Streams_to_BigQuery).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **spannerInstanceId**: The Spanner instance to read change streams from.
* **spannerDatabase**: The Spanner database to read change streams from.
* **spannerMetadataInstanceId**: The Spanner instance to use for the change streams connector metadata table.
* **spannerMetadataDatabase**: The Spanner database to use for the change streams connector metadata table.
* **spannerChangeStreamName**: The name of the Spanner change stream to read from.
* **bigQueryDataset**: The BigQuery dataset for change streams output.

### Optional parameters

* **spannerProjectId**: The project to read change streams from. This value is also the project where the change streams connector metadata table is created. The default value for this parameter is the project where the Dataflow pipeline is running.
* **spannerDatabaseRole**: The Spanner database role to use when running the template. This parameter is required only when the IAM principal who is running the template is a fine-grained access control user. The database role must have the `SELECT` privilege on the change stream and the `EXECUTE` privilege on the change stream's read function. For more information, see Fine-grained access control for change streams (https://cloud.google.com/spanner/docs/fgac-change-streams).
* **spannerMetadataTableName**: The Spanner change streams connector metadata table name to use. If not provided, a Spanner change streams connector metadata table is automatically created during the pipeline flow. You must provide this parameter when updating an existing pipeline. Otherwise, don't provide this parameter.
* **rpcPriority**: The request priority for Spanner calls. The value must be one of the following values: `HIGH`, `MEDIUM`, or `LOW`. The default value is `HIGH`.
* **spannerHost**: The Cloud Spanner endpoint to call in the template. Only used for testing. For example, `https://batch-spanner.googleapis.com`.
* **startTimestamp**: The starting DateTime (https://datatracker.ietf.org/doc/html/rfc3339), inclusive, to use for reading change streams. Ex-2021-10-12T07:20:50.52Z. Defaults to the timestamp when the pipeline starts, that is, the current time.
* **endTimestamp**: The ending DateTime (https://datatracker.ietf.org/doc/html/rfc3339), inclusive, to use for reading change streams.Ex-2021-10-12T07:20:50.52Z. Defaults to an infinite time in the future.
* **bigQueryProjectId**: The BigQuery project. The default value is the project for the Dataflow job.
* **bigQueryChangelogTableNameTemplate**: The template for the name of the BigQuery table that contains the changelog. Defaults to: {_metadata_spanner_table_name}_changelog.
* **deadLetterQueueDirectory**: The path to store any unprocessed records. The default path is a directory under the Dataflow job's temp location. The default value is usually sufficient.
* **dlqRetryMinutes**: The number of minutes between dead-letter queue retries. The default value is `10`.
* **ignoreFields**: A comma-separated list of fields (case sensitive) to ignore. These fields might be fields of watched tables, or metadata fields added by the pipeline. Ignored fields aren't inserted into BigQuery. When you ignore the _metadata_spanner_table_name field, the bigQueryChangelogTableNameTemplate parameter is also ignored. Defaults to empty.
* **disableDlqRetries**: Whether or not to disable retries for the DLQ. Defaults to: false.
* **useStorageWriteApi**: If true, the pipeline uses the BigQuery Storage Write API (https://cloud.google.com/bigquery/docs/write-api). The default value is `false`. For more information, see Using the Storage Write API (https://beam.apache.org/documentation/io/built-in/google-bigquery/#storage-write-api).
* **useStorageWriteApiAtLeastOnce**:  When using the Storage Write API, specifies the write semantics. To use at-least once semantics (https://beam.apache.org/documentation/io/built-in/google-bigquery/#at-least-once-semantics), set this parameter to `true`. To use exactly-once semantics, set the parameter to `false`. This parameter applies only when `useStorageWriteApi` is `true`. The default value is `false`.
* **numStorageWriteApiStreams**: When using the Storage Write API, specifies the number of write streams. If `useStorageWriteApi` is `true` and `useStorageWriteApiAtLeastOnce` is `false`, then you must set this parameter. Defaults to: 0.
* **storageWriteApiTriggeringFrequencySec**: When using the Storage Write API, specifies the triggering frequency, in seconds. If `useStorageWriteApi` is `true` and `useStorageWriteApiAtLeastOnce` is `false`, then you must set this parameter.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/googlecloud-to-googlecloud/src/main/java/com/google/cloud/teleport/v2/templates/spannerchangestreamstobigquery/SpannerChangeStreamsToBigQuery.java)

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
-DtemplateName="Spanner_Change_Streams_to_BigQuery" \
-f v2/googlecloud-to-googlecloud
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
export RPC_PRIORITY=HIGH
export SPANNER_HOST=<spannerHost>
export START_TIMESTAMP=""
export END_TIMESTAMP=""
export BIG_QUERY_PROJECT_ID=""
export BIG_QUERY_CHANGELOG_TABLE_NAME_TEMPLATE={_metadata_spanner_table_name}_changelog
export DEAD_LETTER_QUEUE_DIRECTORY=""
export DLQ_RETRY_MINUTES=10
export IGNORE_FIELDS=""
export DISABLE_DLQ_RETRIES=false
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
  --parameters "disableDlqRetries=$DISABLE_DLQ_RETRIES" \
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
export RPC_PRIORITY=HIGH
export SPANNER_HOST=<spannerHost>
export START_TIMESTAMP=""
export END_TIMESTAMP=""
export BIG_QUERY_PROJECT_ID=""
export BIG_QUERY_CHANGELOG_TABLE_NAME_TEMPLATE={_metadata_spanner_table_name}_changelog
export DEAD_LETTER_QUEUE_DIRECTORY=""
export DLQ_RETRY_MINUTES=10
export IGNORE_FIELDS=""
export DISABLE_DLQ_RETRIES=false
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
-Dparameters="spannerProjectId=$SPANNER_PROJECT_ID,spannerInstanceId=$SPANNER_INSTANCE_ID,spannerDatabase=$SPANNER_DATABASE,spannerDatabaseRole=$SPANNER_DATABASE_ROLE,spannerMetadataInstanceId=$SPANNER_METADATA_INSTANCE_ID,spannerMetadataDatabase=$SPANNER_METADATA_DATABASE,spannerMetadataTableName=$SPANNER_METADATA_TABLE_NAME,spannerChangeStreamName=$SPANNER_CHANGE_STREAM_NAME,rpcPriority=$RPC_PRIORITY,spannerHost=$SPANNER_HOST,startTimestamp=$START_TIMESTAMP,endTimestamp=$END_TIMESTAMP,bigQueryDataset=$BIG_QUERY_DATASET,bigQueryProjectId=$BIG_QUERY_PROJECT_ID,bigQueryChangelogTableNameTemplate=$BIG_QUERY_CHANGELOG_TABLE_NAME_TEMPLATE,deadLetterQueueDirectory=$DEAD_LETTER_QUEUE_DIRECTORY,dlqRetryMinutes=$DLQ_RETRY_MINUTES,ignoreFields=$IGNORE_FIELDS,disableDlqRetries=$DISABLE_DLQ_RETRIES,useStorageWriteApi=$USE_STORAGE_WRITE_API,useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE,numStorageWriteApiStreams=$NUM_STORAGE_WRITE_API_STREAMS,storageWriteApiTriggeringFrequencySec=$STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC" \
-f v2/googlecloud-to-googlecloud
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
cd v2/googlecloud-to-googlecloud/terraform/Spanner_Change_Streams_to_BigQuery
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

resource "google_dataflow_flex_template_job" "spanner_change_streams_to_bigquery" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Spanner_Change_Streams_to_BigQuery"
  name              = "spanner-change-streams-to-bigquery"
  region            = var.region
  parameters        = {
    spannerInstanceId = "<spannerInstanceId>"
    spannerDatabase = "<spannerDatabase>"
    spannerMetadataInstanceId = "<spannerMetadataInstanceId>"
    spannerMetadataDatabase = "<spannerMetadataDatabase>"
    spannerChangeStreamName = "<spannerChangeStreamName>"
    bigQueryDataset = "<bigQueryDataset>"
    # spannerProjectId = ""
    # spannerDatabaseRole = "<spannerDatabaseRole>"
    # spannerMetadataTableName = "<spannerMetadataTableName>"
    # rpcPriority = "HIGH"
    # spannerHost = "<spannerHost>"
    # startTimestamp = ""
    # endTimestamp = ""
    # bigQueryProjectId = ""
    # bigQueryChangelogTableNameTemplate = "{_metadata_spanner_table_name}_changelog"
    # deadLetterQueueDirectory = ""
    # dlqRetryMinutes = "10"
    # ignoreFields = ""
    # disableDlqRetries = "false"
    # useStorageWriteApi = "false"
    # useStorageWriteApiAtLeastOnce = "false"
    # numStorageWriteApiStreams = "0"
    # storageWriteApiTriggeringFrequencySec = "<storageWriteApiTriggeringFrequencySec>"
  }
}
```
