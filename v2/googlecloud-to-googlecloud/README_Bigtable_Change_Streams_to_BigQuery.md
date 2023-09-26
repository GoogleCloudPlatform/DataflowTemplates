
Cloud Bigtable change streams to BigQuery template
---
Streaming pipeline. Streams Bigtable data change records and writes them into
BigQuery using Dataflow Runner V2.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/cloud-bigtable-change-streams-to-bigquery)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Bigtable_Change_Streams_to_BigQuery).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required Parameters

* **bigQueryDataset** (BigQuery dataset): The BigQuery dataset for change streams output.
* **bigtableChangeStreamAppProfile** (Cloud Bigtable application profile ID): The application profile is used to distinguish workload in Cloud Bigtable.
* **bigtableReadInstanceId** (Source Bigtable Instance ID): The ID of the Cloud Bigtable instance that contains the table.
* **bigtableReadTableId** (Source Cloud Bigtable table ID): The Cloud Bigtable table to read from.

### Optional Parameters

* **writeRowkeyAsBytes** (Write rowkeys as BigQuery BYTES): When set true rowkeys are written to BYTES column, otherwise to STRING column. Defaults to false.
* **writeValuesAsBytes** (Write values as BigQuery BYTES): When set true values are written to BYTES column, otherwise to STRING column. Defaults to false.
* **writeNumericTimestamps** (Write Bigtable timestamp as BigQuery INT): When set true values are written to INT column, otherwise to TIMESTAMP column. Columns affected: `timestamp`, `timestamp_from`, `timestamp_to`. Defaults to false. When set to true the value is a number of microseconds since midnight of 01-JAN-1970.
* **bigQueryProjectId** (BigQuery project ID): The BigQuery Project. Default is the project for the Dataflow job.
* **bigQueryChangelogTableName** (BigQuery changelog table name): The BigQuery table name that contains the changelog records. Default: {bigtableTableId}_changelog.
* **bigQueryChangelogTablePartitionGranularity** (Changelog table will be partitioned at specified granularity): When set, table partitioning will be in effect. Accepted values: `HOUR`, `DAY`, `MONTH`, `YEAR`. Default is no partitioning.
* **bigQueryChangelogTablePartitionExpirationMs** (Sets partition expiration time in milliseconds): When set true partitions older than specified number of milliseconds will be deleted. Default is no expiration.
* **bigQueryChangelogTableFieldsToIgnore** (Optional changelog table columns to be disabled): A comma-separated list of the changelog columns which will not be created and populated if specified. Supported values should be from the following list: `is_gc`, `source_instance`, `source_cluster`, `source_table`, `tiebreaker`, `big_query_commit_timestamp`. Defaults to all columns to be populated.
* **dlqDirectory** (Dead letter queue directory): The file path to store any unprocessed records with the reason they failed to be processed. Default is a directory under the Dataflow job's temp location. The default value is enough under most conditions.
* **bigtableChangeStreamMetadataInstanceId** (Cloud Bigtable change streams metadata instance ID): The Cloud Bigtable instance to use for the change streams connector metadata table. Defaults to empty.
* **bigtableChangeStreamMetadataTableTableId** (Cloud Bigtable change streams metadata table ID): The Cloud Bigtable change streams connector metadata table ID to use. If not provided, a Cloud Bigtable change streams connector metadata table will automatically be created during the pipeline flow. Defaults to empty.
* **bigtableChangeStreamCharset** (Bigtable change streams charset name when reading values and column qualifiers): Bigtable change streams charset name when reading values and column qualifiers. Default is UTF-8.
* **bigtableChangeStreamStartTimestamp** (The timestamp to read change streams from): The starting DateTime, inclusive, to use for reading change streams (https://tools.ietf.org/html/rfc3339). For example, 2022-05-05T07:59:59Z. Defaults to the timestamp when the pipeline starts.
* **bigtableChangeStreamIgnoreColumnFamilies** (Cloud Bigtable change streams column families to ignore): A comma-separated list of column family names changes to which won't be captured. Defaults to empty.
* **bigtableChangeStreamIgnoreColumns** (Cloud Bigtable change streams columns to ignore): A comma-separated list of column names changes to which won't be captured. Defaults to empty.
* **bigtableChangeStreamName** (A unique name of the client pipeline): Allows to resume processing from the point where a previously running pipeline stopped.
* **bigtableChangeStreamResume** (Resume streaming with the same change stream name): When set to true< a new pipeline will resume processing from the point at which a previously running pipeline with the same bigtableChangeStreamName stopped. If pipeline with the given bigtableChangeStreamName never ran in the past, a new pipeline will fail to start. When set to false a new pipeline will be started. If pipeline with the same bigtableChangeStreamName already ran in the past for the given source, a new pipeline will fail to start. Defaults to false.
* **bigtableReadProjectId** (Source Cloud Bigtable Project ID): Project to read Cloud Bigtable data from. The default for this parameter is the project where the Dataflow pipeline is running.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!
[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/googlecloud-to-googlecloud/src/main/java/com/google/cloud/teleport/v2/templates/bigtablechangestreamstobigquery/BigtableChangeStreamsToBigQuery.java)

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
-DtemplateName="Bigtable_Change_Streams_to_BigQuery" \
-pl v2/googlecloud-to-googlecloud \
-am
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Bigtable_Change_Streams_to_BigQuery
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Bigtable_Change_Streams_to_BigQuery"

### Required
export BIG_QUERY_DATASET=<bigQueryDataset>
export BIGTABLE_CHANGE_STREAM_APP_PROFILE=<bigtableChangeStreamAppProfile>
export BIGTABLE_READ_INSTANCE_ID=<bigtableReadInstanceId>
export BIGTABLE_READ_TABLE_ID=<bigtableReadTableId>

### Optional
export WRITE_ROWKEY_AS_BYTES=false
export WRITE_VALUES_AS_BYTES=false
export WRITE_NUMERIC_TIMESTAMPS=false
export BIG_QUERY_PROJECT_ID=""
export BIG_QUERY_CHANGELOG_TABLE_NAME=""
export BIG_QUERY_CHANGELOG_TABLE_PARTITION_GRANULARITY=""
export BIG_QUERY_CHANGELOG_TABLE_PARTITION_EXPIRATION_MS=<bigQueryChangelogTablePartitionExpirationMs>
export BIG_QUERY_CHANGELOG_TABLE_FIELDS_TO_IGNORE=<bigQueryChangelogTableFieldsToIgnore>
export DLQ_DIRECTORY=""
export BIGTABLE_CHANGE_STREAM_METADATA_INSTANCE_ID=""
export BIGTABLE_CHANGE_STREAM_METADATA_TABLE_TABLE_ID=""
export BIGTABLE_CHANGE_STREAM_CHARSET=UTF-8
export BIGTABLE_CHANGE_STREAM_START_TIMESTAMP=""
export BIGTABLE_CHANGE_STREAM_IGNORE_COLUMN_FAMILIES=""
export BIGTABLE_CHANGE_STREAM_IGNORE_COLUMNS=""
export BIGTABLE_CHANGE_STREAM_NAME=<bigtableChangeStreamName>
export BIGTABLE_CHANGE_STREAM_RESUME=false
export BIGTABLE_READ_PROJECT_ID=""

gcloud dataflow flex-template run "bigtable-change-streams-to-bigquery-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "bigQueryDataset=$BIG_QUERY_DATASET" \
  --parameters "writeRowkeyAsBytes=$WRITE_ROWKEY_AS_BYTES" \
  --parameters "writeValuesAsBytes=$WRITE_VALUES_AS_BYTES" \
  --parameters "writeNumericTimestamps=$WRITE_NUMERIC_TIMESTAMPS" \
  --parameters "bigQueryProjectId=$BIG_QUERY_PROJECT_ID" \
  --parameters "bigQueryChangelogTableName=$BIG_QUERY_CHANGELOG_TABLE_NAME" \
  --parameters "bigQueryChangelogTablePartitionGranularity=$BIG_QUERY_CHANGELOG_TABLE_PARTITION_GRANULARITY" \
  --parameters "bigQueryChangelogTablePartitionExpirationMs=$BIG_QUERY_CHANGELOG_TABLE_PARTITION_EXPIRATION_MS" \
  --parameters "bigQueryChangelogTableFieldsToIgnore=$BIG_QUERY_CHANGELOG_TABLE_FIELDS_TO_IGNORE" \
  --parameters "dlqDirectory=$DLQ_DIRECTORY" \
  --parameters "bigtableChangeStreamMetadataInstanceId=$BIGTABLE_CHANGE_STREAM_METADATA_INSTANCE_ID" \
  --parameters "bigtableChangeStreamMetadataTableTableId=$BIGTABLE_CHANGE_STREAM_METADATA_TABLE_TABLE_ID" \
  --parameters "bigtableChangeStreamAppProfile=$BIGTABLE_CHANGE_STREAM_APP_PROFILE" \
  --parameters "bigtableChangeStreamCharset=$BIGTABLE_CHANGE_STREAM_CHARSET" \
  --parameters "bigtableChangeStreamStartTimestamp=$BIGTABLE_CHANGE_STREAM_START_TIMESTAMP" \
  --parameters "bigtableChangeStreamIgnoreColumnFamilies=$BIGTABLE_CHANGE_STREAM_IGNORE_COLUMN_FAMILIES" \
  --parameters "bigtableChangeStreamIgnoreColumns=$BIGTABLE_CHANGE_STREAM_IGNORE_COLUMNS" \
  --parameters "bigtableChangeStreamName=$BIGTABLE_CHANGE_STREAM_NAME" \
  --parameters "bigtableChangeStreamResume=$BIGTABLE_CHANGE_STREAM_RESUME" \
  --parameters "bigtableReadInstanceId=$BIGTABLE_READ_INSTANCE_ID" \
  --parameters "bigtableReadTableId=$BIGTABLE_READ_TABLE_ID" \
  --parameters "bigtableReadProjectId=$BIGTABLE_READ_PROJECT_ID"
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
export BIG_QUERY_DATASET=<bigQueryDataset>
export BIGTABLE_CHANGE_STREAM_APP_PROFILE=<bigtableChangeStreamAppProfile>
export BIGTABLE_READ_INSTANCE_ID=<bigtableReadInstanceId>
export BIGTABLE_READ_TABLE_ID=<bigtableReadTableId>

### Optional
export WRITE_ROWKEY_AS_BYTES=false
export WRITE_VALUES_AS_BYTES=false
export WRITE_NUMERIC_TIMESTAMPS=false
export BIG_QUERY_PROJECT_ID=""
export BIG_QUERY_CHANGELOG_TABLE_NAME=""
export BIG_QUERY_CHANGELOG_TABLE_PARTITION_GRANULARITY=""
export BIG_QUERY_CHANGELOG_TABLE_PARTITION_EXPIRATION_MS=<bigQueryChangelogTablePartitionExpirationMs>
export BIG_QUERY_CHANGELOG_TABLE_FIELDS_TO_IGNORE=<bigQueryChangelogTableFieldsToIgnore>
export DLQ_DIRECTORY=""
export BIGTABLE_CHANGE_STREAM_METADATA_INSTANCE_ID=""
export BIGTABLE_CHANGE_STREAM_METADATA_TABLE_TABLE_ID=""
export BIGTABLE_CHANGE_STREAM_CHARSET=UTF-8
export BIGTABLE_CHANGE_STREAM_START_TIMESTAMP=""
export BIGTABLE_CHANGE_STREAM_IGNORE_COLUMN_FAMILIES=""
export BIGTABLE_CHANGE_STREAM_IGNORE_COLUMNS=""
export BIGTABLE_CHANGE_STREAM_NAME=<bigtableChangeStreamName>
export BIGTABLE_CHANGE_STREAM_RESUME=false
export BIGTABLE_READ_PROJECT_ID=""

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="bigtable-change-streams-to-bigquery-job" \
-DtemplateName="Bigtable_Change_Streams_to_BigQuery" \
-Dparameters="bigQueryDataset=$BIG_QUERY_DATASET,writeRowkeyAsBytes=$WRITE_ROWKEY_AS_BYTES,writeValuesAsBytes=$WRITE_VALUES_AS_BYTES,writeNumericTimestamps=$WRITE_NUMERIC_TIMESTAMPS,bigQueryProjectId=$BIG_QUERY_PROJECT_ID,bigQueryChangelogTableName=$BIG_QUERY_CHANGELOG_TABLE_NAME,bigQueryChangelogTablePartitionGranularity=$BIG_QUERY_CHANGELOG_TABLE_PARTITION_GRANULARITY,bigQueryChangelogTablePartitionExpirationMs=$BIG_QUERY_CHANGELOG_TABLE_PARTITION_EXPIRATION_MS,bigQueryChangelogTableFieldsToIgnore=$BIG_QUERY_CHANGELOG_TABLE_FIELDS_TO_IGNORE,dlqDirectory=$DLQ_DIRECTORY,bigtableChangeStreamMetadataInstanceId=$BIGTABLE_CHANGE_STREAM_METADATA_INSTANCE_ID,bigtableChangeStreamMetadataTableTableId=$BIGTABLE_CHANGE_STREAM_METADATA_TABLE_TABLE_ID,bigtableChangeStreamAppProfile=$BIGTABLE_CHANGE_STREAM_APP_PROFILE,bigtableChangeStreamCharset=$BIGTABLE_CHANGE_STREAM_CHARSET,bigtableChangeStreamStartTimestamp=$BIGTABLE_CHANGE_STREAM_START_TIMESTAMP,bigtableChangeStreamIgnoreColumnFamilies=$BIGTABLE_CHANGE_STREAM_IGNORE_COLUMN_FAMILIES,bigtableChangeStreamIgnoreColumns=$BIGTABLE_CHANGE_STREAM_IGNORE_COLUMNS,bigtableChangeStreamName=$BIGTABLE_CHANGE_STREAM_NAME,bigtableChangeStreamResume=$BIGTABLE_CHANGE_STREAM_RESUME,bigtableReadInstanceId=$BIGTABLE_READ_INSTANCE_ID,bigtableReadTableId=$BIGTABLE_READ_TABLE_ID,bigtableReadProjectId=$BIGTABLE_READ_PROJECT_ID" \
-pl v2/googlecloud-to-googlecloud \
-am
```
