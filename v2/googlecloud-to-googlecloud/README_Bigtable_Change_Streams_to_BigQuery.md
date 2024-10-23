
Cloud Bigtable Change Streams to BigQuery template
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

### Required parameters

* **bigQueryDataset** : The dataset name of the destination BigQuery table.
* **bigtableChangeStreamAppProfile** : The Bigtable application profile ID. The application profile must use single-cluster routing and allow single-row transactions.
* **bigtableReadInstanceId** : The source Bigtable instance ID.
* **bigtableReadTableId** : The source Bigtable table ID.

### Optional parameters

* **writeRowkeyAsBytes** : Whether to write rowkeys as BigQuery `BYTES`. When set to `true`, row keys are written to the `BYTES` column. Otherwise, rowkeys are written to the `STRING` column. Defaults to `false`.
* **writeValuesAsBytes** : When set true values are written to BYTES column, otherwise to STRING column. Defaults to false.
* **writeNumericTimestamps** : Whether to write the Bigtable timestamp as BigQuery `INT64`. When set to true, values are written to the `INT64` column. Otherwise, values are written to the `TIMESTAMP` column. Columns affected: `timestamp`, `timestamp_from`, and `timestamp_to`. Defaults to `false`. When set to `true`, the time is measured in microseconds since the Unix epoch (January 1, 1970 at UTC).
* **bigQueryProjectId** : The BigQuery dataset project ID. The default is the project for the Dataflow job.
* **bigQueryChangelogTableName** : Destination BigQuery table name. If not specified, the value `bigtableReadTableId + "_changelog"` is used. Defaults to empty.
* **bigQueryChangelogTablePartitionGranularity** : Specifies a granularity for partitioning the changelog table. When set, the table is partitioned. Use one of the following supported values: `HOUR`, `DAY`, `MONTH`, or `YEAR`. By default, the table isn't partitioned.
* **bigQueryChangelogTablePartitionExpirationMs** : Sets the changelog table partition expiration time, in milliseconds. When set to true, partitions older than the specified number of milliseconds are deleted. By default, no expiration is set.
* **bigQueryChangelogTableFieldsToIgnore** : A comma-separated list of the changelog columns that, when specified, aren't created and populated. Use one of the following supported values: `is_gc`, `source_instance`, `source_cluster`, `source_table`, `tiebreaker`, or `big_query_commit_timestamp`. By default, all columns are populated.
* **dlqDirectory** : The directory to use for the dead-letter queue. Records that fail to be processed are stored in this directory. The default is a directory under the Dataflow job's temp location. In most cases, you can use the default path.
* **bigtableChangeStreamMetadataInstanceId** : The Bigtable change streams metadata instance ID. Defaults to empty.
* **bigtableChangeStreamMetadataTableTableId** : The ID of the Bigtable change streams connector metadata table. If not provided, a Bigtable change streams connector metadata table is automatically created during pipeline execution. Defaults to empty.
* **bigtableChangeStreamCharset** : The Bigtable change streams charset name. Defaults to: UTF-8.
* **bigtableChangeStreamStartTimestamp** : The starting timestamp (https://tools.ietf.org/html/rfc3339), inclusive, to use for reading change streams. For example, `2022-05-05T07:59:59Z`. Defaults to the timestamp of the pipeline start time.
* **bigtableChangeStreamIgnoreColumnFamilies** : A comma-separated list of column family name changes to ignore. Defaults to empty.
* **bigtableChangeStreamIgnoreColumns** : A comma-separated list of column name changes to ignore. Defaults to empty.
* **bigtableChangeStreamName** : A unique name for the client pipeline. Lets you resume processing from the point at which a previously running pipeline stopped. Defaults to an automatically generated name. See the Dataflow job logs for the value used.
* **bigtableChangeStreamResume** : When set to `true`, a new pipeline resumes processing from the point at which a previously running pipeline with the same `bigtableChangeStreamName` value stopped. If the pipeline with the given `bigtableChangeStreamName` value has never run, a new pipeline doesn't start. When set to `false`, a new pipeline starts. If a pipeline with the same `bigtableChangeStreamName` value has already run for the given source, a new pipeline doesn't start. Defaults to `false`.
* **bigtableReadProjectId** : The Bigtable project ID. The default is the project for the Dataflow job.



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
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#templates-plugin).

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
-f v2/googlecloud-to-googlecloud
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
-f v2/googlecloud-to-googlecloud
```

#### Troubleshooting
If there are compilation errors related to template metadata or template plugin framework,
make sure the plugin dependencies are up-to-date by running:
```
mvn clean install -pl plugins/templates-maven-plugin,metadata -am
```
See [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#templates-plugin)
for more information.



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
cd v2/googlecloud-to-googlecloud/terraform/Bigtable_Change_Streams_to_BigQuery
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

resource "google_dataflow_flex_template_job" "bigtable_change_streams_to_bigquery" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Bigtable_Change_Streams_to_BigQuery"
  name              = "bigtable-change-streams-to-bigquery"
  region            = var.region
  parameters        = {
    bigQueryDataset = "<bigQueryDataset>"
    bigtableChangeStreamAppProfile = "<bigtableChangeStreamAppProfile>"
    bigtableReadInstanceId = "<bigtableReadInstanceId>"
    bigtableReadTableId = "<bigtableReadTableId>"
    # writeRowkeyAsBytes = "false"
    # writeValuesAsBytes = "false"
    # writeNumericTimestamps = "false"
    # bigQueryProjectId = ""
    # bigQueryChangelogTableName = ""
    # bigQueryChangelogTablePartitionGranularity = ""
    # bigQueryChangelogTablePartitionExpirationMs = "<bigQueryChangelogTablePartitionExpirationMs>"
    # bigQueryChangelogTableFieldsToIgnore = "<bigQueryChangelogTableFieldsToIgnore>"
    # dlqDirectory = ""
    # bigtableChangeStreamMetadataInstanceId = ""
    # bigtableChangeStreamMetadataTableTableId = ""
    # bigtableChangeStreamCharset = "UTF-8"
    # bigtableChangeStreamStartTimestamp = ""
    # bigtableChangeStreamIgnoreColumnFamilies = ""
    # bigtableChangeStreamIgnoreColumns = ""
    # bigtableChangeStreamName = "<bigtableChangeStreamName>"
    # bigtableChangeStreamResume = "false"
    # bigtableReadProjectId = ""
  }
}
```
