
Dataplex: Tier Data from BigQuery to Cloud Storage template
---
A pipeline that exports all tables from a BigQuery dataset to Cloud Storage,
registering metadata for the newly created files in Dataplex.



:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required Parameters

* **sourceBigQueryDataset** (Source BigQuery dataset.): Dataplex asset name for the BigQuery dataset to tier data from. Format: projects/<name>/locations/<loc>/lakes/<lake-name>/zones/<zone-name>/assets/<asset name> (Dataplex asset name) or projects/<name>/datasets/<dataset-id> (BigQuery dataset ID).
* **destinationStorageBucketAssetName** (Dataplex asset name for the destination Cloud Storage bucket.): Dataplex asset name for the Cloud Storage bucket to tier data to. Format: projects/<name>/locations/<loc>/lakes/<lake-name>/zones/<zone-name>/assets/<asset name>.
* **maxParallelBigQueryMetadataRequests** (Maximum number of parallel requests.): The maximum number of parallel requests that will be sent to BigQuery when loading table/partition metadata. Defaults to: 5.

### Optional Parameters

* **tables** (Source BigQuery tables to tier.): A comma-separated list of BigQuery tables to tier. If none specified, all tables will be tiered. Tables should be specified by their name only (no project/dataset prefix). Case-sensitive!.
* **exportDataModifiedBeforeDateTime** (Move data older than the date.): Move data older than this date (and optional time). For partitioned tables, move partitions last modified before this date/time. For non-partitioned tables, move if the table was last modified before this date/time. If not specified, move all tables / partitions. The date/time is parsed in the default time zone by default, but optional suffixes Z and +HH:mm are supported. Format: YYYY-MM-DD or YYYY-MM-DDTHH:mm:ss or YYYY-MM-DDTHH:mm:ss+03:00. Relative date/time (https://en.wikipedia.org/wiki/ISO_8601#Durations) is also supported. Format: -PnDTnHnMn.nS (must start with -P meaning time in the past).
* **fileFormat** (Output file format in Cloud Storage.): Output file format in Cloud Storage. Format: PARQUET or AVRO. Defaults to: PARQUET.
* **fileCompression** (Output file compression in Cloud Storage.): Output file compression. Format: UNCOMPRESSED, SNAPPY, GZIP, or BZIP2. BZIP2 not supported for PARQUET files. Defaults to: SNAPPY.
* **partitionIdRegExp** (Partition ID regular expression filter.): Process partitions with partition ID matching this regexp only. Default: process all.
* **writeDisposition** (Action that occurs if a destination file already exists.): Specifies the action that occurs if a destination file already exists. Format: OVERWRITE, FAIL, SKIP. If SKIP, only files that don't exist in the destination directory will be processed. If FAIL and at least one file already exists, no data will be processed and an error will be produced. Defaults to: SKIP.
* **enforceSamePartitionKey** (Enforce same partition key.): Whether to enforce the same partition key. Due to a BigQuery limitation, it's not possible to have a partitioned external table with the partition key (in the file path) to have the same name as one of the columns in the file. If this param is true (the default), the partition key of the target file will be set to the original partition column name and the column in the file will be renamed. If false, it's the partition key that will be renamed.
* **deleteSourceData** (Delete source data from BigQuery.): Whether to delete source data from BigQuery after a successful export. Format: true or false. Defaults to: false.
* **updateDataplexMetadata** (Update Dataplex metadata.): Whether to update Dataplex metadata for the newly created entities. Only supported for Cloud Storage destination. If enabled, the pipeline will automatically copy the schema from source to the destination Dataplex entities, and the automated Dataplex Discovery won't run for them. Use this flag in cases where you have managed schema at the source. Defaults to: false.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!
[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/dataplex/src/main/java/com/google/cloud/teleport/v2/templates/DataplexBigQueryToGcs.java)

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
-DtemplateName="Dataplex_BigQuery_to_GCS" \
-pl v2/dataplex \
-am
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Dataplex_BigQuery_to_GCS
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Dataplex_BigQuery_to_GCS"

### Required
export SOURCE_BIG_QUERY_DATASET=<sourceBigQueryDataset>
export DESTINATION_STORAGE_BUCKET_ASSET_NAME=<destinationStorageBucketAssetName>
export MAX_PARALLEL_BIG_QUERY_METADATA_REQUESTS=5

### Optional
export TABLES=<tables>
export EXPORT_DATA_MODIFIED_BEFORE_DATE_TIME=<exportDataModifiedBeforeDateTime>
export FILE_FORMAT=PARQUET
export FILE_COMPRESSION=SNAPPY
export PARTITION_ID_REG_EXP=<partitionIdRegExp>
export WRITE_DISPOSITION=SKIP
export ENFORCE_SAME_PARTITION_KEY=true
export DELETE_SOURCE_DATA=false
export UPDATE_DATAPLEX_METADATA=false

gcloud dataflow flex-template run "dataplex-bigquery-to-gcs-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "sourceBigQueryDataset=$SOURCE_BIG_QUERY_DATASET" \
  --parameters "tables=$TABLES" \
  --parameters "destinationStorageBucketAssetName=$DESTINATION_STORAGE_BUCKET_ASSET_NAME" \
  --parameters "exportDataModifiedBeforeDateTime=$EXPORT_DATA_MODIFIED_BEFORE_DATE_TIME" \
  --parameters "maxParallelBigQueryMetadataRequests=$MAX_PARALLEL_BIG_QUERY_METADATA_REQUESTS" \
  --parameters "fileFormat=$FILE_FORMAT" \
  --parameters "fileCompression=$FILE_COMPRESSION" \
  --parameters "partitionIdRegExp=$PARTITION_ID_REG_EXP" \
  --parameters "writeDisposition=$WRITE_DISPOSITION" \
  --parameters "enforceSamePartitionKey=$ENFORCE_SAME_PARTITION_KEY" \
  --parameters "deleteSourceData=$DELETE_SOURCE_DATA" \
  --parameters "updateDataplexMetadata=$UPDATE_DATAPLEX_METADATA"
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
export SOURCE_BIG_QUERY_DATASET=<sourceBigQueryDataset>
export DESTINATION_STORAGE_BUCKET_ASSET_NAME=<destinationStorageBucketAssetName>
export MAX_PARALLEL_BIG_QUERY_METADATA_REQUESTS=5

### Optional
export TABLES=<tables>
export EXPORT_DATA_MODIFIED_BEFORE_DATE_TIME=<exportDataModifiedBeforeDateTime>
export FILE_FORMAT=PARQUET
export FILE_COMPRESSION=SNAPPY
export PARTITION_ID_REG_EXP=<partitionIdRegExp>
export WRITE_DISPOSITION=SKIP
export ENFORCE_SAME_PARTITION_KEY=true
export DELETE_SOURCE_DATA=false
export UPDATE_DATAPLEX_METADATA=false

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="dataplex-bigquery-to-gcs-job" \
-DtemplateName="Dataplex_BigQuery_to_GCS" \
-Dparameters="sourceBigQueryDataset=$SOURCE_BIG_QUERY_DATASET,tables=$TABLES,destinationStorageBucketAssetName=$DESTINATION_STORAGE_BUCKET_ASSET_NAME,exportDataModifiedBeforeDateTime=$EXPORT_DATA_MODIFIED_BEFORE_DATE_TIME,maxParallelBigQueryMetadataRequests=$MAX_PARALLEL_BIG_QUERY_METADATA_REQUESTS,fileFormat=$FILE_FORMAT,fileCompression=$FILE_COMPRESSION,partitionIdRegExp=$PARTITION_ID_REG_EXP,writeDisposition=$WRITE_DISPOSITION,enforceSamePartitionKey=$ENFORCE_SAME_PARTITION_KEY,deleteSourceData=$DELETE_SOURCE_DATA,updateDataplexMetadata=$UPDATE_DATAPLEX_METADATA" \
-pl v2/dataplex \
-am
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

resource "google_dataflow_flex_template_job" "dataplex_bigquery_to_gcs" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Dataplex_BigQuery_to_GCS"
  name              = "dataplex-bigquery-to-gcs"
  region            = var.region
  parameters        = {
    sourceBigQueryDataset = "<sourceBigQueryDataset>"
    destinationStorageBucketAssetName = "<destinationStorageBucketAssetName>"
    maxParallelBigQueryMetadataRequests = "5"
    # tables = "<tables>"
    # exportDataModifiedBeforeDateTime = "<exportDataModifiedBeforeDateTime>"
    # fileFormat = "PARQUET"
    # fileCompression = "SNAPPY"
    # partitionIdRegExp = "<partitionIdRegExp>"
    # writeDisposition = "SKIP"
    # enforceSamePartitionKey = "true"
    # deleteSourceData = "false"
    # updateDataplexMetadata = "false"
  }
}
```
