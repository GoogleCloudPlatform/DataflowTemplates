BigQuery to Bigtable Template
---
A pipeline to export a BigQuery table into Bigtable.

:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/bigquery-to-bigtable)
on how to use it without having to build from sources.

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Mandatory Parameters

* **readIdColumn** (Unique identifier column): Name of the BigQuery column storing the unique identifier of the row.
* **bigtableWriteInstanceId** (Bigtable Instance ID): The ID of the Cloud Bigtable instance that contains the table.
* **bigtableWriteTableId** (Bigtable Table ID): The ID of the Cloud Bigtable table to write.
* **bigtableWriteColumnFamily** (The Bigtable Column Family): This specifies the column family to write data into.

### Optional Parameters

* **inputTableSpec** (BigQuery source table): BigQuery source table spec. (Example: bigquery-project:dataset.input_table).
* **outputDeadletterTable** (The dead-letter table name to output failed messages to BigQuery): Messages failed to reach the output table for all kind of reasons (e.g., mismatched schema, malformed json) are written to this table. If it doesn't exist, it will be created during pipeline execution. (Example: your-project-id:your-dataset.your-table-name).
* **query** (Input SQL query.): Query to be executed on the source to extract the data. (Example: select * from sampledb.sample_table).
* **useLegacySql** (Set to true to use legacy SQL): Set to true to use legacy SQL (only applicable if supplying query). Defaults to: false.
* **bigtableRpcAttemptTimeoutMs** (The timeout for an RPC attempt in milliseconds): This sets the timeout for an RPC attempt in milliseconds.
* **bigtableRpcTimeoutMs** (The total timeout for an RPC operation in milliseconds): This sets the total timeout for an RPC operation in milliseconds.
* **bigtableAdditionalRetryCodes** (The additional retry codes): This sets the additional retry codes, separated by ',' (Example: RESOURCE_EXHAUSTED,DEADLINE_EXCEEDED).
* **bigtableWriteAppProfile** (Bigtable App Profile): Bigtable App Profile to use for the export. The default for this parameter is the Bigtable instance's default app profile.
* **bigtableWriteProjectId** (Bigtable Project ID): The ID of the Google Cloud project of the Cloud Bigtable instance that you want to write data to.
* **bigtableBulkWriteLatencyTargetMs** (Bigtable's latency target in milliseconds for latency-based throttling): This enables latency-based throttling and specifies the target latency.
* **bigtableBulkWriteMaxRowKeyCount** (The max number of row keys in a Bigtable batch write operation): This sets the max number of row keys in a Bigtable batch write operation.
* **bigtableBulkWriteMaxRequestSizeBytes** (The max amount of bytes in a Bigtable batch write operation): This sets the max amount of bytes in a Bigtable batch write operation.

## Getting Started

### Requirements

* Java 11
* Maven
* Valid resources for mandatory parameters.
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
    * `gcloud auth login`
    * `gcloud auth application-default login`

The following instructions use the
[Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates#templates-plugin)
. Install the plugin with the following command to proceed:

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
-DtemplateName="BigQuery_to_Bigtable" \
-pl v2/bigquery-to-bigtable \
-am
```

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/BigQuery_to_Bigtable
```

The specific path should be copied as it will be used in the following steps.

#### Running the Template

**Using the staged template**:

You can use the path above run the template (or share with others for execution).

To start a job with that template at any time using `gcloud`, you can use:

```shell
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>
export REGION=us-central1
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/BigQuery_to_Bigtable"

### Mandatory
export READ_ID_COLUMN=<readIdColumn>
export BIGTABLE_WRITE_INSTANCE_ID=<bigtableWriteInstanceId>
export BIGTABLE_WRITE_TABLE_ID=<bigtableWriteTableId>
export BIGTABLE_WRITE_COLUMN_FAMILY=<bigtableWriteColumnFamily>

### Optional
export INPUT_TABLE_SPEC=<inputTableSpec>
export OUTPUT_DEADLETTER_TABLE=<outputDeadletterTable>
export QUERY=<query>
export USE_LEGACY_SQL=false
export BIGTABLE_RPC_ATTEMPT_TIMEOUT_MS=<bigtableRpcAttemptTimeoutMs>
export BIGTABLE_RPC_TIMEOUT_MS=<bigtableRpcTimeoutMs>
export BIGTABLE_ADDITIONAL_RETRY_CODES=<bigtableAdditionalRetryCodes>
export BIGTABLE_WRITE_APP_PROFILE="default"
export BIGTABLE_WRITE_PROJECT_ID=<bigtableWriteProjectId>
export BIGTABLE_BULK_WRITE_LATENCY_TARGET_MS=<bigtableBulkWriteLatencyTargetMs>
export BIGTABLE_BULK_WRITE_MAX_ROW_KEY_COUNT=<bigtableBulkWriteMaxRowKeyCount>
export BIGTABLE_BULK_WRITE_MAX_REQUEST_SIZE_BYTES=<bigtableBulkWriteMaxRequestSizeBytes>

gcloud dataflow flex-template run "bigquery-to-bigtable-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "readIdColumn=$READ_ID_COLUMN" \
  --parameters "inputTableSpec=$INPUT_TABLE_SPEC" \
  --parameters "outputDeadletterTable=$OUTPUT_DEADLETTER_TABLE" \
  --parameters "query=$QUERY" \
  --parameters "useLegacySql=$USE_LEGACY_SQL" \
  --parameters "bigtableRpcAttemptTimeoutMs=$BIGTABLE_RPC_ATTEMPT_TIMEOUT_MS" \
  --parameters "bigtableRpcTimeoutMs=$BIGTABLE_RPC_TIMEOUT_MS" \
  --parameters "bigtableAdditionalRetryCodes=$BIGTABLE_ADDITIONAL_RETRY_CODES" \
  --parameters "bigtableWriteInstanceId=$BIGTABLE_WRITE_INSTANCE_ID" \
  --parameters "bigtableWriteTableId=$BIGTABLE_WRITE_TABLE_ID" \
  --parameters "bigtableWriteColumnFamily=$BIGTABLE_WRITE_COLUMN_FAMILY" \
  --parameters "bigtableWriteAppProfile=$BIGTABLE_WRITE_APP_PROFILE" \
  --parameters "bigtableWriteProjectId=$BIGTABLE_WRITE_PROJECT_ID" \
  --parameters "bigtableBulkWriteLatencyTargetMs=$BIGTABLE_BULK_WRITE_LATENCY_TARGET_MS" \
  --parameters "bigtableBulkWriteMaxRowKeyCount=$BIGTABLE_BULK_WRITE_MAX_ROW_KEY_COUNT" \
  --parameters "bigtableBulkWriteMaxRequestSizeBytes=$BIGTABLE_BULK_WRITE_MAX_REQUEST_SIZE_BYTES"
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

### Mandatory
export READ_ID_COLUMN=<readIdColumn>
export BIGTABLE_WRITE_INSTANCE_ID=<bigtableWriteInstanceId>
export BIGTABLE_WRITE_TABLE_ID=<bigtableWriteTableId>
export BIGTABLE_WRITE_COLUMN_FAMILY=<bigtableWriteColumnFamily>

### Optional
export INPUT_TABLE_SPEC=<inputTableSpec>
export OUTPUT_DEADLETTER_TABLE=<outputDeadletterTable>
export QUERY=<query>
export USE_LEGACY_SQL=false
export BIGTABLE_RPC_ATTEMPT_TIMEOUT_MS=<bigtableRpcAttemptTimeoutMs>
export BIGTABLE_RPC_TIMEOUT_MS=<bigtableRpcTimeoutMs>
export BIGTABLE_ADDITIONAL_RETRY_CODES=<bigtableAdditionalRetryCodes>
export BIGTABLE_WRITE_APP_PROFILE="default"
export BIGTABLE_WRITE_PROJECT_ID=<bigtableWriteProjectId>
export BIGTABLE_BULK_WRITE_LATENCY_TARGET_MS=<bigtableBulkWriteLatencyTargetMs>
export BIGTABLE_BULK_WRITE_MAX_ROW_KEY_COUNT=<bigtableBulkWriteMaxRowKeyCount>
export BIGTABLE_BULK_WRITE_MAX_REQUEST_SIZE_BYTES=<bigtableBulkWriteMaxRequestSizeBytes>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="bigquery-to-bigtable-job" \
-DtemplateName="BigQuery_to_Bigtable" \
-Dparameters="readIdColumn=$READ_ID_COLUMN,inputTableSpec=$INPUT_TABLE_SPEC,outputDeadletterTable=$OUTPUT_DEADLETTER_TABLE,query=$QUERY,useLegacySql=$USE_LEGACY_SQL,bigtableRpcAttemptTimeoutMs=$BIGTABLE_RPC_ATTEMPT_TIMEOUT_MS,bigtableRpcTimeoutMs=$BIGTABLE_RPC_TIMEOUT_MS,bigtableAdditionalRetryCodes=$BIGTABLE_ADDITIONAL_RETRY_CODES,bigtableWriteInstanceId=$BIGTABLE_WRITE_INSTANCE_ID,bigtableWriteTableId=$BIGTABLE_WRITE_TABLE_ID,bigtableWriteColumnFamily=$BIGTABLE_WRITE_COLUMN_FAMILY,bigtableWriteAppProfile=$BIGTABLE_WRITE_APP_PROFILE,bigtableWriteProjectId=$BIGTABLE_WRITE_PROJECT_ID,bigtableBulkWriteLatencyTargetMs=$BIGTABLE_BULK_WRITE_LATENCY_TARGET_MS,bigtableBulkWriteMaxRowKeyCount=$BIGTABLE_BULK_WRITE_MAX_ROW_KEY_COUNT,bigtableBulkWriteMaxRequestSizeBytes=$BIGTABLE_BULK_WRITE_MAX_REQUEST_SIZE_BYTES" \
-pl v2/bigquery-to-bigtable \
-am
```
