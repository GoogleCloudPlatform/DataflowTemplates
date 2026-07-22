
Bigtable Change Streams to Bigtable Replicator template
---
A streaming pipeline that replicates Bigtable change stream mutations to another
Bigtable instance.



:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **bigtableChangeStreamAppProfile**: The Bigtable application profile ID. The application profile must use single-cluster routing and allow single-row transactions.
* **bigtableReadInstanceId**: The source Bigtable instance ID.
* **bigtableReadTableId**: The source Bigtable table ID.
* **bigtableWriteInstanceId**: The ID of the Bigtable instance that contains the table.
* **bigtableWriteTableId**: The ID of the Bigtable table to write to.
* **bigtableWriteColumnFamily**: The name of the column family of the Bigtable table to write data into.

### Optional parameters

* **bidirectionalReplicationEnabled**: Whether bidirectional replication between bigtable instances is enabled, adds additional logic to filter out replicated mutations. Defaults to: false.
* **cbtQualifier**: Bidirectional replication source CBT qualifier to append. Defaults to: BIDIRECTIONAL_REPL_SOURCE_CBT.
* **cbtFilterQualifier**: Bidirectional replication filter CBT qualifier to check and ignore. Defaults to: BIDIRECTIONAL_REPL_SOURCE_CBT.
* **dryRunEnabled**: When dry run is enabled, pipeline will not write to Bigtable. Defaults to: false.
* **filterGCMutations**: Filters out garbage collection Delete mutations from CBT. Defaults to: false.
* **addRedistribute**: When set to true, redistributes change stream mutations by their row key to balance load across workers. Defaults to: false.
* **bigtableChangeStreamMetadataInstanceId**: The Bigtable change streams metadata instance ID. Defaults to empty.
* **bigtableChangeStreamMetadataTableTableId**: The ID of the Bigtable change streams connector metadata table. If not provided, a Bigtable change streams connector metadata table is automatically created during pipeline execution. Defaults to empty.
* **bigtableChangeStreamCharset**: The Bigtable change streams charset name. Defaults to: UTF-8.
* **bigtableChangeStreamStartTimestamp**: The starting timestamp (https://tools.ietf.org/html/rfc3339), inclusive, to use for reading change streams. For example, `2022-05-05T07:59:59Z`. Defaults to the timestamp of the pipeline start time.
* **bigtableChangeStreamIgnoreColumnFamilies**: A comma-separated list of column family name changes to ignore. Defaults to empty.
* **bigtableChangeStreamIgnoreColumns**: A comma-separated list of column name changes to ignore. Example: "cf1:col1,cf2:col2". Defaults to empty.
* **bigtableChangeStreamName**: A unique name for the client pipeline. Lets you resume processing from the point at which a previously running pipeline stopped. Defaults to an automatically generated name. See the Dataflow job logs for the value used.
* **bigtableChangeStreamResume**: When set to `true`, a new pipeline resumes processing from the point at which a previously running pipeline with the same `bigtableChangeStreamName` value stopped. If the pipeline with the given `bigtableChangeStreamName` value has never run, a new pipeline doesn't start. When set to `false`, a new pipeline starts. If a pipeline with the same `bigtableChangeStreamName` value has already run for the given source, a new pipeline doesn't start. Defaults to `false`.
* **bigtableReadChangeStreamTimeoutMs**: The timeout for Bigtable ReadChangeStream requests in milliseconds.
* **bigtableReadProjectId**: The Bigtable project ID. The default is the project for the Dataflow job.
* **bigtableReadAppProfile**: Bigtable App Profile to use for reads. The default for this parameter is the Bigtable instance's default app profile.
* **bigtableRpcAttemptTimeoutMs**: The timeout for each Bigtable RPC attempt in milliseconds.
* **bigtableRpcTimeoutMs**: The total timeout for a Bigtable RPC operation in milliseconds.
* **bigtableAdditionalRetryCodes**: The additional retry codes. For example, `RESOURCE_EXHAUSTED,DEADLINE_EXCEEDED`.
* **bigtableWriteAppProfile**: The ID of the Bigtable application profile to use for the export. If you do not specify an app profile, Bigtable uses the default app profile (https://cloud.google.com/bigtable/docs/app-profiles#default-app-profile) of the instance.
* **bigtableWriteProjectId**: The ID of the Google Cloud project that contains the Bigtable instanceto write data to.
* **bigtableBulkWriteLatencyTargetMs**: The latency target of Bigtable in milliseconds for latency-based throttling.
* **bigtableBulkWriteMaxRowKeyCount**: The maximum number of row keys in a Bigtable batch write operation.
* **bigtableBulkWriteMaxRequestSizeBytes**: The maximum bytes to include per Bigtable batch write operation.
* **bigtableBulkWriteFlowControl**: When set to true, enables bulk write flow control which will useserver's signal to throttle the writes. Defaults to: false.



## Getting Started

### Requirements

* Java 17
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/googlecloud-to-googlecloud/src/main/java/com/google/cloud/teleport/v2/templates/bigtablechangestreamstobigtable/BigtableChangeStreamsToBigtable.java)

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#templates-plugin).

#### Validating the Template

This template has a validation command that is used to check code quality.

```shell
mvn clean install -PtemplatesValidate \
-DskipTests -am \
-pl v2/googlecloud-to-googlecloud
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
export ARTIFACT_REGISTRY_REPO=<region>-docker.pkg.dev/$PROJECT/<repo>

mvn clean package -PtemplatesStage  \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-DartifactRegistry="$ARTIFACT_REGISTRY_REPO" \
-DstagePrefix="templates" \
-DtemplateName="Bigtable_Change_Streams_to_Bigtable" \
-pl v2/googlecloud-to-googlecloud -am
```

The `-DartifactRegistry` parameter can be specified to set the artifact registry repository of the Flex Templates image.
If not provided, it defaults to `gcr.io/<project>`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Bigtable_Change_Streams_to_Bigtable
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Bigtable_Change_Streams_to_Bigtable"

### Required
export BIGTABLE_CHANGE_STREAM_APP_PROFILE=<bigtableChangeStreamAppProfile>
export BIGTABLE_READ_INSTANCE_ID=<bigtableReadInstanceId>
export BIGTABLE_READ_TABLE_ID=<bigtableReadTableId>
export BIGTABLE_WRITE_INSTANCE_ID=<bigtableWriteInstanceId>
export BIGTABLE_WRITE_TABLE_ID=<bigtableWriteTableId>
export BIGTABLE_WRITE_COLUMN_FAMILY=<bigtableWriteColumnFamily>

### Optional
export BIDIRECTIONAL_REPLICATION_ENABLED=false
export CBT_QUALIFIER=BIDIRECTIONAL_REPL_SOURCE_CBT
export CBT_FILTER_QUALIFIER=BIDIRECTIONAL_REPL_SOURCE_CBT
export DRY_RUN_ENABLED=false
export FILTER_GCMUTATIONS=false
export ADD_REDISTRIBUTE=false
export BIGTABLE_CHANGE_STREAM_METADATA_INSTANCE_ID=""
export BIGTABLE_CHANGE_STREAM_METADATA_TABLE_TABLE_ID=""
export BIGTABLE_CHANGE_STREAM_CHARSET=UTF-8
export BIGTABLE_CHANGE_STREAM_START_TIMESTAMP=""
export BIGTABLE_CHANGE_STREAM_IGNORE_COLUMN_FAMILIES=""
export BIGTABLE_CHANGE_STREAM_IGNORE_COLUMNS=""
export BIGTABLE_CHANGE_STREAM_NAME=<bigtableChangeStreamName>
export BIGTABLE_CHANGE_STREAM_RESUME=false
export BIGTABLE_READ_CHANGE_STREAM_TIMEOUT_MS=<bigtableReadChangeStreamTimeoutMs>
export BIGTABLE_READ_PROJECT_ID=""
export BIGTABLE_READ_APP_PROFILE=default
export BIGTABLE_RPC_ATTEMPT_TIMEOUT_MS=<bigtableRpcAttemptTimeoutMs>
export BIGTABLE_RPC_TIMEOUT_MS=<bigtableRpcTimeoutMs>
export BIGTABLE_ADDITIONAL_RETRY_CODES=<bigtableAdditionalRetryCodes>
export BIGTABLE_WRITE_APP_PROFILE=default
export BIGTABLE_WRITE_PROJECT_ID=<bigtableWriteProjectId>
export BIGTABLE_BULK_WRITE_LATENCY_TARGET_MS=<bigtableBulkWriteLatencyTargetMs>
export BIGTABLE_BULK_WRITE_MAX_ROW_KEY_COUNT=<bigtableBulkWriteMaxRowKeyCount>
export BIGTABLE_BULK_WRITE_MAX_REQUEST_SIZE_BYTES=<bigtableBulkWriteMaxRequestSizeBytes>
export BIGTABLE_BULK_WRITE_FLOW_CONTROL=false

gcloud dataflow flex-template run "bigtable-change-streams-to-bigtable-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "bidirectionalReplicationEnabled=$BIDIRECTIONAL_REPLICATION_ENABLED" \
  --parameters "cbtQualifier=$CBT_QUALIFIER" \
  --parameters "cbtFilterQualifier=$CBT_FILTER_QUALIFIER" \
  --parameters "dryRunEnabled=$DRY_RUN_ENABLED" \
  --parameters "filterGCMutations=$FILTER_GCMUTATIONS" \
  --parameters "addRedistribute=$ADD_REDISTRIBUTE" \
  --parameters "bigtableChangeStreamMetadataInstanceId=$BIGTABLE_CHANGE_STREAM_METADATA_INSTANCE_ID" \
  --parameters "bigtableChangeStreamMetadataTableTableId=$BIGTABLE_CHANGE_STREAM_METADATA_TABLE_TABLE_ID" \
  --parameters "bigtableChangeStreamAppProfile=$BIGTABLE_CHANGE_STREAM_APP_PROFILE" \
  --parameters "bigtableChangeStreamCharset=$BIGTABLE_CHANGE_STREAM_CHARSET" \
  --parameters "bigtableChangeStreamStartTimestamp=$BIGTABLE_CHANGE_STREAM_START_TIMESTAMP" \
  --parameters "bigtableChangeStreamIgnoreColumnFamilies=$BIGTABLE_CHANGE_STREAM_IGNORE_COLUMN_FAMILIES" \
  --parameters "bigtableChangeStreamIgnoreColumns=$BIGTABLE_CHANGE_STREAM_IGNORE_COLUMNS" \
  --parameters "bigtableChangeStreamName=$BIGTABLE_CHANGE_STREAM_NAME" \
  --parameters "bigtableChangeStreamResume=$BIGTABLE_CHANGE_STREAM_RESUME" \
  --parameters "bigtableReadChangeStreamTimeoutMs=$BIGTABLE_READ_CHANGE_STREAM_TIMEOUT_MS" \
  --parameters "bigtableReadInstanceId=$BIGTABLE_READ_INSTANCE_ID" \
  --parameters "bigtableReadTableId=$BIGTABLE_READ_TABLE_ID" \
  --parameters "bigtableReadProjectId=$BIGTABLE_READ_PROJECT_ID" \
  --parameters "bigtableReadAppProfile=$BIGTABLE_READ_APP_PROFILE" \
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
  --parameters "bigtableBulkWriteMaxRequestSizeBytes=$BIGTABLE_BULK_WRITE_MAX_REQUEST_SIZE_BYTES" \
  --parameters "bigtableBulkWriteFlowControl=$BIGTABLE_BULK_WRITE_FLOW_CONTROL"
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
export BIGTABLE_CHANGE_STREAM_APP_PROFILE=<bigtableChangeStreamAppProfile>
export BIGTABLE_READ_INSTANCE_ID=<bigtableReadInstanceId>
export BIGTABLE_READ_TABLE_ID=<bigtableReadTableId>
export BIGTABLE_WRITE_INSTANCE_ID=<bigtableWriteInstanceId>
export BIGTABLE_WRITE_TABLE_ID=<bigtableWriteTableId>
export BIGTABLE_WRITE_COLUMN_FAMILY=<bigtableWriteColumnFamily>

### Optional
export BIDIRECTIONAL_REPLICATION_ENABLED=false
export CBT_QUALIFIER=BIDIRECTIONAL_REPL_SOURCE_CBT
export CBT_FILTER_QUALIFIER=BIDIRECTIONAL_REPL_SOURCE_CBT
export DRY_RUN_ENABLED=false
export FILTER_GCMUTATIONS=false
export ADD_REDISTRIBUTE=false
export BIGTABLE_CHANGE_STREAM_METADATA_INSTANCE_ID=""
export BIGTABLE_CHANGE_STREAM_METADATA_TABLE_TABLE_ID=""
export BIGTABLE_CHANGE_STREAM_CHARSET=UTF-8
export BIGTABLE_CHANGE_STREAM_START_TIMESTAMP=""
export BIGTABLE_CHANGE_STREAM_IGNORE_COLUMN_FAMILIES=""
export BIGTABLE_CHANGE_STREAM_IGNORE_COLUMNS=""
export BIGTABLE_CHANGE_STREAM_NAME=<bigtableChangeStreamName>
export BIGTABLE_CHANGE_STREAM_RESUME=false
export BIGTABLE_READ_CHANGE_STREAM_TIMEOUT_MS=<bigtableReadChangeStreamTimeoutMs>
export BIGTABLE_READ_PROJECT_ID=""
export BIGTABLE_READ_APP_PROFILE=default
export BIGTABLE_RPC_ATTEMPT_TIMEOUT_MS=<bigtableRpcAttemptTimeoutMs>
export BIGTABLE_RPC_TIMEOUT_MS=<bigtableRpcTimeoutMs>
export BIGTABLE_ADDITIONAL_RETRY_CODES=<bigtableAdditionalRetryCodes>
export BIGTABLE_WRITE_APP_PROFILE=default
export BIGTABLE_WRITE_PROJECT_ID=<bigtableWriteProjectId>
export BIGTABLE_BULK_WRITE_LATENCY_TARGET_MS=<bigtableBulkWriteLatencyTargetMs>
export BIGTABLE_BULK_WRITE_MAX_ROW_KEY_COUNT=<bigtableBulkWriteMaxRowKeyCount>
export BIGTABLE_BULK_WRITE_MAX_REQUEST_SIZE_BYTES=<bigtableBulkWriteMaxRequestSizeBytes>
export BIGTABLE_BULK_WRITE_FLOW_CONTROL=false

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="bigtable-change-streams-to-bigtable-job" \
-DtemplateName="Bigtable_Change_Streams_to_Bigtable" \
-Dparameters="bidirectionalReplicationEnabled=$BIDIRECTIONAL_REPLICATION_ENABLED,cbtQualifier=$CBT_QUALIFIER,cbtFilterQualifier=$CBT_FILTER_QUALIFIER,dryRunEnabled=$DRY_RUN_ENABLED,filterGCMutations=$FILTER_GCMUTATIONS,addRedistribute=$ADD_REDISTRIBUTE,bigtableChangeStreamMetadataInstanceId=$BIGTABLE_CHANGE_STREAM_METADATA_INSTANCE_ID,bigtableChangeStreamMetadataTableTableId=$BIGTABLE_CHANGE_STREAM_METADATA_TABLE_TABLE_ID,bigtableChangeStreamAppProfile=$BIGTABLE_CHANGE_STREAM_APP_PROFILE,bigtableChangeStreamCharset=$BIGTABLE_CHANGE_STREAM_CHARSET,bigtableChangeStreamStartTimestamp=$BIGTABLE_CHANGE_STREAM_START_TIMESTAMP,bigtableChangeStreamIgnoreColumnFamilies=$BIGTABLE_CHANGE_STREAM_IGNORE_COLUMN_FAMILIES,bigtableChangeStreamIgnoreColumns=$BIGTABLE_CHANGE_STREAM_IGNORE_COLUMNS,bigtableChangeStreamName=$BIGTABLE_CHANGE_STREAM_NAME,bigtableChangeStreamResume=$BIGTABLE_CHANGE_STREAM_RESUME,bigtableReadChangeStreamTimeoutMs=$BIGTABLE_READ_CHANGE_STREAM_TIMEOUT_MS,bigtableReadInstanceId=$BIGTABLE_READ_INSTANCE_ID,bigtableReadTableId=$BIGTABLE_READ_TABLE_ID,bigtableReadProjectId=$BIGTABLE_READ_PROJECT_ID,bigtableReadAppProfile=$BIGTABLE_READ_APP_PROFILE,bigtableRpcAttemptTimeoutMs=$BIGTABLE_RPC_ATTEMPT_TIMEOUT_MS,bigtableRpcTimeoutMs=$BIGTABLE_RPC_TIMEOUT_MS,bigtableAdditionalRetryCodes=$BIGTABLE_ADDITIONAL_RETRY_CODES,bigtableWriteInstanceId=$BIGTABLE_WRITE_INSTANCE_ID,bigtableWriteTableId=$BIGTABLE_WRITE_TABLE_ID,bigtableWriteColumnFamily=$BIGTABLE_WRITE_COLUMN_FAMILY,bigtableWriteAppProfile=$BIGTABLE_WRITE_APP_PROFILE,bigtableWriteProjectId=$BIGTABLE_WRITE_PROJECT_ID,bigtableBulkWriteLatencyTargetMs=$BIGTABLE_BULK_WRITE_LATENCY_TARGET_MS,bigtableBulkWriteMaxRowKeyCount=$BIGTABLE_BULK_WRITE_MAX_ROW_KEY_COUNT,bigtableBulkWriteMaxRequestSizeBytes=$BIGTABLE_BULK_WRITE_MAX_REQUEST_SIZE_BYTES,bigtableBulkWriteFlowControl=$BIGTABLE_BULK_WRITE_FLOW_CONTROL" \
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
cd v2/googlecloud-to-googlecloud/terraform/Bigtable_Change_Streams_to_Bigtable
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

resource "google_dataflow_flex_template_job" "bigtable_change_streams_to_bigtable" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Bigtable_Change_Streams_to_Bigtable"
  name              = "bigtable-change-streams-to-bigtable"
  region            = var.region
  parameters        = {
    bigtableChangeStreamAppProfile = "<bigtableChangeStreamAppProfile>"
    bigtableReadInstanceId = "<bigtableReadInstanceId>"
    bigtableReadTableId = "<bigtableReadTableId>"
    bigtableWriteInstanceId = "<bigtableWriteInstanceId>"
    bigtableWriteTableId = "<bigtableWriteTableId>"
    bigtableWriteColumnFamily = "<bigtableWriteColumnFamily>"
    # bidirectionalReplicationEnabled = "false"
    # cbtQualifier = "BIDIRECTIONAL_REPL_SOURCE_CBT"
    # cbtFilterQualifier = "BIDIRECTIONAL_REPL_SOURCE_CBT"
    # dryRunEnabled = "false"
    # filterGCMutations = "false"
    # addRedistribute = "false"
    # bigtableChangeStreamMetadataInstanceId = ""
    # bigtableChangeStreamMetadataTableTableId = ""
    # bigtableChangeStreamCharset = "UTF-8"
    # bigtableChangeStreamStartTimestamp = ""
    # bigtableChangeStreamIgnoreColumnFamilies = ""
    # bigtableChangeStreamIgnoreColumns = ""
    # bigtableChangeStreamName = "<bigtableChangeStreamName>"
    # bigtableChangeStreamResume = "false"
    # bigtableReadChangeStreamTimeoutMs = "<bigtableReadChangeStreamTimeoutMs>"
    # bigtableReadProjectId = ""
    # bigtableReadAppProfile = "default"
    # bigtableRpcAttemptTimeoutMs = "<bigtableRpcAttemptTimeoutMs>"
    # bigtableRpcTimeoutMs = "<bigtableRpcTimeoutMs>"
    # bigtableAdditionalRetryCodes = "<bigtableAdditionalRetryCodes>"
    # bigtableWriteAppProfile = "default"
    # bigtableWriteProjectId = "<bigtableWriteProjectId>"
    # bigtableBulkWriteLatencyTargetMs = "<bigtableBulkWriteLatencyTargetMs>"
    # bigtableBulkWriteMaxRowKeyCount = "<bigtableBulkWriteMaxRowKeyCount>"
    # bigtableBulkWriteMaxRequestSizeBytes = "<bigtableBulkWriteMaxRequestSizeBytes>"
    # bigtableBulkWriteFlowControl = "false"
  }
}
```
