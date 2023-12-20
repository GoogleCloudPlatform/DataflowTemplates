
Bigtable Change Streams to HBase Replicator template
---
A streaming pipeline that replicates Bigtable change stream mutations to HBase.



:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required Parameters

* **hbaseRootDir** (Hbase root directory): Hbase root directory, corresponds to hbase.rootdir.
* **hbaseZookeeperQuorumHost** (Zookeeper quorum host): Zookeeper quorum host, corresponds to hbase.zookeeper.quorum host.
* **bigtableChangeStreamAppProfile** (Cloud Bigtable application profile ID): The application profile is used to distinguish workload in Cloud Bigtable.
* **bigtableReadInstanceId** (Source Bigtable Instance ID): The ID of the Cloud Bigtable instance that contains the table.
* **bigtableReadTableId** (Source Cloud Bigtable table ID): The Cloud Bigtable table to read from.

### Optional Parameters

* **bidirectionalReplicationEnabled** (Bidirectional replication): Whether bidirectional replication between hbase and bigtable is enabled, adds additional logic to filter out hbase-replicated mutations. Defaults to: false.
* **cbtQualifier** (Source CBT qualifier): Bidirectional replication source CBT qualifier. Defaults to: BIDIRECTIONAL_REPL_SOURCE_CBT.
* **dryRunEnabled** (Dry run): When dry run is enabled, pipeline will not write to Hbase. Defaults to: false.
* **filterGCMutations** (Filter GC mutations): Filters out garbage collection Delete mutations from CBT. Defaults to: false.
* **hbaseQualifier** (Source Hbase qualifier): Bidirectional replication source Hbase qualifier. Defaults to: BIDIRECTIONAL_REPL_SOURCE_HBASE.
* **hbaseZookeeperQuorumPort** (Zookeeper quorum port): Zookeeper quorum port, corresponds to hbase.zookeeper.quorum port. Defaults to: 2181.
* **bigtableChangeStreamMetadataInstanceId** (Cloud Bigtable change streams metadata instance ID): The Cloud Bigtable instance to use for the change streams connector metadata table. Defaults to empty.
* **bigtableChangeStreamMetadataTableTableId** (Cloud Bigtable change streams metadata table ID): The Cloud Bigtable change streams connector metadata table ID to use. If not provided, a Cloud Bigtable change streams connector metadata table will automatically be created during the pipeline flow. Defaults to empty.
* **bigtableChangeStreamCharset** (Bigtable change streams charset name when reading values and column qualifiers): Bigtable change streams charset name when reading values and column qualifiers. Default is UTF-8.
* **bigtableChangeStreamStartTimestamp** (The timestamp to read change streams from): The starting DateTime, inclusive, to use for reading change streams (https://tools.ietf.org/html/rfc3339). For example, 2022-05-05T07:59:59Z. Defaults to the timestamp when the pipeline starts.
* **bigtableChangeStreamIgnoreColumnFamilies** (Cloud Bigtable change streams column families to ignore): A comma-separated list of column family names changes to which won't be captured. Defaults to empty.
* **bigtableChangeStreamIgnoreColumns** (Cloud Bigtable change streams columns to ignore): A comma-separated list of column names changes to which won't be captured. Defaults to empty.
* **bigtableChangeStreamName** (A unique name of the client pipeline): Allows to resume processing from the point where a previously running pipeline stopped.
* **bigtableChangeStreamResume** (Resume streaming with the same change stream name): When set to true< a new pipeline will resume processing from the point at which a previously running pipeline with the same bigtableChangeStreamName stopped. If pipeline with the given bigtableChangeStreamName never ran in the past, a new pipeline will fail to start. When set to false a new pipeline will be started. If pipeline with the same bigtableChangeStreamName already ran in the past for the given source, a new pipeline will fail to start. Defaults to false.
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

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/bigtable-changestreams-to-hbase/src/main/java/com/google/cloud/teleport/v2/templates/BigtableChangeStreamsToHBase.java)

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
-DtemplateName="Bigtable_Change_Streams_to_HBase" \
-f v2/bigtable-changestreams-to-hbase
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Bigtable_Change_Streams_to_HBase
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Bigtable_Change_Streams_to_HBase"

### Required
export HBASE_ROOT_DIR=<hbaseRootDir>
export HBASE_ZOOKEEPER_QUORUM_HOST=<hbaseZookeeperQuorumHost>
export BIGTABLE_CHANGE_STREAM_APP_PROFILE=<bigtableChangeStreamAppProfile>
export BIGTABLE_READ_INSTANCE_ID=<bigtableReadInstanceId>
export BIGTABLE_READ_TABLE_ID=<bigtableReadTableId>

### Optional
export BIDIRECTIONAL_REPLICATION_ENABLED=false
export CBT_QUALIFIER=BIDIRECTIONAL_REPL_SOURCE_CBT
export DRY_RUN_ENABLED=false
export FILTER_GCMUTATIONS=false
export HBASE_QUALIFIER=BIDIRECTIONAL_REPL_SOURCE_HBASE
export HBASE_ZOOKEEPER_QUORUM_PORT=2181
export BIGTABLE_CHANGE_STREAM_METADATA_INSTANCE_ID=""
export BIGTABLE_CHANGE_STREAM_METADATA_TABLE_TABLE_ID=""
export BIGTABLE_CHANGE_STREAM_CHARSET=UTF-8
export BIGTABLE_CHANGE_STREAM_START_TIMESTAMP=""
export BIGTABLE_CHANGE_STREAM_IGNORE_COLUMN_FAMILIES=""
export BIGTABLE_CHANGE_STREAM_IGNORE_COLUMNS=""
export BIGTABLE_CHANGE_STREAM_NAME=<bigtableChangeStreamName>
export BIGTABLE_CHANGE_STREAM_RESUME=false
export BIGTABLE_READ_PROJECT_ID=""
export BIGTABLE_READ_APP_PROFILE=default
export BIGTABLE_RPC_ATTEMPT_TIMEOUT_MS=<bigtableRpcAttemptTimeoutMs>
export BIGTABLE_RPC_TIMEOUT_MS=<bigtableRpcTimeoutMs>
export BIGTABLE_ADDITIONAL_RETRY_CODES=<bigtableAdditionalRetryCodes>

gcloud dataflow flex-template run "bigtable-change-streams-to-hbase-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "bidirectionalReplicationEnabled=$BIDIRECTIONAL_REPLICATION_ENABLED" \
  --parameters "cbtQualifier=$CBT_QUALIFIER" \
  --parameters "dryRunEnabled=$DRY_RUN_ENABLED" \
  --parameters "filterGCMutations=$FILTER_GCMUTATIONS" \
  --parameters "hbaseQualifier=$HBASE_QUALIFIER" \
  --parameters "hbaseRootDir=$HBASE_ROOT_DIR" \
  --parameters "hbaseZookeeperQuorumHost=$HBASE_ZOOKEEPER_QUORUM_HOST" \
  --parameters "hbaseZookeeperQuorumPort=$HBASE_ZOOKEEPER_QUORUM_PORT" \
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
export HBASE_ROOT_DIR=<hbaseRootDir>
export HBASE_ZOOKEEPER_QUORUM_HOST=<hbaseZookeeperQuorumHost>
export BIGTABLE_CHANGE_STREAM_APP_PROFILE=<bigtableChangeStreamAppProfile>
export BIGTABLE_READ_INSTANCE_ID=<bigtableReadInstanceId>
export BIGTABLE_READ_TABLE_ID=<bigtableReadTableId>

### Optional
export BIDIRECTIONAL_REPLICATION_ENABLED=false
export CBT_QUALIFIER=BIDIRECTIONAL_REPL_SOURCE_CBT
export DRY_RUN_ENABLED=false
export FILTER_GCMUTATIONS=false
export HBASE_QUALIFIER=BIDIRECTIONAL_REPL_SOURCE_HBASE
export HBASE_ZOOKEEPER_QUORUM_PORT=2181
export BIGTABLE_CHANGE_STREAM_METADATA_INSTANCE_ID=""
export BIGTABLE_CHANGE_STREAM_METADATA_TABLE_TABLE_ID=""
export BIGTABLE_CHANGE_STREAM_CHARSET=UTF-8
export BIGTABLE_CHANGE_STREAM_START_TIMESTAMP=""
export BIGTABLE_CHANGE_STREAM_IGNORE_COLUMN_FAMILIES=""
export BIGTABLE_CHANGE_STREAM_IGNORE_COLUMNS=""
export BIGTABLE_CHANGE_STREAM_NAME=<bigtableChangeStreamName>
export BIGTABLE_CHANGE_STREAM_RESUME=false
export BIGTABLE_READ_PROJECT_ID=""
export BIGTABLE_READ_APP_PROFILE=default
export BIGTABLE_RPC_ATTEMPT_TIMEOUT_MS=<bigtableRpcAttemptTimeoutMs>
export BIGTABLE_RPC_TIMEOUT_MS=<bigtableRpcTimeoutMs>
export BIGTABLE_ADDITIONAL_RETRY_CODES=<bigtableAdditionalRetryCodes>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="bigtable-change-streams-to-hbase-job" \
-DtemplateName="Bigtable_Change_Streams_to_HBase" \
-Dparameters="bidirectionalReplicationEnabled=$BIDIRECTIONAL_REPLICATION_ENABLED,cbtQualifier=$CBT_QUALIFIER,dryRunEnabled=$DRY_RUN_ENABLED,filterGCMutations=$FILTER_GCMUTATIONS,hbaseQualifier=$HBASE_QUALIFIER,hbaseRootDir=$HBASE_ROOT_DIR,hbaseZookeeperQuorumHost=$HBASE_ZOOKEEPER_QUORUM_HOST,hbaseZookeeperQuorumPort=$HBASE_ZOOKEEPER_QUORUM_PORT,bigtableChangeStreamMetadataInstanceId=$BIGTABLE_CHANGE_STREAM_METADATA_INSTANCE_ID,bigtableChangeStreamMetadataTableTableId=$BIGTABLE_CHANGE_STREAM_METADATA_TABLE_TABLE_ID,bigtableChangeStreamAppProfile=$BIGTABLE_CHANGE_STREAM_APP_PROFILE,bigtableChangeStreamCharset=$BIGTABLE_CHANGE_STREAM_CHARSET,bigtableChangeStreamStartTimestamp=$BIGTABLE_CHANGE_STREAM_START_TIMESTAMP,bigtableChangeStreamIgnoreColumnFamilies=$BIGTABLE_CHANGE_STREAM_IGNORE_COLUMN_FAMILIES,bigtableChangeStreamIgnoreColumns=$BIGTABLE_CHANGE_STREAM_IGNORE_COLUMNS,bigtableChangeStreamName=$BIGTABLE_CHANGE_STREAM_NAME,bigtableChangeStreamResume=$BIGTABLE_CHANGE_STREAM_RESUME,bigtableReadInstanceId=$BIGTABLE_READ_INSTANCE_ID,bigtableReadTableId=$BIGTABLE_READ_TABLE_ID,bigtableReadProjectId=$BIGTABLE_READ_PROJECT_ID,bigtableReadAppProfile=$BIGTABLE_READ_APP_PROFILE,bigtableRpcAttemptTimeoutMs=$BIGTABLE_RPC_ATTEMPT_TIMEOUT_MS,bigtableRpcTimeoutMs=$BIGTABLE_RPC_TIMEOUT_MS,bigtableAdditionalRetryCodes=$BIGTABLE_ADDITIONAL_RETRY_CODES" \
-f v2/bigtable-changestreams-to-hbase
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

resource "google_dataflow_flex_template_job" "bigtable_change_streams_to_hbase" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Bigtable_Change_Streams_to_HBase"
  name              = "bigtable-change-streams-to-hbase"
  region            = var.region
  parameters        = {
    hbaseRootDir = "<hbaseRootDir>"
    hbaseZookeeperQuorumHost = "<hbaseZookeeperQuorumHost>"
    bigtableChangeStreamAppProfile = "<bigtableChangeStreamAppProfile>"
    bigtableReadInstanceId = "<bigtableReadInstanceId>"
    bigtableReadTableId = "<bigtableReadTableId>"
    # bidirectionalReplicationEnabled = "false"
    # cbtQualifier = "BIDIRECTIONAL_REPL_SOURCE_CBT"
    # dryRunEnabled = "false"
    # filterGCMutations = "false"
    # hbaseQualifier = "BIDIRECTIONAL_REPL_SOURCE_HBASE"
    # hbaseZookeeperQuorumPort = "2181"
    # bigtableChangeStreamMetadataInstanceId = ""
    # bigtableChangeStreamMetadataTableTableId = ""
    # bigtableChangeStreamCharset = "UTF-8"
    # bigtableChangeStreamStartTimestamp = ""
    # bigtableChangeStreamIgnoreColumnFamilies = ""
    # bigtableChangeStreamIgnoreColumns = ""
    # bigtableChangeStreamName = "<bigtableChangeStreamName>"
    # bigtableChangeStreamResume = "false"
    # bigtableReadProjectId = ""
    # bigtableReadAppProfile = "default"
    # bigtableRpcAttemptTimeoutMs = "<bigtableRpcAttemptTimeoutMs>"
    # bigtableRpcTimeoutMs = "<bigtableRpcTimeoutMs>"
    # bigtableAdditionalRetryCodes = "RESOURCE_EXHAUSTED,DEADLINE_EXCEEDED"
  }
}
```
