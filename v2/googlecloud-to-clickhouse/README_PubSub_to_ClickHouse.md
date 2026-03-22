
Pub/Sub to ClickHouse template
---
The Pub/Sub to ClickHouse template is a streaming pipeline that reads
JSON-encoded messages from a Pub/Sub subscription and writes them to a ClickHouse
table. Messages that fail due to schema mismatch or malformed JSON are routed to
a dead-letter destination: a ClickHouse table, a Pub/Sub topic, or both —
determined implicitly by which of --clickHouseDeadLetterTable and
--deadLetterTopic are provided. The target table and any ClickHouse dead-letter
table must exist prior to execution.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/pubsub-to-clickhouse)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=PubSub_to_ClickHouse).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **inputSubscription**: Pub/Sub subscription to read messages from. For example, `projects/<PROJECT_ID>/subscriptions/<SUBSCRIPTION_NAME>`.
* **clickHouseUrl**: The ClickHouse endpoint URL. Use https:// for SSL connections (ClickHouse Cloud) or http:// for non-SSL connections. For example, `https://<HOST>:8443 or http://<HOST>:8123`.
* **clickHouseDatabase**: The name of the ClickHouse database where the target table resides. For example, `default`.
* **clickHouseTable**: The name of the ClickHouse table to write data into. The table must exist before running the pipeline. For example, `my_table`.
* **clickHouseUsername**: The username to use for authenticating with ClickHouse. For example, `default`.
* **clickHousePassword**: The password to use for authenticating with ClickHouse.

### Optional parameters

* **clickHouseDeadLetterTable**: The ClickHouse table to write failed messages into. If set, failed messages are written to this table. Can be combined with --deadLetterTopic to write to both destinations simultaneously. At least one of --clickHouseDeadLetterTable or --deadLetterTopic must be provided. The table must exist in ClickHouse with the following schema: (raw_message String, error_message String, stack_trace String, failed_at String). For example, `my_table_dead_letter`.
* **deadLetterTopic**: The Pub/Sub topic to publish failed messages to. If set, failed messages are published to this topic. Can be combined with --clickHouseDeadLetterTable to write to both destinations simultaneously. At least one of --clickHouseDeadLetterTable or --deadLetterTopic must be provided. For example, `projects/<PROJECT_ID>/topics/<TOPIC_NAME>`.
* **windowSeconds**: Duration in seconds for time-based batching windows. If only this is set, time-only mode is used. If set with --batchRowCount, combined mode fires on whichever comes first. If neither is set, combined mode uses defaults (30s + 1000 rows).
* **batchRowCount**: Number of rows to accumulate before flushing to ClickHouse. If only this is set, count-only mode is used. If set with --windowSeconds, combined mode fires on whichever comes first. If neither is set, combined mode uses defaults (30s + 1000 rows).
* **maxInsertBlockSize**: Maximum number of rows per INSERT statement sent to ClickHouse. Defaults to 1,000,000. For example, `10000`.
* **maxRetries**: Maximum number of retry attempts for failed ClickHouse inserts. Defaults to 5.
* **insertDeduplicate**: Whether to enable deduplication for INSERT queries in replicated ClickHouse tables. Defaults to true.
* **insertQuorum**: For INSERT queries in replicated tables, wait for writing to the specified number of replicas and linearize the data addition. 0 disables quorum writes. Disabled by default.
* **insertDistributedSync**: If enabled, INSERT queries into distributed tables wait until data is sent to all nodes in the cluster. Defaults to true.



## Getting Started

### Requirements

* Java 17
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/googlecloud-to-clickhouse/src/main/java/com/google/cloud/teleport/v2/clickhouse/templates/PubSubToClickHouse.java)

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#templates-plugin).

#### Validating the Template

This template has a validation command that is used to check code quality.

```shell
mvn clean install -PtemplatesValidate \
-DskipTests -am \
-pl v2/googlecloud-to-clickhouse
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
-DtemplateName="PubSub_to_ClickHouse" \
-pl v2/googlecloud-to-clickhouse -am
```

The `-DartifactRegistry` parameter can be specified to set the artifact registry repository of the Flex Templates image.
If not provided, it defaults to `gcr.io/<project>`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/PubSub_to_ClickHouse
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/PubSub_to_ClickHouse"

### Required
export INPUT_SUBSCRIPTION=<inputSubscription>
export CLICK_HOUSE_URL=<clickHouseUrl>
export CLICK_HOUSE_DATABASE=<clickHouseDatabase>
export CLICK_HOUSE_TABLE=<clickHouseTable>
export CLICK_HOUSE_USERNAME=<clickHouseUsername>
export CLICK_HOUSE_PASSWORD=<clickHousePassword>

### Optional
export CLICK_HOUSE_DEAD_LETTER_TABLE=<clickHouseDeadLetterTable>
export DEAD_LETTER_TOPIC=<deadLetterTopic>
export WINDOW_SECONDS=<windowSeconds>
export BATCH_ROW_COUNT=<batchRowCount>
export MAX_INSERT_BLOCK_SIZE=1000000
export MAX_RETRIES=5
export INSERT_DEDUPLICATE=true
export INSERT_QUORUM=<insertQuorum>
export INSERT_DISTRIBUTED_SYNC=true

gcloud dataflow flex-template run "pubsub-to-clickhouse-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "inputSubscription=$INPUT_SUBSCRIPTION" \
  --parameters "clickHouseUrl=$CLICK_HOUSE_URL" \
  --parameters "clickHouseDatabase=$CLICK_HOUSE_DATABASE" \
  --parameters "clickHouseTable=$CLICK_HOUSE_TABLE" \
  --parameters "clickHouseUsername=$CLICK_HOUSE_USERNAME" \
  --parameters "clickHousePassword=$CLICK_HOUSE_PASSWORD" \
  --parameters "clickHouseDeadLetterTable=$CLICK_HOUSE_DEAD_LETTER_TABLE" \
  --parameters "deadLetterTopic=$DEAD_LETTER_TOPIC" \
  --parameters "windowSeconds=$WINDOW_SECONDS" \
  --parameters "batchRowCount=$BATCH_ROW_COUNT" \
  --parameters "maxInsertBlockSize=$MAX_INSERT_BLOCK_SIZE" \
  --parameters "maxRetries=$MAX_RETRIES" \
  --parameters "insertDeduplicate=$INSERT_DEDUPLICATE" \
  --parameters "insertQuorum=$INSERT_QUORUM" \
  --parameters "insertDistributedSync=$INSERT_DISTRIBUTED_SYNC"
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
export INPUT_SUBSCRIPTION=<inputSubscription>
export CLICK_HOUSE_URL=<clickHouseUrl>
export CLICK_HOUSE_DATABASE=<clickHouseDatabase>
export CLICK_HOUSE_TABLE=<clickHouseTable>
export CLICK_HOUSE_USERNAME=<clickHouseUsername>
export CLICK_HOUSE_PASSWORD=<clickHousePassword>

### Optional
export CLICK_HOUSE_DEAD_LETTER_TABLE=<clickHouseDeadLetterTable>
export DEAD_LETTER_TOPIC=<deadLetterTopic>
export WINDOW_SECONDS=<windowSeconds>
export BATCH_ROW_COUNT=<batchRowCount>
export MAX_INSERT_BLOCK_SIZE=1000000
export MAX_RETRIES=5
export INSERT_DEDUPLICATE=true
export INSERT_QUORUM=<insertQuorum>
export INSERT_DISTRIBUTED_SYNC=true

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="pubsub-to-clickhouse-job" \
-DtemplateName="PubSub_to_ClickHouse" \
-Dparameters="inputSubscription=$INPUT_SUBSCRIPTION,clickHouseUrl=$CLICK_HOUSE_URL,clickHouseDatabase=$CLICK_HOUSE_DATABASE,clickHouseTable=$CLICK_HOUSE_TABLE,clickHouseUsername=$CLICK_HOUSE_USERNAME,clickHousePassword=$CLICK_HOUSE_PASSWORD,clickHouseDeadLetterTable=$CLICK_HOUSE_DEAD_LETTER_TABLE,deadLetterTopic=$DEAD_LETTER_TOPIC,windowSeconds=$WINDOW_SECONDS,batchRowCount=$BATCH_ROW_COUNT,maxInsertBlockSize=$MAX_INSERT_BLOCK_SIZE,maxRetries=$MAX_RETRIES,insertDeduplicate=$INSERT_DEDUPLICATE,insertQuorum=$INSERT_QUORUM,insertDistributedSync=$INSERT_DISTRIBUTED_SYNC" \
-f v2/googlecloud-to-clickhouse
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
cd v2/googlecloud-to-clickhouse/terraform/PubSub_to_ClickHouse
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

resource "google_dataflow_flex_template_job" "pubsub_to_clickhouse" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/PubSub_to_ClickHouse"
  name              = "pubsub-to-clickhouse"
  region            = var.region
  parameters        = {
    inputSubscription = "<inputSubscription>"
    clickHouseUrl = "<clickHouseUrl>"
    clickHouseDatabase = "<clickHouseDatabase>"
    clickHouseTable = "<clickHouseTable>"
    clickHouseUsername = "<clickHouseUsername>"
    clickHousePassword = "<clickHousePassword>"
    # clickHouseDeadLetterTable = "<clickHouseDeadLetterTable>"
    # deadLetterTopic = "<deadLetterTopic>"
    # windowSeconds = "<windowSeconds>"
    # batchRowCount = "<batchRowCount>"
    # maxInsertBlockSize = "1000000"
    # maxRetries = "5"
    # insertDeduplicate = "true"
    # insertQuorum = "<insertQuorum>"
    # insertDistributedSync = "true"
  }
}
```
