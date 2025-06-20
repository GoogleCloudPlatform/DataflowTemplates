
BigQuery to ClickHouse template
---
The BigQuery to ClickHouse template is a batch pipeline that ingests data from a
BigQuery table into ClickHouse table. The template can either read the entire
table or read specific records using a supplied query.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/bigquery-to-clickhouse)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=BigQuery_to_ClickHouse).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **jdbcUrl**: The target ClickHouse JDBC URL in the format `jdbc:clickhouse://host:port/schema`. Any JDBC option could be added at the end of the JDBC URL. For example, `jdbc:clickhouse://localhost:8123/default`.
* **clickHouseUsername**: The ClickHouse username to authenticate with.
* **clickHouseTable**: The target ClickHouse table name to insert the data to.

### Optional parameters

* **inputTableSpec**: The BigQuery table to read from. If you specify `inputTableSpec`, the template reads the data directly from BigQuery storage by using the BigQuery Storage Read API (https://cloud.google.com/bigquery/docs/reference/storage). For information about limitations in the Storage Read API, see https://cloud.google.com/bigquery/docs/reference/storage#limitations. You must specify either `inputTableSpec` or `query`. If you set both parameters, the template uses the `query` parameter. For example, `<BIGQUERY_PROJECT>:<DATASET_NAME>.<INPUT_TABLE>`.
* **outputDeadletterTable**: The BigQuery table for messages that failed to reach the output table. If a table doesn't exist, it is created during pipeline execution. If not specified, `<outputTableSpec>_error_records` is used. For example, `<PROJECT_ID>:<DATASET_NAME>.<DEADLETTER_TABLE>`.
* **query**: The SQL query to use to read data from BigQuery. If the BigQuery dataset is in a different project than the Dataflow job, specify the full dataset name in the SQL query, for example: <PROJECT_ID>.<DATASET_NAME>.<TABLE_NAME>. By default, the `query` parameter uses GoogleSQL (https://cloud.google.com/bigquery/docs/introduction-sql), unless `useLegacySql` is `true`. You must specify either `inputTableSpec` or `query`. If you set both parameters, the template uses the `query` parameter. For example, `select * from sampledb.sample_table`.
* **useLegacySql**: Set to `true` to use legacy SQL. This parameter only applies when using the `query` parameter. Defaults to `false`.
* **queryLocation**: Needed when reading from an authorized view without underlying table's permission. For example, `US`.
* **queryTempDataset**: With this option, you can set an existing dataset to create the temporary table to store the results of the query. For example, `temp_dataset`.
* **KMSEncryptionKey**: If reading from BigQuery using query source, use this Cloud KMS key to encrypt any temporary tables created. For example, `projects/your-project/locations/global/keyRings/your-keyring/cryptoKeys/your-key`.
* **clickHousePassword**: The ClickHouse password to authenticate with.
* **maxInsertBlockSize**: The maximum block size for insertion, if we control the creation of blocks for insertion (ClickHouseIO option).
* **insertDistributedSync**: If setting is enabled, insert query into distributed waits until data will be sent to all nodes in cluster. (ClickHouseIO option).
* **insertQuorum**: For INSERT queries in the replicated table, wait writing for the specified number of replicas and linearize the addition of the data. 0 - disabled.
This setting is disabled in default server settings (ClickHouseIO option).
* **insertDeduplicate**: For INSERT queries in the replicated table, specifies that deduplication of inserting blocks should be performed.
* **maxRetries**: Maximum number of retries per insert.



## Getting Started

### Requirements

* Java 17
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/googlecloud-to-clickhouse/src/main/java/com/google/cloud/teleport/v2/clickhouse/templates/BigQueryToClickHouse.java)

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
-DtemplateName="BigQuery_to_ClickHouse" \
-f v2/googlecloud-to-clickhouse
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/BigQuery_to_ClickHouse
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/BigQuery_to_ClickHouse"

### Required
export JDBC_URL=<jdbcUrl>
export CLICK_HOUSE_USERNAME=<clickHouseUsername>
export CLICK_HOUSE_TABLE=<clickHouseTable>

### Optional
export INPUT_TABLE_SPEC=<inputTableSpec>
export OUTPUT_DEADLETTER_TABLE=<outputDeadletterTable>
export QUERY=<query>
export USE_LEGACY_SQL=false
export QUERY_LOCATION=<queryLocation>
export QUERY_TEMP_DATASET=<queryTempDataset>
export KMSENCRYPTION_KEY=<KMSEncryptionKey>
export CLICK_HOUSE_PASSWORD=<clickHousePassword>
export MAX_INSERT_BLOCK_SIZE=<maxInsertBlockSize>
export INSERT_DISTRIBUTED_SYNC=<insertDistributedSync>
export INSERT_QUORUM=<insertQuorum>
export INSERT_DEDUPLICATE=<insertDeduplicate>
export MAX_RETRIES=<maxRetries>

gcloud dataflow flex-template run "bigquery-to-clickhouse-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "inputTableSpec=$INPUT_TABLE_SPEC" \
  --parameters "outputDeadletterTable=$OUTPUT_DEADLETTER_TABLE" \
  --parameters "query=$QUERY" \
  --parameters "useLegacySql=$USE_LEGACY_SQL" \
  --parameters "queryLocation=$QUERY_LOCATION" \
  --parameters "queryTempDataset=$QUERY_TEMP_DATASET" \
  --parameters "KMSEncryptionKey=$KMSENCRYPTION_KEY" \
  --parameters "jdbcUrl=$JDBC_URL" \
  --parameters "clickHouseUsername=$CLICK_HOUSE_USERNAME" \
  --parameters "clickHousePassword=$CLICK_HOUSE_PASSWORD" \
  --parameters "clickHouseTable=$CLICK_HOUSE_TABLE" \
  --parameters "maxInsertBlockSize=$MAX_INSERT_BLOCK_SIZE" \
  --parameters "insertDistributedSync=$INSERT_DISTRIBUTED_SYNC" \
  --parameters "insertQuorum=$INSERT_QUORUM" \
  --parameters "insertDeduplicate=$INSERT_DEDUPLICATE" \
  --parameters "maxRetries=$MAX_RETRIES"
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
export JDBC_URL=<jdbcUrl>
export CLICK_HOUSE_USERNAME=<clickHouseUsername>
export CLICK_HOUSE_TABLE=<clickHouseTable>

### Optional
export INPUT_TABLE_SPEC=<inputTableSpec>
export OUTPUT_DEADLETTER_TABLE=<outputDeadletterTable>
export QUERY=<query>
export USE_LEGACY_SQL=false
export QUERY_LOCATION=<queryLocation>
export QUERY_TEMP_DATASET=<queryTempDataset>
export KMSENCRYPTION_KEY=<KMSEncryptionKey>
export CLICK_HOUSE_PASSWORD=<clickHousePassword>
export MAX_INSERT_BLOCK_SIZE=<maxInsertBlockSize>
export INSERT_DISTRIBUTED_SYNC=<insertDistributedSync>
export INSERT_QUORUM=<insertQuorum>
export INSERT_DEDUPLICATE=<insertDeduplicate>
export MAX_RETRIES=<maxRetries>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="bigquery-to-clickhouse-job" \
-DtemplateName="BigQuery_to_ClickHouse" \
-Dparameters="inputTableSpec=$INPUT_TABLE_SPEC,outputDeadletterTable=$OUTPUT_DEADLETTER_TABLE,query=$QUERY,useLegacySql=$USE_LEGACY_SQL,queryLocation=$QUERY_LOCATION,queryTempDataset=$QUERY_TEMP_DATASET,KMSEncryptionKey=$KMSENCRYPTION_KEY,jdbcUrl=$JDBC_URL,clickHouseUsername=$CLICK_HOUSE_USERNAME,clickHousePassword=$CLICK_HOUSE_PASSWORD,clickHouseTable=$CLICK_HOUSE_TABLE,maxInsertBlockSize=$MAX_INSERT_BLOCK_SIZE,insertDistributedSync=$INSERT_DISTRIBUTED_SYNC,insertQuorum=$INSERT_QUORUM,insertDeduplicate=$INSERT_DEDUPLICATE,maxRetries=$MAX_RETRIES" \
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
cd v2/googlecloud-to-clickhouse/terraform/BigQuery_to_ClickHouse
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

resource "google_dataflow_flex_template_job" "bigquery_to_clickhouse" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/BigQuery_to_ClickHouse"
  name              = "bigquery-to-clickhouse"
  region            = var.region
  parameters        = {
    jdbcUrl = "<jdbcUrl>"
    clickHouseUsername = "<clickHouseUsername>"
    clickHouseTable = "<clickHouseTable>"
    # inputTableSpec = "<inputTableSpec>"
    # outputDeadletterTable = "<outputDeadletterTable>"
    # query = "<query>"
    # useLegacySql = "false"
    # queryLocation = "<queryLocation>"
    # queryTempDataset = "<queryTempDataset>"
    # KMSEncryptionKey = "<KMSEncryptionKey>"
    # clickHousePassword = "<clickHousePassword>"
    # maxInsertBlockSize = "<maxInsertBlockSize>"
    # insertDistributedSync = "<insertDistributedSync>"
    # insertQuorum = "<insertQuorum>"
    # insertDeduplicate = "<insertDeduplicate>"
    # maxRetries = "<maxRetries>"
  }
}
```
