
Spanner to BigQuery template
---
The Spanner to BigQuery template is a batch pipeline that reads data from a
Spanner table, and writes them to a BigQuery table.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/spanner-to-bigquery)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Cloud_Spanner_to_BigQuery_Flex).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **spannerInstanceId** : The instance ID of the Spanner database to read from.
* **spannerDatabaseId** : The database ID of the Spanner database to export.
* **outputTableSpec** : The BigQuery output table location to write the output to. For example, `<PROJECT_ID>:<DATASET_NAME>.<TABLE_NAME>`.Depending on the `createDisposition` specified, the output table might be created automatically using the user provided Avro schema.

### Optional parameters

* **spannerProjectId** : The ID of the project that the Spanner database resides in. The default value for this parameter is the project where the Dataflow pipeline is running.
* **spannerTableId** : The table name of the Spanner database to export. Ignored if sqlQuery is set.
* **spannerRpcPriority** : The request priority (https://cloud.google.com/spanner/docs/reference/rest/v1/RequestOptions) for Spanner calls. Possible values are `HIGH`, `MEDIUM`, and `LOW`. The default value is `HIGH`.
* **sqlQuery** : The SQL query to use to read data from the Spanner database. Required if spannerTableId is empty.
* **bigQuerySchemaPath** : The Cloud Storage path (gs://) to the JSON file that defines your BigQuery schema. (Example: gs://your-bucket/your-schema.json).
* **writeDisposition** : The BigQuery WriteDisposition (https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobconfigurationload) value. For example, `WRITE_APPEND`, `WRITE_EMPTY`, or `WRITE_TRUNCATE`. Defaults to `WRITE_APPEND`.
* **createDisposition** : The BigQuery CreateDisposition (https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobconfigurationload). For example, `CREATE_IF_NEEDED` and `CREATE_NEVER`. Defaults to `CREATE_IF_NEEDED`.
* **useStorageWriteApi** : If `true`, the pipeline uses the BigQuery Storage Write API (https://cloud.google.com/bigquery/docs/write-api). The default value is `false`. For more information, see Using the Storage Write API (https://beam.apache.org/documentation/io/built-in/google-bigquery/#storage-write-api).
* **useStorageWriteApiAtLeastOnce** : When using the Storage Write API, specifies the write semantics. To use at-least-once semantics (https://beam.apache.org/documentation/io/built-in/google-bigquery/#at-least-once-semantics), set this parameter to `true`. To use exactly-once semantics, set the parameter to `false`. This parameter applies only when `useStorageWriteApi` is `true`. The default value is `false`.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/googlecloud-to-googlecloud/src/main/java/com/google/cloud/teleport/v2/templates/SpannerToBigQuery.java)

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
-DtemplateName="Cloud_Spanner_to_BigQuery_Flex" \
-f v2/googlecloud-to-googlecloud
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Cloud_Spanner_to_BigQuery_Flex
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Cloud_Spanner_to_BigQuery_Flex"

### Required
export SPANNER_INSTANCE_ID=<spannerInstanceId>
export SPANNER_DATABASE_ID=<spannerDatabaseId>
export OUTPUT_TABLE_SPEC=<outputTableSpec>

### Optional
export SPANNER_PROJECT_ID=""
export SPANNER_TABLE_ID=<spannerTableId>
export SPANNER_RPC_PRIORITY=<spannerRpcPriority>
export SQL_QUERY=<sqlQuery>
export BIG_QUERY_SCHEMA_PATH=<bigQuerySchemaPath>
export WRITE_DISPOSITION=WRITE_APPEND
export CREATE_DISPOSITION=CREATE_IF_NEEDED
export USE_STORAGE_WRITE_API=false
export USE_STORAGE_WRITE_API_AT_LEAST_ONCE=false

gcloud dataflow flex-template run "cloud-spanner-to-bigquery-flex-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "spannerProjectId=$SPANNER_PROJECT_ID" \
  --parameters "spannerInstanceId=$SPANNER_INSTANCE_ID" \
  --parameters "spannerDatabaseId=$SPANNER_DATABASE_ID" \
  --parameters "spannerTableId=$SPANNER_TABLE_ID" \
  --parameters "spannerRpcPriority=$SPANNER_RPC_PRIORITY" \
  --parameters "sqlQuery=$SQL_QUERY" \
  --parameters "bigQuerySchemaPath=$BIG_QUERY_SCHEMA_PATH" \
  --parameters "outputTableSpec=$OUTPUT_TABLE_SPEC" \
  --parameters "writeDisposition=$WRITE_DISPOSITION" \
  --parameters "createDisposition=$CREATE_DISPOSITION" \
  --parameters "useStorageWriteApi=$USE_STORAGE_WRITE_API" \
  --parameters "useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE"
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
export SPANNER_DATABASE_ID=<spannerDatabaseId>
export OUTPUT_TABLE_SPEC=<outputTableSpec>

### Optional
export SPANNER_PROJECT_ID=""
export SPANNER_TABLE_ID=<spannerTableId>
export SPANNER_RPC_PRIORITY=<spannerRpcPriority>
export SQL_QUERY=<sqlQuery>
export BIG_QUERY_SCHEMA_PATH=<bigQuerySchemaPath>
export WRITE_DISPOSITION=WRITE_APPEND
export CREATE_DISPOSITION=CREATE_IF_NEEDED
export USE_STORAGE_WRITE_API=false
export USE_STORAGE_WRITE_API_AT_LEAST_ONCE=false

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="cloud-spanner-to-bigquery-flex-job" \
-DtemplateName="Cloud_Spanner_to_BigQuery_Flex" \
-Dparameters="spannerProjectId=$SPANNER_PROJECT_ID,spannerInstanceId=$SPANNER_INSTANCE_ID,spannerDatabaseId=$SPANNER_DATABASE_ID,spannerTableId=$SPANNER_TABLE_ID,spannerRpcPriority=$SPANNER_RPC_PRIORITY,sqlQuery=$SQL_QUERY,bigQuerySchemaPath=$BIG_QUERY_SCHEMA_PATH,outputTableSpec=$OUTPUT_TABLE_SPEC,writeDisposition=$WRITE_DISPOSITION,createDisposition=$CREATE_DISPOSITION,useStorageWriteApi=$USE_STORAGE_WRITE_API,useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE" \
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
cd v2/googlecloud-to-googlecloud/terraform/Cloud_Spanner_to_BigQuery_Flex
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

resource "google_dataflow_flex_template_job" "cloud_spanner_to_bigquery_flex" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Cloud_Spanner_to_BigQuery_Flex"
  name              = "cloud-spanner-to-bigquery-flex"
  region            = var.region
  parameters        = {
    spannerInstanceId = "<spannerInstanceId>"
    spannerDatabaseId = "<spannerDatabaseId>"
    outputTableSpec = "<outputTableSpec>"
    # spannerProjectId = ""
    # spannerTableId = "<spannerTableId>"
    # spannerRpcPriority = "<spannerRpcPriority>"
    # sqlQuery = "<sqlQuery>"
    # bigQuerySchemaPath = "gs://your-bucket/your-schema.json"
    # writeDisposition = "WRITE_APPEND"
    # createDisposition = "CREATE_IF_NEEDED"
    # useStorageWriteApi = "false"
    # useStorageWriteApiAtLeastOnce = "false"
  }
}
```
