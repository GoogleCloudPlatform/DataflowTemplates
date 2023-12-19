
BigQuery export to Parquet (via Storage API) template
---
The BigQuery export to Parquet template is a batch pipeline that reads data from
a BigQuery table and writes it to a Cloud Storage bucket in Parquet format. This
template utilizes the <a
href="https://cloud.google.com/bigquery/docs/reference/storage">BigQuery Storage
API</a> to export the data.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/bigquery-to-parquet)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=BigQuery_to_Parquet).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required Parameters

* **tableRef** (BigQuery table to export): BigQuery table location to export in the format <project>:<dataset>.<table>. (Example: your-project:your-dataset.your-table-name).
* **bucket** (Output Cloud Storage file(s)): Path and filename prefix for writing output files. (Example: gs://your-bucket/export/).

### Optional Parameters

* **numShards** (Maximum output shards): The maximum number of output shards produced when writing. A higher number of shards means higher throughput for writing to Cloud Storage, but potentially higher data aggregation cost across shards when processing output Cloud Storage files. Defaults to: 0.
* **fields** (List of field names): Comma separated list of fields to select from the table.
* **rowRestriction** (Row restrictions/filter.): Read only rows which match the specified filter, which must be a SQL expression compatible with Google standard SQL (https://cloud.google.com/bigquery/docs/reference/standard-sql). If no value is specified, then all rows are returned.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/bigquery-to-parquet/src/main/java/com/google/cloud/teleport/v2/templates/BigQueryToParquet.java)

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
-DtemplateName="BigQuery_to_Parquet" \
-f v2/bigquery-to-parquet
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/BigQuery_to_Parquet
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/BigQuery_to_Parquet"

### Required
export TABLE_REF=<tableRef>
export BUCKET=<bucket>

### Optional
export NUM_SHARDS=0
export FIELDS=<fields>
export ROW_RESTRICTION=<rowRestriction>

gcloud dataflow flex-template run "bigquery-to-parquet-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "tableRef=$TABLE_REF" \
  --parameters "bucket=$BUCKET" \
  --parameters "numShards=$NUM_SHARDS" \
  --parameters "fields=$FIELDS" \
  --parameters "rowRestriction=$ROW_RESTRICTION"
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
export TABLE_REF=<tableRef>
export BUCKET=<bucket>

### Optional
export NUM_SHARDS=0
export FIELDS=<fields>
export ROW_RESTRICTION=<rowRestriction>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="bigquery-to-parquet-job" \
-DtemplateName="BigQuery_to_Parquet" \
-Dparameters="tableRef=$TABLE_REF,bucket=$BUCKET,numShards=$NUM_SHARDS,fields=$FIELDS,rowRestriction=$ROW_RESTRICTION" \
-f v2/bigquery-to-parquet
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

resource "google_dataflow_flex_template_job" "bigquery_to_parquet" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/BigQuery_to_Parquet"
  name              = "bigquery-to-parquet"
  region            = var.region
  parameters        = {
    tableRef = "your-project:your-dataset.your-table-name"
    bucket = "gs://your-bucket/export/"
    # numShards = "0"
    # fields = "<fields>"
    # rowRestriction = "<rowRestriction>"
  }
}
```
