
CSV files on Cloud Storage to BigQuery (YAML) template
---
The CSV files on Cloud Storage to BigQuery template is a batch pipeline that
reads CSV files from Cloud Storage and writes them to a BigQuery table.



:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **csvPath**: The Cloud Storage path or file pattern to the CSV file(s) to read. For example: gs://my-bucket/path/*.csv.
* **table**: BigQuery table location to write the output to or read from. The name  should be in the format <project>:<dataset>.<table_name>. For write,  the table's schema must match input objects. For example, `my-project:my_dataset.my_table`.

### Optional parameters

* **delimiter**: A single character string used to separate fields, e.g. ',' or '	'. Defaults to ','.
* **comment**: A single character string indicating that the remainder of the line should not be parsed, e.g. '#'. For example, `#`.
* **filenameColumn**: If not None, the name of the column to add to each record, containing the filename of the source file. For example, `source_file`.
* **createDisposition**: Specifies whether a table should be created if it does not exist.  Valid inputs are 'CREATE_NEVER' and 'CREATE_IF_NEEDED'. For example, `CREATE_NEVER`. Defaults to: CREATE_NEVER.
* **writeDisposition**: How to specify if a write should append to an existing table, replace the table, or verify that the table is empty. Note that the dataset being written to must already exist. Unbounded collections can only be written using 'WRITE_EMPTY' or 'WRITE_APPEND'. For example, `WRITE_APPEND`. Defaults to: WRITE_APPEND.
* **numStreams**: Number of streams defines the parallelism of the BigQueryIO’s Write  transform and roughly corresponds to the number of Storage Write API’s  streams which will be used by the pipeline. See https://cloud.google.com/blog/products/data-analytics/streaming-data-into-bigquery-using-storage-write-api for the recommended values. The default value is 1. For example, `1`.



## Getting Started

### Requirements

* Java 17
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=yaml/src/main/java/com/google/cloud/teleport/templates/yaml/GcsCsvToBigQueryYaml.java)

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#templates-plugin).

#### Validating the Template

This template has a validation command that is used to check code quality.

```shell
mvn clean install -PtemplatesValidate \
-DskipTests -am \
-pl yaml
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
-DtemplateName="GCS_CSV_to_BigQuery_Yaml" \
-f yaml
```

The `-DartifactRegistry` parameter can be specified to set the artifact registry repository of the Flex Templates image.
If not provided, it defaults to `gcr.io/<project>`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/GCS_CSV_to_BigQuery_Yaml
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/GCS_CSV_to_BigQuery_Yaml"

### Required
export CSV_PATH=<csvPath>
export TABLE=<table>

### Optional
export DELIMITER=<delimiter>
export COMMENT=<comment>
export FILENAME_COLUMN=<filenameColumn>
export CREATE_DISPOSITION=CREATE_NEVER
export WRITE_DISPOSITION=WRITE_APPEND
export NUM_STREAMS=1

gcloud dataflow flex-template run "gcs-csv-to-bigquery-yaml-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "csvPath=$CSV_PATH" \
  --parameters "delimiter=$DELIMITER" \
  --parameters "comment=$COMMENT" \
  --parameters "filenameColumn=$FILENAME_COLUMN" \
  --parameters "table=$TABLE" \
  --parameters "createDisposition=$CREATE_DISPOSITION" \
  --parameters "writeDisposition=$WRITE_DISPOSITION" \
  --parameters "numStreams=$NUM_STREAMS"
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
export CSV_PATH=<csvPath>
export TABLE=<table>

### Optional
export DELIMITER=<delimiter>
export COMMENT=<comment>
export FILENAME_COLUMN=<filenameColumn>
export CREATE_DISPOSITION=CREATE_NEVER
export WRITE_DISPOSITION=WRITE_APPEND
export NUM_STREAMS=1

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="gcs-csv-to-bigquery-yaml-job" \
-DtemplateName="GCS_CSV_to_BigQuery_Yaml" \
-Dparameters="csvPath=$CSV_PATH,delimiter=$DELIMITER,comment=$COMMENT,filenameColumn=$FILENAME_COLUMN,table=$TABLE,createDisposition=$CREATE_DISPOSITION,writeDisposition=$WRITE_DISPOSITION,numStreams=$NUM_STREAMS" \
-f yaml
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
cd yaml/terraform/GCS_CSV_to_BigQuery_Yaml
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

resource "google_dataflow_flex_template_job" "gcs_csv_to_bigquery_yaml" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/GCS_CSV_to_BigQuery_Yaml"
  name              = "gcs-csv-to-bigquery-yaml"
  region            = var.region
  parameters        = {
    csvPath = "<csvPath>"
    table = "<table>"
    # delimiter = "<delimiter>"
    # comment = "<comment>"
    # filenameColumn = "<filenameColumn>"
    # createDisposition = "CREATE_NEVER"
    # writeDisposition = "WRITE_APPEND"
    # numStreams = "1"
  }
}
```
