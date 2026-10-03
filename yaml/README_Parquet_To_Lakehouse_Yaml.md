
Parquet files to Lakehouse template
---
The Parquet files to Lakehouse template is a batch pipeline that matches Parquet
files, optionally copies them to Google Cloud Storage, and adds them to a
Lakehouse table.



:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **filePattern**: A file pattern (glob) matching the input Parquet files to add to the Lakehouse table. For example, `gs://your-bucket/path/*.parquet`.
* **lakehouseTable**: A fully-qualified table identifier, e.g., my_dataset.my_table. For example, `my_dataset.my_table`.
* **errorPath**: The Cloud Storage path where failed records will be written in JSON format. For example, `gs://your-bucket/errors/error.json`.

### Optional parameters

* **gcsFilePath**: An optional Google Cloud Storage directory path to copy the matched Parquet files into before registering them in the Lakehouse table. For example, `gs://your-bucket/warehouse/data/`.
* **lakehouseCatalogProperties**: A map of properties for setting up the Lakehouse catalog. For example, `{"type": "hadoop", "warehouse": "gs://your-bucket/warehouse"}`.
* **lakehouseConfigProperties**: A map of properties to pass to the Hadoop Configuration. For example, `{"fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"}`.
* **lakehousePartitionFields**: A list of fields and transforms for partitioning, e.g., ['day(ts)', 'category']. For example, `["day(ts)", "bucket(id, 4)"]`.
* **lakehouseTableProperties**: A map of Lakehouse table properties to set when the table is created. For example, `{"commit.retry.num-retries": "2"}`.
* **lakehouseProjectId**: The Google Cloud project ID used for the default BigLake Iceberg REST catalog when lakehouseCatalogProperties is not provided. For example, `your-project-id`.
* **lakehouseLocationPrefix**: A location prefix used by the catalog when registering data files in the Lakehouse table. For example, `gs://your-bucket/warehouse`.



## Getting Started

### Requirements

* Java 17
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=yaml/src/main/java/com/google/cloud/teleport/templates/yaml/ParquetToLakehouseYaml.java)

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
-DtemplateName="Parquet_To_Lakehouse_Yaml" \
-f yaml
```

The `-DartifactRegistry` parameter can be specified to set the artifact registry repository of the Flex Templates image.
If not provided, it defaults to `gcr.io/<project>`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Parquet_To_Lakehouse_Yaml
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Parquet_To_Lakehouse_Yaml"

### Required
export FILE_PATTERN=<filePattern>
export LAKEHOUSE_TABLE=<lakehouseTable>
export ERROR_PATH=<errorPath>

### Optional
export GCS_FILE_PATH=<gcsFilePath>
export LAKEHOUSE_CATALOG_PROPERTIES=<lakehouseCatalogProperties>
export LAKEHOUSE_CONFIG_PROPERTIES=<lakehouseConfigProperties>
export LAKEHOUSE_PARTITION_FIELDS=<lakehousePartitionFields>
export LAKEHOUSE_TABLE_PROPERTIES=<lakehouseTableProperties>
export LAKEHOUSE_PROJECT_ID=<lakehouseProjectId>
export LAKEHOUSE_LOCATION_PREFIX=<lakehouseLocationPrefix>

gcloud dataflow flex-template run "parquet-to-lakehouse-yaml-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "filePattern=$FILE_PATTERN" \
  --parameters "gcsFilePath=$GCS_FILE_PATH" \
  --parameters "lakehouseTable=$LAKEHOUSE_TABLE" \
  --parameters "lakehouseCatalogProperties=$LAKEHOUSE_CATALOG_PROPERTIES" \
  --parameters "lakehouseConfigProperties=$LAKEHOUSE_CONFIG_PROPERTIES" \
  --parameters "lakehousePartitionFields=$LAKEHOUSE_PARTITION_FIELDS" \
  --parameters "lakehouseTableProperties=$LAKEHOUSE_TABLE_PROPERTIES" \
  --parameters "lakehouseProjectId=$LAKEHOUSE_PROJECT_ID" \
  --parameters "lakehouseLocationPrefix=$LAKEHOUSE_LOCATION_PREFIX" \
  --parameters "errorPath=$ERROR_PATH"
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
export FILE_PATTERN=<filePattern>
export LAKEHOUSE_TABLE=<lakehouseTable>
export ERROR_PATH=<errorPath>

### Optional
export GCS_FILE_PATH=<gcsFilePath>
export LAKEHOUSE_CATALOG_PROPERTIES=<lakehouseCatalogProperties>
export LAKEHOUSE_CONFIG_PROPERTIES=<lakehouseConfigProperties>
export LAKEHOUSE_PARTITION_FIELDS=<lakehousePartitionFields>
export LAKEHOUSE_TABLE_PROPERTIES=<lakehouseTableProperties>
export LAKEHOUSE_PROJECT_ID=<lakehouseProjectId>
export LAKEHOUSE_LOCATION_PREFIX=<lakehouseLocationPrefix>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="parquet-to-lakehouse-yaml-job" \
-DtemplateName="Parquet_To_Lakehouse_Yaml" \
-Dparameters="filePattern=$FILE_PATTERN,gcsFilePath=$GCS_FILE_PATH,lakehouseTable=$LAKEHOUSE_TABLE,lakehouseCatalogProperties=$LAKEHOUSE_CATALOG_PROPERTIES,lakehouseConfigProperties=$LAKEHOUSE_CONFIG_PROPERTIES,lakehousePartitionFields=$LAKEHOUSE_PARTITION_FIELDS,lakehouseTableProperties=$LAKEHOUSE_TABLE_PROPERTIES,lakehouseProjectId=$LAKEHOUSE_PROJECT_ID,lakehouseLocationPrefix=$LAKEHOUSE_LOCATION_PREFIX,errorPath=$ERROR_PATH" \
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
cd yaml/terraform/Parquet_To_Lakehouse_Yaml
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

resource "google_dataflow_flex_template_job" "parquet_to_lakehouse_yaml" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Parquet_To_Lakehouse_Yaml"
  name              = "parquet-to-lakehouse-yaml"
  region            = var.region
  parameters        = {
    filePattern = "<filePattern>"
    lakehouseTable = "<lakehouseTable>"
    errorPath = "<errorPath>"
    # gcsFilePath = "<gcsFilePath>"
    # lakehouseCatalogProperties = "<lakehouseCatalogProperties>"
    # lakehouseConfigProperties = "<lakehouseConfigProperties>"
    # lakehousePartitionFields = "<lakehousePartitionFields>"
    # lakehouseTableProperties = "<lakehouseTableProperties>"
    # lakehouseProjectId = "<lakehouseProjectId>"
    # lakehouseLocationPrefix = "<lakehouseLocationPrefix>"
  }
}
```
