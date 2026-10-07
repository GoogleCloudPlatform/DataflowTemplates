
Iceberg to Lakehouse template
---
The Iceberg to Lakehouse template is a batch pipeline that reads data from an
Iceberg table and outputs the records to a Lakehouse table.



:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **table**: A fully-qualified table identifier, e.g., my_dataset.my_table. For example, `my_dataset.my_table`.
* **catalogName**: The name of the Iceberg catalog that contains the table. For example, `my_hadoop_catalog`.
* **catalogProperties**: A map of properties for setting up the Iceberg catalog. For example, `{"type": "hadoop", "warehouse": "gs://your-bucket/warehouse"}`.
* **lakehouseTable**: A fully-qualified table identifier, e.g., my_dataset.my_table. For example, `my_dataset.my_table`.
* **lakehouseCatalogName**: The name of the Lakehouse catalog that contains the table. For example, `my_hadoop_catalog`.

### Optional parameters

* **configProperties**: A map of properties to pass to the Hadoop Configuration. For example, `{"fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"}`.
* **drop**: A list of field names to drop. Mutually exclusive with 'keep' and 'only'. For example, `["field_to_drop_1", "field_to_drop_2"]`.
* **keep**: A list of field names to keep. Mutually exclusive with 'drop' and 'only'. For example, `["field_to_keep_1", "field_to_keep_2"]`.
* **filter**: A filter expression to apply to records from the Iceberg table. For example, `age > 18`.
* **lakehouseCatalogProperties**: A map of properties for setting up the Lakehouse catalog. For example, `{"type": "hadoop", "warehouse": "gs://your-bucket/warehouse"}`.
* **lakehouseConfigProperties**: A map of properties to pass to the Hadoop Configuration. For example, `{"fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"}`.
* **lakehousePartitionFields**: A list of fields and transforms for partitioning, e.g., ['day(ts)', 'category']. For example, `["day(ts)", "bucket(id, 4)"]`.
* **lakehouseTableProperties**: A map of Lakehouse table properties to set when the table is created. For example, `{"commit.retry.num-retries": "2"}`.
* **lakehouseDrop**: A list of field names to drop. Mutually exclusive with 'keep' and 'only'. For example, `["field_to_drop_1", "field_to_drop_2"]`.
* **lakehouseKeep**: A list of field names to keep. Mutually exclusive with 'drop' and 'only'. For example, `["field_to_keep_1", "field_to_keep_2"]`.
* **lakehouseOnly**: The name of a single field to write. Mutually exclusive with 'keep' and 'drop'. For example, `my_record_field`.
* **lakehouseDistributionMode**: Defines distribution of write data. Supported distributions are 'none' (don't shuffle rows, default) and 'hash' (shuffle rows by partition key before writing data). For example, `none`.
* **lakehouseAutosharding**: If true, enables dynamic sharding to automatically adjust the number of parallel writers based on data volume. Only available with 'hash' distribution mode. For example, `False`.



## Getting Started

### Requirements

* Java 17
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=yaml/src/main/java/com/google/cloud/teleport/templates/yaml/IcebergToLakehouseYaml.java)

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
-DtemplateName="Iceberg_To_Lakehouse_Yaml" \
-f yaml
```

The `-DartifactRegistry` parameter can be specified to set the artifact registry repository of the Flex Templates image.
If not provided, it defaults to `gcr.io/<project>`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Iceberg_To_Lakehouse_Yaml
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Iceberg_To_Lakehouse_Yaml"

### Required
export TABLE=<table>
export CATALOG_NAME=<catalogName>
export CATALOG_PROPERTIES=<catalogProperties>
export LAKEHOUSE_TABLE=<lakehouseTable>
export LAKEHOUSE_CATALOG_NAME=<lakehouseCatalogName>

### Optional
export CONFIG_PROPERTIES=<configProperties>
export DROP=<drop>
export KEEP=<keep>
export FILTER=<filter>
export LAKEHOUSE_CATALOG_PROPERTIES=<lakehouseCatalogProperties>
export LAKEHOUSE_CONFIG_PROPERTIES=<lakehouseConfigProperties>
export LAKEHOUSE_PARTITION_FIELDS=<lakehousePartitionFields>
export LAKEHOUSE_TABLE_PROPERTIES=<lakehouseTableProperties>
export LAKEHOUSE_DROP=<lakehouseDrop>
export LAKEHOUSE_KEEP=<lakehouseKeep>
export LAKEHOUSE_ONLY=<lakehouseOnly>
export LAKEHOUSE_DISTRIBUTION_MODE=<lakehouseDistributionMode>
export LAKEHOUSE_AUTOSHARDING=<lakehouseAutosharding>

gcloud dataflow flex-template run "iceberg-to-lakehouse-yaml-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "table=$TABLE" \
  --parameters "catalogName=$CATALOG_NAME" \
  --parameters "catalogProperties=$CATALOG_PROPERTIES" \
  --parameters "configProperties=$CONFIG_PROPERTIES" \
  --parameters "drop=$DROP" \
  --parameters "keep=$KEEP" \
  --parameters "filter=$FILTER" \
  --parameters "lakehouseTable=$LAKEHOUSE_TABLE" \
  --parameters "lakehouseCatalogProperties=$LAKEHOUSE_CATALOG_PROPERTIES" \
  --parameters "lakehouseConfigProperties=$LAKEHOUSE_CONFIG_PROPERTIES" \
  --parameters "lakehousePartitionFields=$LAKEHOUSE_PARTITION_FIELDS" \
  --parameters "lakehouseTableProperties=$LAKEHOUSE_TABLE_PROPERTIES" \
  --parameters "lakehouseCatalogName=$LAKEHOUSE_CATALOG_NAME" \
  --parameters "lakehouseDrop=$LAKEHOUSE_DROP" \
  --parameters "lakehouseKeep=$LAKEHOUSE_KEEP" \
  --parameters "lakehouseOnly=$LAKEHOUSE_ONLY" \
  --parameters "lakehouseDistributionMode=$LAKEHOUSE_DISTRIBUTION_MODE" \
  --parameters "lakehouseAutosharding=$LAKEHOUSE_AUTOSHARDING"
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
export TABLE=<table>
export CATALOG_NAME=<catalogName>
export CATALOG_PROPERTIES=<catalogProperties>
export LAKEHOUSE_TABLE=<lakehouseTable>
export LAKEHOUSE_CATALOG_NAME=<lakehouseCatalogName>

### Optional
export CONFIG_PROPERTIES=<configProperties>
export DROP=<drop>
export KEEP=<keep>
export FILTER=<filter>
export LAKEHOUSE_CATALOG_PROPERTIES=<lakehouseCatalogProperties>
export LAKEHOUSE_CONFIG_PROPERTIES=<lakehouseConfigProperties>
export LAKEHOUSE_PARTITION_FIELDS=<lakehousePartitionFields>
export LAKEHOUSE_TABLE_PROPERTIES=<lakehouseTableProperties>
export LAKEHOUSE_DROP=<lakehouseDrop>
export LAKEHOUSE_KEEP=<lakehouseKeep>
export LAKEHOUSE_ONLY=<lakehouseOnly>
export LAKEHOUSE_DISTRIBUTION_MODE=<lakehouseDistributionMode>
export LAKEHOUSE_AUTOSHARDING=<lakehouseAutosharding>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="iceberg-to-lakehouse-yaml-job" \
-DtemplateName="Iceberg_To_Lakehouse_Yaml" \
-Dparameters="table=$TABLE,catalogName=$CATALOG_NAME,catalogProperties=$CATALOG_PROPERTIES,configProperties=$CONFIG_PROPERTIES,drop=$DROP,keep=$KEEP,filter=$FILTER,lakehouseTable=$LAKEHOUSE_TABLE,lakehouseCatalogProperties=$LAKEHOUSE_CATALOG_PROPERTIES,lakehouseConfigProperties=$LAKEHOUSE_CONFIG_PROPERTIES,lakehousePartitionFields=$LAKEHOUSE_PARTITION_FIELDS,lakehouseTableProperties=$LAKEHOUSE_TABLE_PROPERTIES,lakehouseCatalogName=$LAKEHOUSE_CATALOG_NAME,lakehouseDrop=$LAKEHOUSE_DROP,lakehouseKeep=$LAKEHOUSE_KEEP,lakehouseOnly=$LAKEHOUSE_ONLY,lakehouseDistributionMode=$LAKEHOUSE_DISTRIBUTION_MODE,lakehouseAutosharding=$LAKEHOUSE_AUTOSHARDING" \
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
cd yaml/terraform/Iceberg_To_Lakehouse_Yaml
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

resource "google_dataflow_flex_template_job" "iceberg_to_lakehouse_yaml" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Iceberg_To_Lakehouse_Yaml"
  name              = "iceberg-to-lakehouse-yaml"
  region            = var.region
  parameters        = {
    table = "<table>"
    catalogName = "<catalogName>"
    catalogProperties = "<catalogProperties>"
    lakehouseTable = "<lakehouseTable>"
    lakehouseCatalogName = "<lakehouseCatalogName>"
    # configProperties = "<configProperties>"
    # drop = "<drop>"
    # keep = "<keep>"
    # filter = "<filter>"
    # lakehouseCatalogProperties = "<lakehouseCatalogProperties>"
    # lakehouseConfigProperties = "<lakehouseConfigProperties>"
    # lakehousePartitionFields = "<lakehousePartitionFields>"
    # lakehouseTableProperties = "<lakehouseTableProperties>"
    # lakehouseDrop = "<lakehouseDrop>"
    # lakehouseKeep = "<lakehouseKeep>"
    # lakehouseOnly = "<lakehouseOnly>"
    # lakehouseDistributionMode = "<lakehouseDistributionMode>"
    # lakehouseAutosharding = "<lakehouseAutosharding>"
  }
}
```
