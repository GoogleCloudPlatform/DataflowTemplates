
Delta Lake to Lakehouse template
---
The Delta Lake to Lakehouse template is a batch pipeline that reads data from a
Delta Lake table and outputs the records to a Lakehouse table.



:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **deltaLakeTable**: The GCS path to the Delta Lake table, e.g., gs://your-bucket/path/to/table. For example, `gs://your-bucket/path/to/table`.
* **lakehouseTable**: A fully-qualified table identifier, e.g., my_dataset.my_table. For example, `my_dataset.my_table`.
* **lakehouseCatalogName**: The name of the Lakehouse catalog that contains the table. For example, `my_hadoop_catalog`.
* **lakehouseCatalogProperties**: A map of properties for setting up the Lakehouse catalog. For example, `{"type": "hadoop", "warehouse": "gs://your-bucket/warehouse"}`.

### Optional parameters

* **deltaLakeHadoopConfig**: A map of properties to pass to Hadoop Configuration, e.g. key-value pairs. For example, `{"fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"}`. Defaults to: {"fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem", "fs.AbstractFileSystem.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS", "fs.gs.auth.type": "APPLICATION_DEFAULT", "fs.gs.project.id": ""}.
* **lakehouseConfigProperties**: A map of properties to pass to the Hadoop Configuration. For example, `{"fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"}`.
* **lakehouseDrop**: A list of field names to drop. Mutually exclusive with 'keep' and 'only'. For example, `["field_to_drop_1", "field_to_drop_2"]`.
* **lakehouseFilter**: A filter expression to apply to records from the Lakehouse table. For example, `age > 18`.
* **lakehouseKeep**: A list of field names to keep. Mutually exclusive with 'drop' and 'only'. For example, `["field_to_keep_1", "field_to_keep_2"]`.
* **lakehouseOnly**: The name of a single field to write. Mutually exclusive with 'keep' and 'drop'. For example, `my_record_field`.
* **lakehousePartitionFields**: A list of fields and transforms for partitioning, e.g., ['day(ts)', 'category']. For example, `["day(ts)", "bucket(id, 4)"]`.
* **lakehouseTableProperties**: A map of Lakehouse table properties to set when the table is created. For example, `{"commit.retry.num-retries": "2"}`.



## Getting Started

### Requirements

* Java 17
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=yaml/src/main/java/com/google/cloud/teleport/templates/yaml/DeltaLakeToLakehouseYaml.java)

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
-DtemplateName="DeltaLake_To_Lakehouse_Yaml" \
-f yaml
```

The `-DartifactRegistry` parameter can be specified to set the artifact registry repository of the Flex Templates image.
If not provided, it defaults to `gcr.io/<project>`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/DeltaLake_To_Lakehouse_Yaml
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/DeltaLake_To_Lakehouse_Yaml"

### Required
export DELTA_LAKE_TABLE=<deltaLakeTable>
export LAKEHOUSE_TABLE=<lakehouseTable>
export LAKEHOUSE_CATALOG_NAME=<lakehouseCatalogName>
export LAKEHOUSE_CATALOG_PROPERTIES=<lakehouseCatalogProperties>

### Optional
export DELTA_LAKE_HADOOP_CONFIG="{"fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem", "fs.AbstractFileSystem.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS", "fs.gs.auth.type": "APPLICATION_DEFAULT", "fs.gs.project.id": ""}"
export LAKEHOUSE_CONFIG_PROPERTIES=<lakehouseConfigProperties>
export LAKEHOUSE_DROP=<lakehouseDrop>
export LAKEHOUSE_FILTER=<lakehouseFilter>
export LAKEHOUSE_KEEP=<lakehouseKeep>
export LAKEHOUSE_ONLY=<lakehouseOnly>
export LAKEHOUSE_PARTITION_FIELDS=<lakehousePartitionFields>
export LAKEHOUSE_TABLE_PROPERTIES=<lakehouseTableProperties>

gcloud dataflow flex-template run "deltalake-to-lakehouse-yaml-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "deltaLakeTable=$DELTA_LAKE_TABLE" \
  --parameters "deltaLakeHadoopConfig=$DELTA_LAKE_HADOOP_CONFIG" \
  --parameters "lakehouseTable=$LAKEHOUSE_TABLE" \
  --parameters "lakehouseCatalogName=$LAKEHOUSE_CATALOG_NAME" \
  --parameters "lakehouseCatalogProperties=$LAKEHOUSE_CATALOG_PROPERTIES" \
  --parameters "lakehouseConfigProperties=$LAKEHOUSE_CONFIG_PROPERTIES" \
  --parameters "lakehouseDrop=$LAKEHOUSE_DROP" \
  --parameters "lakehouseFilter=$LAKEHOUSE_FILTER" \
  --parameters "lakehouseKeep=$LAKEHOUSE_KEEP" \
  --parameters "lakehouseOnly=$LAKEHOUSE_ONLY" \
  --parameters "lakehousePartitionFields=$LAKEHOUSE_PARTITION_FIELDS" \
  --parameters "lakehouseTableProperties=$LAKEHOUSE_TABLE_PROPERTIES"
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
export DELTA_LAKE_TABLE=<deltaLakeTable>
export LAKEHOUSE_TABLE=<lakehouseTable>
export LAKEHOUSE_CATALOG_NAME=<lakehouseCatalogName>
export LAKEHOUSE_CATALOG_PROPERTIES=<lakehouseCatalogProperties>

### Optional
export DELTA_LAKE_HADOOP_CONFIG="{"fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem", "fs.AbstractFileSystem.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS", "fs.gs.auth.type": "APPLICATION_DEFAULT", "fs.gs.project.id": ""}"
export LAKEHOUSE_CONFIG_PROPERTIES=<lakehouseConfigProperties>
export LAKEHOUSE_DROP=<lakehouseDrop>
export LAKEHOUSE_FILTER=<lakehouseFilter>
export LAKEHOUSE_KEEP=<lakehouseKeep>
export LAKEHOUSE_ONLY=<lakehouseOnly>
export LAKEHOUSE_PARTITION_FIELDS=<lakehousePartitionFields>
export LAKEHOUSE_TABLE_PROPERTIES=<lakehouseTableProperties>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="deltalake-to-lakehouse-yaml-job" \
-DtemplateName="DeltaLake_To_Lakehouse_Yaml" \
-Dparameters="deltaLakeTable=$DELTA_LAKE_TABLE,deltaLakeHadoopConfig=$DELTA_LAKE_HADOOP_CONFIG,lakehouseTable=$LAKEHOUSE_TABLE,lakehouseCatalogName=$LAKEHOUSE_CATALOG_NAME,lakehouseCatalogProperties=$LAKEHOUSE_CATALOG_PROPERTIES,lakehouseConfigProperties=$LAKEHOUSE_CONFIG_PROPERTIES,lakehouseDrop=$LAKEHOUSE_DROP,lakehouseFilter=$LAKEHOUSE_FILTER,lakehouseKeep=$LAKEHOUSE_KEEP,lakehouseOnly=$LAKEHOUSE_ONLY,lakehousePartitionFields=$LAKEHOUSE_PARTITION_FIELDS,lakehouseTableProperties=$LAKEHOUSE_TABLE_PROPERTIES" \
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
cd yaml/terraform/DeltaLake_To_Lakehouse_Yaml
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

resource "google_dataflow_flex_template_job" "deltalake_to_lakehouse_yaml" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/DeltaLake_To_Lakehouse_Yaml"
  name              = "deltalake-to-lakehouse-yaml"
  region            = var.region
  parameters        = {
    deltaLakeTable = "<deltaLakeTable>"
    lakehouseTable = "<lakehouseTable>"
    lakehouseCatalogName = "<lakehouseCatalogName>"
    lakehouseCatalogProperties = "<lakehouseCatalogProperties>"
    # deltaLakeHadoopConfig = ""{"fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem", "fs.AbstractFileSystem.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS", "fs.gs.auth.type": "APPLICATION_DEFAULT", "fs.gs.project.id": }""
    # lakehouseConfigProperties = "<lakehouseConfigProperties>"
    # lakehouseDrop = "<lakehouseDrop>"
    # lakehouseFilter = "<lakehouseFilter>"
    # lakehouseKeep = "<lakehouseKeep>"
    # lakehouseOnly = "<lakehouseOnly>"
    # lakehousePartitionFields = "<lakehousePartitionFields>"
    # lakehouseTableProperties = "<lakehouseTableProperties>"
  }
}
```
