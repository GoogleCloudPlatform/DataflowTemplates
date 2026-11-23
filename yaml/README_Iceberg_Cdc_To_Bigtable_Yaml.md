
Iceberg CDC to Bigtable (YAML) template
---
The Iceberg CDC to Bigtable template is a streaming pipeline that continuously
reads row-level changes from an Iceberg table, executes a user-defined mapping,
and writes the resulting mutations to Bigtable. The change type of each record is
available to the mapping as the _change_type field, and UPDATE_BEFORE records are
dropped. Any errors which occur in the transformation of the data are written to
Cloud Storage in JSON format.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided-yaml/iceberg-cdc-to-bigtable)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Iceberg_Cdc_To_Bigtable_Yaml).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **table**: A fully-qualified table identifier, e.g., my_dataset.my_table. For example, `my_dataset.my_table`.
* **catalogName**: The name of the Iceberg catalog that contains the table. For example, `my_hadoop_catalog`.
* **catalogProperties**: A map of properties for setting up the Iceberg catalog. For example, `{"type": "hadoop", "warehouse": "gs://your-bucket/warehouse"}`.
* **fields**: The output fields to compute, each mapping to the expression or callable that creates them. When writing to Bigtable, the expected mutation schema fields are: 'key' (bytes), 'type' (string: 'SetCell', 'DeleteFromColumn', 'DeleteFromFamily', or 'DeleteFromRow'), 'family_name' (string), 'column_qualifier' (bytes), 'value' (bytes), and 'timestamp_micros' (integer, optional; defaults to -1 for current Bigtable server time). For example, `{"key": {"expression": "key.encode('utf-8')", "output_type": "bytes"}, "type": {"expression": "'SetCell'", "output_type": "string"}, "family_name": {"expression": "'cf1'", "output_type": "string"}, "column_qualifier": {"expression": "b'cq1'", "output_type": "bytes"}, "value": {"expression": "value.encode('utf-8')", "output_type": "bytes"}, "timestamp_micros": {"expression": "timestamp_micros", "output_type": "integer"}}`.
* **windowing**: Windowing options - see https://beam.apache.org/documentation/sdks/yaml/#windowing. For example, `{"type": "fixed", "size": "10s"}`.
* **projectId**: The Google Cloud project ID of the Bigtable instance. For example, `my-gcp-project`.
* **instanceId**: The Bigtable instance ID. For example, `my-bigtable-instance`.
* **tableId**: Bigtable table ID to write the output to. For example, `my-bigtable-table`.
* **errorPath**: The Cloud Storage path where failed records will be written in JSON format. For example, `gs://your-bucket/errors/error.json`.

### Optional parameters

* **configProperties**: A map of properties to pass to the Hadoop Configuration. For example, `{"fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"}`.
* **drop**: A list of field names to drop. Mutually exclusive with 'keep' and 'only'. For example, `["field_to_drop_1", "field_to_drop_2"]`.
* **keep**: A list of field names to keep. Mutually exclusive with 'drop' and 'only'. For example, `["field_to_keep_1", "field_to_keep_2"]`.
* **filter**: A filter expression to apply to records from the Iceberg table. For example, `age > 18`.
* **startingStrategy**: Where to start reading changes from. Valid values are 'earliest' and 'latest'. Defaults to 'latest' for streaming pipelines. For example, `earliest`.
* **pollIntervalSeconds**: The interval in seconds at which the source polls the table for new snapshots. Defaults to 60 seconds. For example, `60`.
* **language**: The language used to define (and execute) the expressions and/or  callables in fields. Defaults to python. For example, `python`.



## Getting Started

### Requirements

* Java 17
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=yaml/src/main/java/com/google/cloud/teleport/templates/yaml/IcebergCdcToBigtableYaml.java)

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
-DtemplateName="Iceberg_Cdc_To_Bigtable_Yaml" \
-f yaml
```

The `-DartifactRegistry` parameter can be specified to set the artifact registry repository of the Flex Templates image.
If not provided, it defaults to `gcr.io/<project>`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Iceberg_Cdc_To_Bigtable_Yaml
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Iceberg_Cdc_To_Bigtable_Yaml"

### Required
export TABLE=<table>
export CATALOG_NAME=<catalogName>
export CATALOG_PROPERTIES=<catalogProperties>
export FIELDS=<fields>
export WINDOWING=<windowing>
export PROJECT_ID=<projectId>
export INSTANCE_ID=<instanceId>
export TABLE_ID=<tableId>
export ERROR_PATH=<errorPath>

### Optional
export CONFIG_PROPERTIES=<configProperties>
export DROP=<drop>
export KEEP=<keep>
export FILTER=<filter>
export STARTING_STRATEGY=<startingStrategy>
export POLL_INTERVAL_SECONDS=<pollIntervalSeconds>
export LANGUAGE=python

gcloud dataflow flex-template run "iceberg-cdc-to-bigtable-yaml-job" \
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
  --parameters "startingStrategy=$STARTING_STRATEGY" \
  --parameters "pollIntervalSeconds=$POLL_INTERVAL_SECONDS" \
  --parameters "language=$LANGUAGE" \
  --parameters "fields=$FIELDS" \
  --parameters "windowing=$WINDOWING" \
  --parameters "projectId=$PROJECT_ID" \
  --parameters "instanceId=$INSTANCE_ID" \
  --parameters "tableId=$TABLE_ID" \
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
export TABLE=<table>
export CATALOG_NAME=<catalogName>
export CATALOG_PROPERTIES=<catalogProperties>
export FIELDS=<fields>
export WINDOWING=<windowing>
export PROJECT_ID=<projectId>
export INSTANCE_ID=<instanceId>
export TABLE_ID=<tableId>
export ERROR_PATH=<errorPath>

### Optional
export CONFIG_PROPERTIES=<configProperties>
export DROP=<drop>
export KEEP=<keep>
export FILTER=<filter>
export STARTING_STRATEGY=<startingStrategy>
export POLL_INTERVAL_SECONDS=<pollIntervalSeconds>
export LANGUAGE=python

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="iceberg-cdc-to-bigtable-yaml-job" \
-DtemplateName="Iceberg_Cdc_To_Bigtable_Yaml" \
-Dparameters="table=$TABLE,catalogName=$CATALOG_NAME,catalogProperties=$CATALOG_PROPERTIES,configProperties=$CONFIG_PROPERTIES,drop=$DROP,keep=$KEEP,filter=$FILTER,startingStrategy=$STARTING_STRATEGY,pollIntervalSeconds=$POLL_INTERVAL_SECONDS,language=$LANGUAGE,fields=$FIELDS,windowing=$WINDOWING,projectId=$PROJECT_ID,instanceId=$INSTANCE_ID,tableId=$TABLE_ID,errorPath=$ERROR_PATH" \
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
cd yaml/terraform/Iceberg_Cdc_To_Bigtable_Yaml
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

resource "google_dataflow_flex_template_job" "iceberg_cdc_to_bigtable_yaml" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Iceberg_Cdc_To_Bigtable_Yaml"
  name              = "iceberg-cdc-to-bigtable-yaml"
  region            = var.region
  parameters        = {
    table = "<table>"
    catalogName = "<catalogName>"
    catalogProperties = "<catalogProperties>"
    fields = "<fields>"
    windowing = "<windowing>"
    projectId = "<projectId>"
    instanceId = "<instanceId>"
    tableId = "<tableId>"
    errorPath = "<errorPath>"
    # configProperties = "<configProperties>"
    # drop = "<drop>"
    # keep = "<keep>"
    # filter = "<filter>"
    # startingStrategy = "earliest"
    # pollIntervalSeconds = "60"
    # language = "python"
  }
}
```
