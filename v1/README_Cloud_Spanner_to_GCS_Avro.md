
Cloud Spanner to Avro Files on Cloud Storage template
---
The Cloud Spanner to Avro Files on Cloud Storage template is a batch pipeline
that exports a whole Cloud Spanner database to Cloud Storage in Avro format.
Exporting a Cloud Spanner database creates a folder in the bucket you select. The
folder contains:
- A `spanner-export.json` file.
- A `TableName-manifest.json` file for each table in the database you exported.
- One or more `TableName.avro-#####-of-#####` files.

For example, exporting a database with two tables, Singers and Albums, creates
the following file set:
- `Albums-manifest.json`
- `Albums.avro-00000-of-00002`
- `Albums.avro-00001-of-00002`
- `Singers-manifest.json`
- `Singers.avro-00000-of-00003`
- `Singers.avro-00001-of-00003`
- `Singers.avro-00002-of-00003`
- `spanner-export.json`.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/cloud-spanner-to-avro)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Cloud_Spanner_to_GCS_Avro).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **instanceId** : The instance ID of the Spanner database that you want to export.
* **databaseId** : The database ID of the Spanner database that you want to export.
* **outputDir** : The Cloud Storage path to export Avro files to. The export job creates a new directory under this path that contains the exported files. (Example: gs://your-bucket/your-path).

### Optional parameters

* **avroTempDirectory** : The Cloud Storage path where temporary Avro files are written.
* **spannerHost** : The Cloud Spanner endpoint to call in the template. Only used for testing. (Example: https://batch-spanner.googleapis.com). Defaults to: https://batch-spanner.googleapis.com.
* **snapshotTime** : The timestamp that corresponds to the version of the Spanner database that you want to read. The timestamp must be specified by using RFC 3339 UTC `Zulu` format. The timestamp must be in the past, and maximum timestamp staleness applies. (Example: 1990-12-31T23:59:60Z). Defaults to empty.
* **spannerProjectId** : The ID of the Google Cloud project that contains the Spanner database that you want to read data from.
* **shouldExportTimestampAsLogicalType** : If true, timestamps are exported as a `long` type with `timestamp-micros` logical type. By default, this parameter is set to `false` and timestamps are exported as ISO-8601 strings at nanosecond precision.
* **tableNames** : A comma-separated list of tables specifying the subset of the Spanner database to export. If you set this parameter, you must either include all of the related tables (parent tables and foreign key referenced tables) or set the `shouldExportRelatedTables` parameter to `true`.If the table is in named schema, please use fully qualified name. For example: `sch1.foo` in which `sch1` is the schema name and `foo` is the table name. Defaults to empty.
* **shouldExportRelatedTables** : Whether to include related tables. This parameter is used in conjunction with the `tableNames` parameter. Defaults to: false.
* **spannerPriority** : The request priority for Spanner calls. Possible values are `HIGH`, `MEDIUM`, and `LOW`. The default value is `MEDIUM`.
* **dataBoostEnabled** : Set to `true` to use the compute resources of Spanner Data Boost to run the job with near-zero impact on Spanner OLTP workflows. When set to `true`, you also need the `spanner.databases.useDataBoost` IAM permission. For more information, see the Data Boost overview (https://cloud.google.com/spanner/docs/databoost/databoost-overview). Defaults to: false.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v1/src/main/java/com/google/cloud/teleport/spanner/ExportPipeline.java)

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#templates-plugin).

### Building Template

This template is a Classic Template, meaning that the pipeline code will be
executed only once and the pipeline will be saved to Google Cloud Storage for
further reuse. Please check [Creating classic Dataflow templates](https://cloud.google.com/dataflow/docs/guides/templates/creating-templates)
and [Running classic templates](https://cloud.google.com/dataflow/docs/guides/templates/running-templates)
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
-DtemplateName="Cloud_Spanner_to_GCS_Avro" \
-f v1
```

The `-DgcpTempLocation=<temp-bucket-name>` parameter can be specified to set the GCS bucket used by the DataflowRunner to write
temp files to during serialization. The path used will be `gs://<temp-bucket-name>/temp/`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Classic Template was staged! gs://<bucket-name>/templates/Cloud_Spanner_to_GCS_Avro
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/Cloud_Spanner_to_GCS_Avro"

### Required
export INSTANCE_ID=<instanceId>
export DATABASE_ID=<databaseId>
export OUTPUT_DIR=<outputDir>

### Optional
export AVRO_TEMP_DIRECTORY=<avroTempDirectory>
export SPANNER_HOST=https://batch-spanner.googleapis.com
export SNAPSHOT_TIME=""
export SPANNER_PROJECT_ID=<spannerProjectId>
export SHOULD_EXPORT_TIMESTAMP_AS_LOGICAL_TYPE=false
export TABLE_NAMES=""
export SHOULD_EXPORT_RELATED_TABLES=false
export SPANNER_PRIORITY=<spannerPriority>
export DATA_BOOST_ENABLED=false

gcloud dataflow jobs run "cloud-spanner-to-gcs-avro-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "instanceId=$INSTANCE_ID" \
  --parameters "databaseId=$DATABASE_ID" \
  --parameters "outputDir=$OUTPUT_DIR" \
  --parameters "avroTempDirectory=$AVRO_TEMP_DIRECTORY" \
  --parameters "spannerHost=$SPANNER_HOST" \
  --parameters "snapshotTime=$SNAPSHOT_TIME" \
  --parameters "spannerProjectId=$SPANNER_PROJECT_ID" \
  --parameters "shouldExportTimestampAsLogicalType=$SHOULD_EXPORT_TIMESTAMP_AS_LOGICAL_TYPE" \
  --parameters "tableNames=$TABLE_NAMES" \
  --parameters "shouldExportRelatedTables=$SHOULD_EXPORT_RELATED_TABLES" \
  --parameters "spannerPriority=$SPANNER_PRIORITY" \
  --parameters "dataBoostEnabled=$DATA_BOOST_ENABLED"
```

For more information about the command, please check:
https://cloud.google.com/sdk/gcloud/reference/dataflow/jobs/run


**Using the plugin**:

Instead of just generating the template in the folder, it is possible to stage
and run the template in a single command. This may be useful for testing when
changing the templates.

```shell
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>
export REGION=us-central1

### Required
export INSTANCE_ID=<instanceId>
export DATABASE_ID=<databaseId>
export OUTPUT_DIR=<outputDir>

### Optional
export AVRO_TEMP_DIRECTORY=<avroTempDirectory>
export SPANNER_HOST=https://batch-spanner.googleapis.com
export SNAPSHOT_TIME=""
export SPANNER_PROJECT_ID=<spannerProjectId>
export SHOULD_EXPORT_TIMESTAMP_AS_LOGICAL_TYPE=false
export TABLE_NAMES=""
export SHOULD_EXPORT_RELATED_TABLES=false
export SPANNER_PRIORITY=<spannerPriority>
export DATA_BOOST_ENABLED=false

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="cloud-spanner-to-gcs-avro-job" \
-DtemplateName="Cloud_Spanner_to_GCS_Avro" \
-Dparameters="instanceId=$INSTANCE_ID,databaseId=$DATABASE_ID,outputDir=$OUTPUT_DIR,avroTempDirectory=$AVRO_TEMP_DIRECTORY,spannerHost=$SPANNER_HOST,snapshotTime=$SNAPSHOT_TIME,spannerProjectId=$SPANNER_PROJECT_ID,shouldExportTimestampAsLogicalType=$SHOULD_EXPORT_TIMESTAMP_AS_LOGICAL_TYPE,tableNames=$TABLE_NAMES,shouldExportRelatedTables=$SHOULD_EXPORT_RELATED_TABLES,spannerPriority=$SPANNER_PRIORITY,dataBoostEnabled=$DATA_BOOST_ENABLED" \
-f v1
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
see [dataflow_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_job).

Terraform modules have been generated for most templates in this repository. This includes the relevant parameters
specific to the template. If available, they may be used instead of
[dataflow_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_job)
directly.

To use the autogenerated module, execute the standard
[terraform workflow](https://developer.hashicorp.com/terraform/intro/core-workflow):

```shell
cd v1/terraform/Cloud_Spanner_to_GCS_Avro
terraform init
terraform apply
```

To use
[dataflow_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_job)
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

resource "google_dataflow_job" "cloud_spanner_to_gcs_avro" {

  provider          = google-beta
  template_gcs_path = "gs://dataflow-templates-${var.region}/latest/Cloud_Spanner_to_GCS_Avro"
  name              = "cloud-spanner-to-gcs-avro"
  region            = var.region
  temp_gcs_location = "gs://bucket-name-here/temp"
  parameters        = {
    instanceId = "<instanceId>"
    databaseId = "<databaseId>"
    outputDir = "gs://your-bucket/your-path"
    # avroTempDirectory = "<avroTempDirectory>"
    # spannerHost = "https://batch-spanner.googleapis.com"
    # snapshotTime = "1990-12-31T23:59:60Z"
    # spannerProjectId = "<spannerProjectId>"
    # shouldExportTimestampAsLogicalType = "false"
    # tableNames = ""
    # shouldExportRelatedTables = "false"
    # spannerPriority = "<spannerPriority>"
    # dataBoostEnabled = "false"
  }
}
```
