
GCS Spanner Data Validation template
---
Batch pipeline that reads data from GCS and Spanner compares them to validate
migration correctness.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/gcs-spanner-dv)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=GCS_Spanner_Data_Validator).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **instanceId**: The destination Cloud Spanner instance.
* **databaseId**: The destination Cloud Spanner database.
* **bigQueryDataset**: The BigQuery dataset ID where the validation results will be stored. For example, `validation_report_dataset`.

### Optional parameters

* **gcsInputDirectory**: This directory is used to read the AVRO files of the records read from source. For example, `gs://your-bucket/your-path`.
* **projectId**: This is the name of the Cloud Spanner project.
* **spannerHost**: The Cloud Spanner endpoint to call in the template. For example, `https://batch-spanner.googleapis.com`. Defaults to: https://batch-spanner.googleapis.com.
* **spannerPriority**: The request priority for Cloud Spanner calls. The value must be one of: [`HIGH`,`MEDIUM`,`LOW`]. Defaults to `HIGH`.
* **sessionFilePath**: Session file path in Cloud Storage that contains mapping information from Spanner Migration Tool. Defaults to empty.
* **schemaOverridesFilePath**: A file which specifies the table and the column name overrides from source to spanner. Defaults to empty.
* **tableOverrides**: These are the table name overrides from source to spanner. They are written in thefollowing format: [{SourceTableName1, SpannerTableName1}, {SourceTableName2, SpannerTableName2}]This example shows mapping Singers table to Vocalists and Albums table to Records. For example, `[{Singers, Vocalists}, {Albums, Records}]`. Defaults to empty.
* **columnOverrides**: These are the column name overrides from source to spanner. They are written in thefollowing format: [{SourceTableName1.SourceColumnName1, SourceTableName1.SpannerColumnName1}, {SourceTableName2.SourceColumnName1, SourceTableName2.SpannerColumnName1}]Note that the SourceTableName should remain the same in both the source and spanner pair. To override table names, use tableOverrides.The example shows mapping SingerName to TalentName and AlbumName to RecordName in Singers and Albums table respectively. For example, `[{Singers.SingerName, Singers.TalentName}, {Albums.AlbumName, Albums.RecordName}]`. Defaults to empty.
* **runId**: A unique identifier for the validation run. If not provided, the Dataflow Job Name will be used. For example, `run_20230101_120000`.



## Getting Started

### Requirements

* Java 17
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/gcs-spanner-dv/src/main/java/com/google/cloud/teleport/v2/templates/GCSSpannerDV.java)

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#templates-plugin).

#### Validating the Template

This template has a validation command that is used to check code quality.

```shell
mvn clean install -PtemplatesValidate \
-DskipTests -am \
-pl v2/gcs-spanner-dv
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
-DtemplateName="GCS_Spanner_Data_Validator" \
-pl v2/gcs-spanner-dv -am
```

The `-DartifactRegistry` parameter can be specified to set the artifact registry repository of the Flex Templates image.
If not provided, it defaults to `gcr.io/<project>`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/GCS_Spanner_Data_Validator
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/GCS_Spanner_Data_Validator"

### Required
export INSTANCE_ID=<instanceId>
export DATABASE_ID=<databaseId>
export BIG_QUERY_DATASET=<bigQueryDataset>

### Optional
export GCS_INPUT_DIRECTORY=<gcsInputDirectory>
export PROJECT_ID=<projectId>
export SPANNER_HOST=https://batch-spanner.googleapis.com
export SPANNER_PRIORITY=HIGH
export SESSION_FILE_PATH=""
export SCHEMA_OVERRIDES_FILE_PATH=""
export TABLE_OVERRIDES=""
export COLUMN_OVERRIDES=""
export RUN_ID=<runId>

gcloud dataflow flex-template run "gcs-spanner-data-validator-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "gcsInputDirectory=$GCS_INPUT_DIRECTORY" \
  --parameters "projectId=$PROJECT_ID" \
  --parameters "spannerHost=$SPANNER_HOST" \
  --parameters "instanceId=$INSTANCE_ID" \
  --parameters "databaseId=$DATABASE_ID" \
  --parameters "spannerPriority=$SPANNER_PRIORITY" \
  --parameters "sessionFilePath=$SESSION_FILE_PATH" \
  --parameters "schemaOverridesFilePath=$SCHEMA_OVERRIDES_FILE_PATH" \
  --parameters "tableOverrides=$TABLE_OVERRIDES" \
  --parameters "columnOverrides=$COLUMN_OVERRIDES" \
  --parameters "bigQueryDataset=$BIG_QUERY_DATASET" \
  --parameters "runId=$RUN_ID"
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
export INSTANCE_ID=<instanceId>
export DATABASE_ID=<databaseId>
export BIG_QUERY_DATASET=<bigQueryDataset>

### Optional
export GCS_INPUT_DIRECTORY=<gcsInputDirectory>
export PROJECT_ID=<projectId>
export SPANNER_HOST=https://batch-spanner.googleapis.com
export SPANNER_PRIORITY=HIGH
export SESSION_FILE_PATH=""
export SCHEMA_OVERRIDES_FILE_PATH=""
export TABLE_OVERRIDES=""
export COLUMN_OVERRIDES=""
export RUN_ID=<runId>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="gcs-spanner-data-validator-job" \
-DtemplateName="GCS_Spanner_Data_Validator" \
-Dparameters="gcsInputDirectory=$GCS_INPUT_DIRECTORY,projectId=$PROJECT_ID,spannerHost=$SPANNER_HOST,instanceId=$INSTANCE_ID,databaseId=$DATABASE_ID,spannerPriority=$SPANNER_PRIORITY,sessionFilePath=$SESSION_FILE_PATH,schemaOverridesFilePath=$SCHEMA_OVERRIDES_FILE_PATH,tableOverrides=$TABLE_OVERRIDES,columnOverrides=$COLUMN_OVERRIDES,bigQueryDataset=$BIG_QUERY_DATASET,runId=$RUN_ID" \
-f v2/gcs-spanner-dv
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
cd v2/gcs-spanner-dv/terraform/GCS_Spanner_Data_Validator
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

resource "google_dataflow_flex_template_job" "gcs_spanner_data_validator" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/GCS_Spanner_Data_Validator"
  name              = "gcs-spanner-data-validator"
  region            = var.region
  parameters        = {
    instanceId = "<instanceId>"
    databaseId = "<databaseId>"
    bigQueryDataset = "<bigQueryDataset>"
    # gcsInputDirectory = "<gcsInputDirectory>"
    # projectId = "<projectId>"
    # spannerHost = "https://batch-spanner.googleapis.com"
    # spannerPriority = "HIGH"
    # sessionFilePath = ""
    # schemaOverridesFilePath = ""
    # tableOverrides = ""
    # columnOverrides = ""
    # runId = "<runId>"
  }
}
```
