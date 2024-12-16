
Cloud Bigtable to JSON template
---
The Bigtable to JSON template is a pipeline that reads data from a Bigtable table
and writes it to a Cloud Storage bucket in JSON format.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/bigtable-to-json)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Cloud_Bigtable_to_GCS_Json).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **bigtableProjectId**: The ID for the Google Cloud project that contains the Bigtable instance that you want to read data from.
* **bigtableInstanceId**: The ID of the Bigtable instance that contains the table.
* **bigtableTableId**: The ID of the Bigtable table to read from.
* **outputDirectory**: The Cloud Storage path where the output JSON files are stored. For example, `gs://your-bucket/your-path/`.

### Optional parameters

* **filenamePrefix**: The prefix of the JSON file name. For example, `table1-`. If no value is provided, defaults to `part`.
* **userOption**: Possible values are `FLATTEN` or `NONE`. `FLATTEN` flattens the row to the single level. `NONE` stores the whole row as a JSON string. Defaults to `NONE`.
* **columnsAliases**: A comma-separated list of columns that are required for the Vertex AI Vector Search index. The columns `id` and `embedding` are required for Vertex AI Vector Search. You can use the notation `fromfamily:fromcolumn;to`. For example, if the columns are `rowkey` and `cf:my_embedding`, where `rowkey` has a different name than the embedding column, specify `cf:my_embedding;embedding` and, `rowkey;id`. Only use this option when the value for `userOption` is `FLATTEN`.
* **bigtableAppProfileId**: The ID of the Bigtable application profile to use for the export. If you don't specify an app profile, Bigtable uses the instance's default app profile: https://cloud.google.com/bigtable/docs/app-profiles#default-app-profile.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v1/src/main/java/com/google/cloud/teleport/bigtable/BigtableToJson.java)

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates#templates-plugin).

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
-DtemplateName="Cloud_Bigtable_to_GCS_Json" \
-f v1
```

The `-DgcpTempLocation=<temp-bucket-name>` parameter can be specified to set the GCS bucket used by the DataflowRunner to write
temp files to during serialization. The path used will be `gs://<temp-bucket-name>/temp/`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Classic Template was staged! gs://<bucket-name>/templates/Cloud_Bigtable_to_GCS_Json
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/Cloud_Bigtable_to_GCS_Json"

### Required
export BIGTABLE_PROJECT_ID=<bigtableProjectId>
export BIGTABLE_INSTANCE_ID=<bigtableInstanceId>
export BIGTABLE_TABLE_ID=<bigtableTableId>
export OUTPUT_DIRECTORY=<outputDirectory>

### Optional
export FILENAME_PREFIX=part
export USER_OPTION=NONE
export COLUMNS_ALIASES=<columnsAliases>
export BIGTABLE_APP_PROFILE_ID=default

gcloud dataflow jobs run "cloud-bigtable-to-gcs-json-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "bigtableProjectId=$BIGTABLE_PROJECT_ID" \
  --parameters "bigtableInstanceId=$BIGTABLE_INSTANCE_ID" \
  --parameters "bigtableTableId=$BIGTABLE_TABLE_ID" \
  --parameters "outputDirectory=$OUTPUT_DIRECTORY" \
  --parameters "filenamePrefix=$FILENAME_PREFIX" \
  --parameters "userOption=$USER_OPTION" \
  --parameters "columnsAliases=$COLUMNS_ALIASES" \
  --parameters "bigtableAppProfileId=$BIGTABLE_APP_PROFILE_ID"
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
export BIGTABLE_PROJECT_ID=<bigtableProjectId>
export BIGTABLE_INSTANCE_ID=<bigtableInstanceId>
export BIGTABLE_TABLE_ID=<bigtableTableId>
export OUTPUT_DIRECTORY=<outputDirectory>

### Optional
export FILENAME_PREFIX=part
export USER_OPTION=NONE
export COLUMNS_ALIASES=<columnsAliases>
export BIGTABLE_APP_PROFILE_ID=default

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="cloud-bigtable-to-gcs-json-job" \
-DtemplateName="Cloud_Bigtable_to_GCS_Json" \
-Dparameters="bigtableProjectId=$BIGTABLE_PROJECT_ID,bigtableInstanceId=$BIGTABLE_INSTANCE_ID,bigtableTableId=$BIGTABLE_TABLE_ID,outputDirectory=$OUTPUT_DIRECTORY,filenamePrefix=$FILENAME_PREFIX,userOption=$USER_OPTION,columnsAliases=$COLUMNS_ALIASES,bigtableAppProfileId=$BIGTABLE_APP_PROFILE_ID" \
-f v1
```

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
cd v1/terraform/Cloud_Bigtable_to_GCS_Json
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

resource "google_dataflow_job" "cloud_bigtable_to_gcs_json" {

  provider          = google-beta
  template_gcs_path = "gs://dataflow-templates-${var.region}/latest/Cloud_Bigtable_to_GCS_Json"
  name              = "cloud-bigtable-to-gcs-json"
  region            = var.region
  temp_gcs_location = "gs://bucket-name-here/temp"
  parameters        = {
    bigtableProjectId = "<bigtableProjectId>"
    bigtableInstanceId = "<bigtableInstanceId>"
    bigtableTableId = "<bigtableTableId>"
    outputDirectory = "<outputDirectory>"
    # filenamePrefix = "part"
    # userOption = "NONE"
    # columnsAliases = "<columnsAliases>"
    # bigtableAppProfileId = "default"
  }
}
```
