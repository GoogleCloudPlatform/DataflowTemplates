
Cloud Spanner vectors to Cloud Storage for Vertex Vector Search template
---
The Cloud Spanner to Vector Embeddings on Cloud Storage template is a batch
pipeline that exports vector embeddings data from Cloud Spanner's table to Cloud
Storage in JSON format. Vector embeddings are exported to a Cloud Storage folder
specified by the user in the template parameters. The Cloud Storage folder will
contain the list of exported `.json` files representing vector embeddings in a
format supported by Vertex AI Vector Search Index.

Check <a
href="https://cloud.google.com/vertex-ai/docs/vector-search/setup/format-structure#json">Vector
Search Format Structure</a> for additional details.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/cloud-spanner-to-vertex-vector-search)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Cloud_Spanner_vectors_to_Cloud_Storage).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **spannerProjectId** : The project ID of the Cloud Spanner instance.
* **spannerInstanceId** : The instance ID of the Cloud Spanner from which you want to export the vector embeddings.
* **spannerDatabaseId** : The database ID of the Cloud Spanner from which you want to export the vector embeddings.
* **spannerTable** : Spanner Table to read from.
* **spannerColumnsToExport** : Comma separated list of columns which are required for Vertex AI Vector Search Index. The `id` & `embedding` are required columns for Vertex Vector Search. If the column names don't precisely align with the Vertex AI Vector Search Index input structure, you can establish column mappings using aliases. If you have the columns that don't match the format expected by Vertex, you can use the notation `from:to`. For example, if the columns are `id` and `my_embedding`, in which `id` matches what Vertex expects but the embedding column is named differently, `id, my_embedding:embedding` should be specified.
* **gcsOutputFolder** : The Cloud Storage folder for writing output files. Must end with a slash. (Example: gs://your-bucket/folder1/).
* **gcsOutputFilePrefix** : The filename prefix for writing output files. (Example: vector-embeddings).

### Optional parameters

* **spannerHost** : The Cloud Spanner endpoint to call in the template. The default is set to https://batch-spanner.googleapis.com. (Example: https://batch-spanner.googleapis.com).
* **spannerVersionTime** : If set, specifies the time when the database version must be taken. String is in the RFC 3339 format in UTC time.  Timestamp must be in the past and maximum timestamp staleness applies; see <a href="https://cloud.google.com/spanner/docs/timestamp-bounds#maximum_timestamp_staleness">Maximum Timestamp Staleness</a>. If not set, strong bound is used to read the latest data; see <a href="https://cloud.google.com/spanner/docs/timestamp-bounds#strong">Timestamp Strong Bounds</a>. (Example: 1990-12-31T23:59:60Z). Defaults to empty.
* **spannerDataBoostEnabled** : Use Spanner on-demand compute so the export job will run on independent compute resources and have no impact to current Spanner workloads. This will incur additional charges in Spanner. Refer <a href=" https://cloud.google.com/spanner/docs/databoost/databoost-overview">Data Boost Overview</a>. Defaults to: false.
* **spannerPriority** : The request priority for Cloud Spanner calls. The value must be one of: [HIGH,MEDIUM,LOW]. Defaults to: MEDIUM.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v1/src/main/java/com/google/cloud/teleport/templates/SpannerVectorEmbeddingExport.java)

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
-DtemplateName="Cloud_Spanner_vectors_to_Cloud_Storage" \
-f v1
```

The `-DgcpTempLocation=<temp-bucket-name>` parameter can be specified to set the GCS bucket used by the DataflowRunner to write
temp files to during serialization. The path used will be `gs://<temp-bucket-name>/temp/`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Classic Template was staged! gs://<bucket-name>/templates/Cloud_Spanner_vectors_to_Cloud_Storage
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/Cloud_Spanner_vectors_to_Cloud_Storage"

### Required
export SPANNER_PROJECT_ID=<spannerProjectId>
export SPANNER_INSTANCE_ID=<spannerInstanceId>
export SPANNER_DATABASE_ID=<spannerDatabaseId>
export SPANNER_TABLE=<spannerTable>
export SPANNER_COLUMNS_TO_EXPORT=<spannerColumnsToExport>
export GCS_OUTPUT_FOLDER=<gcsOutputFolder>
export GCS_OUTPUT_FILE_PREFIX=<gcsOutputFilePrefix>

### Optional
export SPANNER_HOST=https://batch-spanner.googleapis.com
export SPANNER_VERSION_TIME=""
export SPANNER_DATA_BOOST_ENABLED=false
export SPANNER_PRIORITY=<spannerPriority>

gcloud dataflow jobs run "cloud-spanner-vectors-to-cloud-storage-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "spannerProjectId=$SPANNER_PROJECT_ID" \
  --parameters "spannerInstanceId=$SPANNER_INSTANCE_ID" \
  --parameters "spannerDatabaseId=$SPANNER_DATABASE_ID" \
  --parameters "spannerTable=$SPANNER_TABLE" \
  --parameters "spannerColumnsToExport=$SPANNER_COLUMNS_TO_EXPORT" \
  --parameters "gcsOutputFolder=$GCS_OUTPUT_FOLDER" \
  --parameters "gcsOutputFilePrefix=$GCS_OUTPUT_FILE_PREFIX" \
  --parameters "spannerHost=$SPANNER_HOST" \
  --parameters "spannerVersionTime=$SPANNER_VERSION_TIME" \
  --parameters "spannerDataBoostEnabled=$SPANNER_DATA_BOOST_ENABLED" \
  --parameters "spannerPriority=$SPANNER_PRIORITY"
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
export SPANNER_PROJECT_ID=<spannerProjectId>
export SPANNER_INSTANCE_ID=<spannerInstanceId>
export SPANNER_DATABASE_ID=<spannerDatabaseId>
export SPANNER_TABLE=<spannerTable>
export SPANNER_COLUMNS_TO_EXPORT=<spannerColumnsToExport>
export GCS_OUTPUT_FOLDER=<gcsOutputFolder>
export GCS_OUTPUT_FILE_PREFIX=<gcsOutputFilePrefix>

### Optional
export SPANNER_HOST=https://batch-spanner.googleapis.com
export SPANNER_VERSION_TIME=""
export SPANNER_DATA_BOOST_ENABLED=false
export SPANNER_PRIORITY=<spannerPriority>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="cloud-spanner-vectors-to-cloud-storage-job" \
-DtemplateName="Cloud_Spanner_vectors_to_Cloud_Storage" \
-Dparameters="spannerProjectId=$SPANNER_PROJECT_ID,spannerInstanceId=$SPANNER_INSTANCE_ID,spannerDatabaseId=$SPANNER_DATABASE_ID,spannerTable=$SPANNER_TABLE,spannerColumnsToExport=$SPANNER_COLUMNS_TO_EXPORT,gcsOutputFolder=$GCS_OUTPUT_FOLDER,gcsOutputFilePrefix=$GCS_OUTPUT_FILE_PREFIX,spannerHost=$SPANNER_HOST,spannerVersionTime=$SPANNER_VERSION_TIME,spannerDataBoostEnabled=$SPANNER_DATA_BOOST_ENABLED,spannerPriority=$SPANNER_PRIORITY" \
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
cd v1/terraform/Cloud_Spanner_vectors_to_Cloud_Storage
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

resource "google_dataflow_job" "cloud_spanner_vectors_to_cloud_storage" {

  provider          = google-beta
  template_gcs_path = "gs://dataflow-templates-${var.region}/latest/Cloud_Spanner_vectors_to_Cloud_Storage"
  name              = "cloud-spanner-vectors-to-cloud-storage"
  region            = var.region
  temp_gcs_location = "gs://bucket-name-here/temp"
  parameters        = {
    spannerProjectId = "<spannerProjectId>"
    spannerInstanceId = "<spannerInstanceId>"
    spannerDatabaseId = "<spannerDatabaseId>"
    spannerTable = "<spannerTable>"
    spannerColumnsToExport = "<spannerColumnsToExport>"
    gcsOutputFolder = "gs://your-bucket/folder1/"
    gcsOutputFilePrefix = "vector-embeddings"
    # spannerHost = "https://batch-spanner.googleapis.com"
    # spannerVersionTime = "1990-12-31T23:59:60Z"
    # spannerDataBoostEnabled = "false"
    # spannerPriority = "<spannerPriority>"
  }
}
```
