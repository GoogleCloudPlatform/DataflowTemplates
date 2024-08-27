
Cloud Spanner to Text Files on Cloud Storage template
---
The Cloud Spanner to Cloud Storage Text template is a batch pipeline that reads
in data from a Cloud Spanner table, and writes it to Cloud Storage as CSV text
files.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/cloud-spanner-to-cloud-storage)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Spanner_to_GCS_Text).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **spannerTable** : The Spanner table to read the data from.
* **spannerProjectId** : The ID of the Google Cloud project that contains the Spanner database to read data from.
* **spannerInstanceId** : The instance ID of the requested table.
* **spannerDatabaseId** : The database ID of the requested table.
* **textWritePrefix** : The Cloud Storage path prefix that specifies where the data is written. (Example: gs://mybucket/somefolder/).

### Optional parameters

* **csvTempDirectory** : The Cloud Storage path where temporary CSV files are written. (Example: gs://your-bucket/your-path).
* **spannerPriority** : The request priority (https://cloud.google.com/spanner/docs/reference/rest/v1/RequestOptions) for Spanner calls. Possible values are `HIGH`, `MEDIUM`, `LOW`. The default value is `MEDIUM`.
* **spannerHost** : The Cloud Spanner endpoint to call in the template. Only used for testing. (Example: https://batch-spanner.googleapis.com). Defaults to: https://batch-spanner.googleapis.com.
* **spannerSnapshotTime** : The timestamp that corresponds to the version of the Spanner database that you want to read from. The timestamp must be specified in the RFC 3339 (https://tools.ietf.org/html/rfc3339) UTC "Zulu" format. The timestamp must be in the past and maximum timestamp staleness (https://cloud.google.com/spanner/docs/timestamp-bounds#maximum_timestamp_staleness) applies. (Example: 1990-12-31T23:59:60Z). Defaults to empty.
* **dataBoostEnabled** : Set to `true` to use the compute resources of Spanner Data Boost to run the job with near-zero impact on Spanner OLTP workflows. When true, requires the `spanner.databases.useDataBoost` Identity and Access Management (IAM) permission. For more information, see Data Boost overview (https://cloud.google.com/spanner/docs/databoost/databoost-overview). Defaults to: false.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v1/src/main/java/com/google/cloud/teleport/templates/SpannerToText.java)

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
-DtemplateName="Spanner_to_GCS_Text" \
-f v1
```

The `-DgcpTempLocation=<temp-bucket-name>` parameter can be specified to set the GCS bucket used by the DataflowRunner to write
temp files to during serialization. The path used will be `gs://<temp-bucket-name>/temp/`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Classic Template was staged! gs://<bucket-name>/templates/Spanner_to_GCS_Text
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/Spanner_to_GCS_Text"

### Required
export SPANNER_TABLE=<spannerTable>
export SPANNER_PROJECT_ID=<spannerProjectId>
export SPANNER_INSTANCE_ID=<spannerInstanceId>
export SPANNER_DATABASE_ID=<spannerDatabaseId>
export TEXT_WRITE_PREFIX=<textWritePrefix>

### Optional
export CSV_TEMP_DIRECTORY=<csvTempDirectory>
export SPANNER_PRIORITY=<spannerPriority>
export SPANNER_HOST=https://batch-spanner.googleapis.com
export SPANNER_SNAPSHOT_TIME=""
export DATA_BOOST_ENABLED=false

gcloud dataflow jobs run "spanner-to-gcs-text-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "csvTempDirectory=$CSV_TEMP_DIRECTORY" \
  --parameters "spannerPriority=$SPANNER_PRIORITY" \
  --parameters "spannerTable=$SPANNER_TABLE" \
  --parameters "spannerProjectId=$SPANNER_PROJECT_ID" \
  --parameters "spannerInstanceId=$SPANNER_INSTANCE_ID" \
  --parameters "spannerDatabaseId=$SPANNER_DATABASE_ID" \
  --parameters "spannerHost=$SPANNER_HOST" \
  --parameters "spannerSnapshotTime=$SPANNER_SNAPSHOT_TIME" \
  --parameters "dataBoostEnabled=$DATA_BOOST_ENABLED" \
  --parameters "textWritePrefix=$TEXT_WRITE_PREFIX"
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
export SPANNER_TABLE=<spannerTable>
export SPANNER_PROJECT_ID=<spannerProjectId>
export SPANNER_INSTANCE_ID=<spannerInstanceId>
export SPANNER_DATABASE_ID=<spannerDatabaseId>
export TEXT_WRITE_PREFIX=<textWritePrefix>

### Optional
export CSV_TEMP_DIRECTORY=<csvTempDirectory>
export SPANNER_PRIORITY=<spannerPriority>
export SPANNER_HOST=https://batch-spanner.googleapis.com
export SPANNER_SNAPSHOT_TIME=""
export DATA_BOOST_ENABLED=false

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="spanner-to-gcs-text-job" \
-DtemplateName="Spanner_to_GCS_Text" \
-Dparameters="csvTempDirectory=$CSV_TEMP_DIRECTORY,spannerPriority=$SPANNER_PRIORITY,spannerTable=$SPANNER_TABLE,spannerProjectId=$SPANNER_PROJECT_ID,spannerInstanceId=$SPANNER_INSTANCE_ID,spannerDatabaseId=$SPANNER_DATABASE_ID,spannerHost=$SPANNER_HOST,spannerSnapshotTime=$SPANNER_SNAPSHOT_TIME,dataBoostEnabled=$DATA_BOOST_ENABLED,textWritePrefix=$TEXT_WRITE_PREFIX" \
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
cd v1/terraform/Spanner_to_GCS_Text
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

resource "google_dataflow_job" "spanner_to_gcs_text" {

  provider          = google-beta
  template_gcs_path = "gs://dataflow-templates-${var.region}/latest/Spanner_to_GCS_Text"
  name              = "spanner-to-gcs-text"
  region            = var.region
  temp_gcs_location = "gs://bucket-name-here/temp"
  parameters        = {
    spannerTable = "<spannerTable>"
    spannerProjectId = "<spannerProjectId>"
    spannerInstanceId = "<spannerInstanceId>"
    spannerDatabaseId = "<spannerDatabaseId>"
    textWritePrefix = "gs://mybucket/somefolder/"
    # csvTempDirectory = "gs://your-bucket/your-path"
    # spannerPriority = "<spannerPriority>"
    # spannerHost = "https://batch-spanner.googleapis.com"
    # spannerSnapshotTime = "1990-12-31T23:59:60Z"
    # dataBoostEnabled = "false"
  }
}
```
