
BigQuery to Datastore template
---
A pipeline that reads rows from BigQuery and writes entities to Datastore.

Note: Nested and repeated BigQuery columns are currently not supported.



:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **readQuery** : A BigQuery SQL query that extracts data from the source. For example, select * from dataset1.sample_table.
* **datastoreWriteProjectId** : The ID of the Google Cloud project to write the Datastore entities to.
* **errorWritePath** : The error log output file to use for write failures that occur during processing. (Example: gs://your-bucket/errors/).

### Optional parameters

* **readIdColumn** : Name of the BigQuery column storing the unique identifier of the row.
* **invalidOutputPath** : Cloud Storage path where to write BigQuery rows that cannot be converted to target entities. (Example: gs://your-bucket/your-path).
* **datastoreWriteEntityKind** : Datastore kind under which entities will be written in the output Google Cloud project.
* **datastoreWriteNamespace** : Datastore namespace under which entities will be written in the output Google Cloud project.
* **datastoreHintNumWorkers** : Hint for the expected number of workers in the Datastore ramp-up throttling step. Default is `500`.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v1/src/main/java/com/google/cloud/teleport/templates/BigQueryToDatastore.java)

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
-DtemplateName="Cloud_BigQuery_to_Cloud_Datastore" \
-f v1
```

The `-DgcpTempLocation=<temp-bucket-name>` parameter can be specified to set the GCS bucket used by the DataflowRunner to write
temp files to during serialization. The path used will be `gs://<temp-bucket-name>/temp/`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Classic Template was staged! gs://<bucket-name>/templates/Cloud_BigQuery_to_Cloud_Datastore
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/Cloud_BigQuery_to_Cloud_Datastore"

### Required
export READ_QUERY=<readQuery>
export DATASTORE_WRITE_PROJECT_ID=<datastoreWriteProjectId>
export ERROR_WRITE_PATH=<errorWritePath>

### Optional
export READ_ID_COLUMN=<readIdColumn>
export INVALID_OUTPUT_PATH=<invalidOutputPath>
export DATASTORE_WRITE_ENTITY_KIND=<datastoreWriteEntityKind>
export DATASTORE_WRITE_NAMESPACE=<datastoreWriteNamespace>
export DATASTORE_HINT_NUM_WORKERS=500

gcloud dataflow jobs run "cloud-bigquery-to-cloud-datastore-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "readQuery=$READ_QUERY" \
  --parameters "readIdColumn=$READ_ID_COLUMN" \
  --parameters "invalidOutputPath=$INVALID_OUTPUT_PATH" \
  --parameters "datastoreWriteProjectId=$DATASTORE_WRITE_PROJECT_ID" \
  --parameters "datastoreWriteEntityKind=$DATASTORE_WRITE_ENTITY_KIND" \
  --parameters "datastoreWriteNamespace=$DATASTORE_WRITE_NAMESPACE" \
  --parameters "datastoreHintNumWorkers=$DATASTORE_HINT_NUM_WORKERS" \
  --parameters "errorWritePath=$ERROR_WRITE_PATH"
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
export READ_QUERY=<readQuery>
export DATASTORE_WRITE_PROJECT_ID=<datastoreWriteProjectId>
export ERROR_WRITE_PATH=<errorWritePath>

### Optional
export READ_ID_COLUMN=<readIdColumn>
export INVALID_OUTPUT_PATH=<invalidOutputPath>
export DATASTORE_WRITE_ENTITY_KIND=<datastoreWriteEntityKind>
export DATASTORE_WRITE_NAMESPACE=<datastoreWriteNamespace>
export DATASTORE_HINT_NUM_WORKERS=500

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="cloud-bigquery-to-cloud-datastore-job" \
-DtemplateName="Cloud_BigQuery_to_Cloud_Datastore" \
-Dparameters="readQuery=$READ_QUERY,readIdColumn=$READ_ID_COLUMN,invalidOutputPath=$INVALID_OUTPUT_PATH,datastoreWriteProjectId=$DATASTORE_WRITE_PROJECT_ID,datastoreWriteEntityKind=$DATASTORE_WRITE_ENTITY_KIND,datastoreWriteNamespace=$DATASTORE_WRITE_NAMESPACE,datastoreHintNumWorkers=$DATASTORE_HINT_NUM_WORKERS,errorWritePath=$ERROR_WRITE_PATH" \
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
cd v1/terraform/Cloud_BigQuery_to_Cloud_Datastore
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

resource "google_dataflow_job" "cloud_bigquery_to_cloud_datastore" {

  provider          = google-beta
  template_gcs_path = "gs://dataflow-templates-${var.region}/latest/Cloud_BigQuery_to_Cloud_Datastore"
  name              = "cloud-bigquery-to-cloud-datastore"
  region            = var.region
  temp_gcs_location = "gs://bucket-name-here/temp"
  parameters        = {
    readQuery = "<readQuery>"
    datastoreWriteProjectId = "<datastoreWriteProjectId>"
    errorWritePath = "gs://your-bucket/errors/"
    # readIdColumn = "<readIdColumn>"
    # invalidOutputPath = "gs://your-bucket/your-path"
    # datastoreWriteEntityKind = "<datastoreWriteEntityKind>"
    # datastoreWriteNamespace = "<datastoreWriteNamespace>"
    # datastoreHintNumWorkers = "500"
  }
}
```
