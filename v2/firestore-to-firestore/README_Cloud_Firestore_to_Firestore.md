
Firestore to Firestore template
---
The Firestore to Firestore template is a batch pipeline that reads documents from
one <a href="https://cloud.google.com/firestore/docs">Firestore</a> database and
writes them to another Firestore database.

Data consistency is guaranteed only at the end of the pipeline when all data has
been written to the destination database.



:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **sourceProjectId**: The source project to read from. For example, `my-project`.
* **sourceDatabaseId**: The source database to read from. For example, `my-database`. Defaults to: (default).
* **destinationProjectId**: The destination project to write to. For example, `my-project`.
* **destinationDatabaseId**: The destination database to write to. For example, `my-database`. Defaults to: (default).

### Optional parameters

* **collectionIds**: If specified, only replicate these collections. If not specified, copy all collections. For example, `my-collection1,my-collection2`. Defaults to empty.
* **readTime**: The read time of the Firestore read operations. Uses current timestamp if not set. For example, `2021-10-12T07:20:50.52Z`. Defaults to empty.



## Getting Started

### Requirements

* Java 17
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/firestore-to-firestore/src/main/java/com/google/cloud/teleport/v2/templates/FirestoreToFirestore.java)

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#templates-plugin).

#### Validating the Template

This template has a validation command that is used to check code quality.

```shell
mvn clean install -PtemplatesValidate \
-DskipTests -am \
-pl v2/firestore-to-firestore
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
-DtemplateName="Cloud_Firestore_to_Firestore" \
-f v2/firestore-to-firestore
```

The `-DartifactRegistry` parameter can be specified to set the artifact registry repository of the Flex Templates image.
If not provided, it defaults to `gcr.io/<project>`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Cloud_Firestore_to_Firestore
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Cloud_Firestore_to_Firestore"

### Required
export SOURCE_PROJECT_ID=<sourceProjectId>
export SOURCE_DATABASE_ID=(default)
export DESTINATION_PROJECT_ID=<destinationProjectId>
export DESTINATION_DATABASE_ID=(default)

### Optional
export COLLECTION_IDS=""
export READ_TIME=""

gcloud dataflow flex-template run "cloud-firestore-to-firestore-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "sourceProjectId=$SOURCE_PROJECT_ID" \
  --parameters "sourceDatabaseId=$SOURCE_DATABASE_ID" \
  --parameters "collectionIds=$COLLECTION_IDS" \
  --parameters "destinationProjectId=$DESTINATION_PROJECT_ID" \
  --parameters "destinationDatabaseId=$DESTINATION_DATABASE_ID" \
  --parameters "readTime=$READ_TIME"
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
export SOURCE_PROJECT_ID=<sourceProjectId>
export SOURCE_DATABASE_ID=(default)
export DESTINATION_PROJECT_ID=<destinationProjectId>
export DESTINATION_DATABASE_ID=(default)

### Optional
export COLLECTION_IDS=""
export READ_TIME=""

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="cloud-firestore-to-firestore-job" \
-DtemplateName="Cloud_Firestore_to_Firestore" \
-Dparameters="sourceProjectId=$SOURCE_PROJECT_ID,sourceDatabaseId=$SOURCE_DATABASE_ID,collectionIds=$COLLECTION_IDS,destinationProjectId=$DESTINATION_PROJECT_ID,destinationDatabaseId=$DESTINATION_DATABASE_ID,readTime=$READ_TIME" \
-f v2/firestore-to-firestore
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
cd v2/firestore-to-firestore/terraform/Cloud_Firestore_to_Firestore
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

resource "google_dataflow_flex_template_job" "cloud_firestore_to_firestore" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Cloud_Firestore_to_Firestore"
  name              = "cloud-firestore-to-firestore"
  region            = var.region
  parameters        = {
    sourceProjectId = "<sourceProjectId>"
    sourceDatabaseId = "(default)"
    destinationProjectId = "<destinationProjectId>"
    destinationDatabaseId = "(default)"
    # collectionIds = ""
    # readTime = ""
  }
}
```
