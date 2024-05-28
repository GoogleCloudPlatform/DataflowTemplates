
Bulk Delete Entities in Datastore [Deprecated] template
---
A pipeline which reads in Entities (via a GQL query) from Datastore, optionally
passes in the JSON encoded Entities to a JavaScript UDF, and then deletes all
matching Entities in the selected target project.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/datastore-bulk-delete)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Datastore_to_Datastore_Delete).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **datastoreReadGqlQuery** : A GQL (https://cloud.google.com/datastore/docs/reference/gql_reference) query that specifies which entities to grab. For example, `SELECT * FROM MyKind`.
* **datastoreReadProjectId** : The ID of the Google Cloud project that contains the Datastore instance that you want to read data from.
* **datastoreDeleteProjectId** : Google Cloud Project Id of where to delete the datastore entities.

### Optional parameters

* **datastoreReadNamespace** : The namespace of the requested entities. To use the default namespace, leave this parameter blank.
* **javascriptTextTransformGcsPath** : The Cloud Storage URI of the .js file that defines the JavaScript user-defined function (UDF) to use. For example, `gs://my-bucket/my-udfs/my_file.js`.
* **javascriptTextTransformFunctionName** : The name of the JavaScript user-defined function (UDF) to use. For example, if your JavaScript function code is `myTransform(inJson) { /*...do stuff...*/ }`, then the function name is `myTransform`. For sample JavaScript UDFs, see UDF Examples (https://github.com/GoogleCloudPlatform/DataflowTemplates#udf-examples).
* **datastoreHintNumWorkers** : Hint for the expected number of workers in the Datastore ramp-up throttling step. Defaults to: 500.


## User-Defined functions (UDFs)

The Bulk Delete Entities in Datastore [Deprecated] Template supports User-Defined functions (UDFs).
UDFs allow you to customize functionality by providing a JavaScript function
without having to maintain or build the entire template code.

Check [Create user-defined functions for Dataflow templates](https://cloud.google.com/dataflow/docs/guides/templates/create-template-udf)
and [Using UDFs](https://github.com/GoogleCloudPlatform/DataflowTemplates#using-udfs)
for more information about how to create and test those functions.


## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v1/src/main/java/com/google/cloud/teleport/templates/DatastoreToDatastoreDelete.java)

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
-DtemplateName="Datastore_to_Datastore_Delete" \
-f v1
```

The `-DgcpTempLocation=<temp-bucket-name>` parameter can be specified to set the GCS bucket used by the DataflowRunner to write
temp files to during serialization. The path used will be `gs://<temp-bucket-name>/temp/`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Classic Template was staged! gs://<bucket-name>/templates/Datastore_to_Datastore_Delete
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/Datastore_to_Datastore_Delete"

### Required
export DATASTORE_READ_GQL_QUERY=<datastoreReadGqlQuery>
export DATASTORE_READ_PROJECT_ID=<datastoreReadProjectId>
export DATASTORE_DELETE_PROJECT_ID=<datastoreDeleteProjectId>

### Optional
export DATASTORE_READ_NAMESPACE=<datastoreReadNamespace>
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>
export DATASTORE_HINT_NUM_WORKERS=500

gcloud dataflow jobs run "datastore-to-datastore-delete-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "datastoreReadGqlQuery=$DATASTORE_READ_GQL_QUERY" \
  --parameters "datastoreReadProjectId=$DATASTORE_READ_PROJECT_ID" \
  --parameters "datastoreReadNamespace=$DATASTORE_READ_NAMESPACE" \
  --parameters "javascriptTextTransformGcsPath=$JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH" \
  --parameters "javascriptTextTransformFunctionName=$JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME" \
  --parameters "datastoreDeleteProjectId=$DATASTORE_DELETE_PROJECT_ID" \
  --parameters "datastoreHintNumWorkers=$DATASTORE_HINT_NUM_WORKERS"
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
export DATASTORE_READ_GQL_QUERY=<datastoreReadGqlQuery>
export DATASTORE_READ_PROJECT_ID=<datastoreReadProjectId>
export DATASTORE_DELETE_PROJECT_ID=<datastoreDeleteProjectId>

### Optional
export DATASTORE_READ_NAMESPACE=<datastoreReadNamespace>
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>
export DATASTORE_HINT_NUM_WORKERS=500

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="datastore-to-datastore-delete-job" \
-DtemplateName="Datastore_to_Datastore_Delete" \
-Dparameters="datastoreReadGqlQuery=$DATASTORE_READ_GQL_QUERY,datastoreReadProjectId=$DATASTORE_READ_PROJECT_ID,datastoreReadNamespace=$DATASTORE_READ_NAMESPACE,javascriptTextTransformGcsPath=$JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH,javascriptTextTransformFunctionName=$JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME,datastoreDeleteProjectId=$DATASTORE_DELETE_PROJECT_ID,datastoreHintNumWorkers=$DATASTORE_HINT_NUM_WORKERS" \
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
cd v1/terraform/Datastore_to_Datastore_Delete
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

resource "google_dataflow_job" "datastore_to_datastore_delete" {

  provider          = google-beta
  template_gcs_path = "gs://dataflow-templates-${var.region}/latest/Datastore_to_Datastore_Delete"
  name              = "datastore-to-datastore-delete"
  region            = var.region
  temp_gcs_location = "gs://bucket-name-here/temp"
  parameters        = {
    datastoreReadGqlQuery = "<datastoreReadGqlQuery>"
    datastoreReadProjectId = "<datastoreReadProjectId>"
    datastoreDeleteProjectId = "<datastoreDeleteProjectId>"
    # datastoreReadNamespace = "<datastoreReadNamespace>"
    # javascriptTextTransformGcsPath = "<javascriptTextTransformGcsPath>"
    # javascriptTextTransformFunctionName = "<javascriptTextTransformFunctionName>"
    # datastoreHintNumWorkers = "500"
  }
}
```
