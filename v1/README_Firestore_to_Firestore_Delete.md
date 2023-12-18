
Bulk Delete Entities in Firestore (Datastore mode) template
---
A pipeline which reads in Entities (via a GQL query) from Firestore, optionally
passes in the JSON encoded Entities to a JavaScript UDF, and then deletes all
matching Entities in the selected target project.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/firestore-bulk-delete)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Firestore_to_Firestore_Delete).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required Parameters

* **firestoreReadGqlQuery** (GQL Query): Specifies which Firestore entities to read. Ex: ‘SELECT * FROM MyKind’.
* **firestoreReadProjectId** (Project ID): The Google Cloud project ID of the Firestore instance to read from.
* **firestoreDeleteProjectId** (Delete all matching entities from the GQL Query present in this Firestore Project Id of): Google Cloud Project Id of where to delete the firestore entities.

### Optional Parameters

* **firestoreReadNamespace** (Namespace): Namespace of requested Firestore entities. Leave blank to use default namespace.
* **firestoreHintNumWorkers** (Expected number of workers): Hint for the expected number of workers in the Firestore ramp-up throttling step. Defaults to: 500.
* **javascriptTextTransformGcsPath** (JavaScript UDF path in Cloud Storage): The Cloud Storage path pattern for the JavaScript code containing your user-defined functions.
* **javascriptTextTransformFunctionName** (JavaScript UDF name): The name of the function to call from your JavaScript file. Use only letters, digits, and underscores. (Example: transform_udf1).


## User-Defined functions (UDFs)

The Bulk Delete Entities in Firestore (Datastore mode) Template supports User-Defined functions (UDFs).
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
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates#templates-plugin)
. Install the plugin with the following command before proceeding:

```shell
mvn clean install -pl plugins/templates-maven-plugin -am
```

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
-DtemplateName="Firestore_to_Firestore_Delete" \
-pl v1 \
-am
```

The `-DgcpTempLocation=<temp-bucket-name>` parameter can be specified to set the GCS bucket used by the DataflowRunner to write
temp files to during serialization. The path used will be `gs://<temp-bucket-name>/temp/`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Classic Template was staged! gs://<bucket-name>/templates/Firestore_to_Firestore_Delete
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/Firestore_to_Firestore_Delete"

### Required
export FIRESTORE_READ_GQL_QUERY=<firestoreReadGqlQuery>
export FIRESTORE_READ_PROJECT_ID=<firestoreReadProjectId>
export FIRESTORE_DELETE_PROJECT_ID=<firestoreDeleteProjectId>

### Optional
export FIRESTORE_READ_NAMESPACE=<firestoreReadNamespace>
export FIRESTORE_HINT_NUM_WORKERS=500
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>

gcloud dataflow jobs run "firestore-to-firestore-delete-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "firestoreReadGqlQuery=$FIRESTORE_READ_GQL_QUERY" \
  --parameters "firestoreReadProjectId=$FIRESTORE_READ_PROJECT_ID" \
  --parameters "firestoreReadNamespace=$FIRESTORE_READ_NAMESPACE" \
  --parameters "firestoreDeleteProjectId=$FIRESTORE_DELETE_PROJECT_ID" \
  --parameters "firestoreHintNumWorkers=$FIRESTORE_HINT_NUM_WORKERS" \
  --parameters "javascriptTextTransformGcsPath=$JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH" \
  --parameters "javascriptTextTransformFunctionName=$JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME"
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
export FIRESTORE_READ_GQL_QUERY=<firestoreReadGqlQuery>
export FIRESTORE_READ_PROJECT_ID=<firestoreReadProjectId>
export FIRESTORE_DELETE_PROJECT_ID=<firestoreDeleteProjectId>

### Optional
export FIRESTORE_READ_NAMESPACE=<firestoreReadNamespace>
export FIRESTORE_HINT_NUM_WORKERS=500
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="firestore-to-firestore-delete-job" \
-DtemplateName="Firestore_to_Firestore_Delete" \
-Dparameters="firestoreReadGqlQuery=$FIRESTORE_READ_GQL_QUERY,firestoreReadProjectId=$FIRESTORE_READ_PROJECT_ID,firestoreReadNamespace=$FIRESTORE_READ_NAMESPACE,firestoreDeleteProjectId=$FIRESTORE_DELETE_PROJECT_ID,firestoreHintNumWorkers=$FIRESTORE_HINT_NUM_WORKERS,javascriptTextTransformGcsPath=$JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH,javascriptTextTransformFunctionName=$JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME" \
-pl v1 \
-am
```

## Terraform

Dataflow supports the utilization of Terraform to manage template jobs,
see [dataflow_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_job).

Here is an example of Terraform configuration:


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

resource "google_dataflow_job" "firestore_to_firestore_delete" {

  provider          = google-beta
  template_gcs_path = "gs://dataflow-templates-${var.region}/latest/Firestore_to_Firestore_Delete"
  name              = "firestore-to-firestore-delete"
  region            = var.region
  temp_gcs_location = "gs://bucket-name-here/temp"
  parameters        = {
    firestoreReadGqlQuery = "<firestoreReadGqlQuery>"
    firestoreReadProjectId = "<firestoreReadProjectId>"
    firestoreDeleteProjectId = "<firestoreDeleteProjectId>"
    # firestoreReadNamespace = "<firestoreReadNamespace>"
    # firestoreHintNumWorkers = "500"
    # javascriptTextTransformGcsPath = "<javascriptTextTransformGcsPath>"
    # javascriptTextTransformFunctionName = "transform_udf1"
  }
}
```
