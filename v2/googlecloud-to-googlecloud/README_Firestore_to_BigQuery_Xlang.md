
Firestore (Datastore mode) to BigQuery with Python UDF template
---
Batch pipeline. Reads Firestore entities and writes them to BigQuery.



:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **outputTableSpec** : BigQuery table location to write the output to. The name should be in the format `<project>:<dataset>.<table_name>`. The table's schema must match input objects.
* **bigQueryLoadingTemporaryDirectory** : Temporary directory for BigQuery loading process (Example: gs://your-bucket/your-files/temp_dir).
* **firestoreReadGqlQuery** : Specifies which Firestore entities to read. Ex: ‘SELECT * FROM MyKind’.
* **firestoreReadProjectId** : The Google Cloud project ID of the Firestore instance to read from.

### Optional parameters

* **bigQuerySchemaPath** : The Cloud Storage path for the BigQuery JSON schema. If `createDisposition` is not set, or set to CREATE_IF_NEEDED, this parameter must be specified. (Example: gs://your-bucket/your-schema.json).
* **firestoreReadNamespace** : Namespace of requested Firestore entities. Leave blank to use default namespace.
* **pythonExternalTextTransformGcsPath** : The Cloud Storage path pattern for the Python code containing your user-defined functions. (Example: gs://your-bucket/your-function.py).
* **pythonExternalTextTransformFunctionName** : The name of the function to call from your Python file. Use only letters, digits, and underscores. (Example: 'transform' or 'transform_udf1').
* **useStorageWriteApi** : If `true`, the pipeline uses the BigQuery Storage Write API (https://cloud.google.com/bigquery/docs/write-api). The default value is `false`. For more information, see Using the Storage Write API (https://beam.apache.org/documentation/io/built-in/google-bigquery/#storage-write-api).
* **useStorageWriteApiAtLeastOnce** : When using the Storage Write API, specifies the write semantics. To use at-least-once semantics (https://beam.apache.org/documentation/io/built-in/google-bigquery/#at-least-once-semantics), set this parameter to `true`. To use exactly-once semantics, set the parameter to `false`. This parameter applies only when `useStorageWriteApi` is `true`. The default value is `false`.
* **writeDisposition** : The BigQuery WriteDisposition (https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobconfigurationload) value. For example, `WRITE_APPEND`, `WRITE_EMPTY`, or `WRITE_TRUNCATE`. Defaults to `WRITE_APPEND`.
* **createDisposition** : The BigQuery CreateDisposition (https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobconfigurationload). For example, `CREATE_IF_NEEDED` and `CREATE_NEVER`. Defaults to `CREATE_IF_NEEDED`.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/googlecloud-to-googlecloud/src/main/java/com/google/cloud/teleport/v2/templates/FirestoreToBigQuery.java)

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#templates-plugin).

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

mvn clean package -PtemplatesStage  \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-DstagePrefix="templates" \
-DtemplateName="Firestore_to_BigQuery_Xlang" \
-f v2/googlecloud-to-googlecloud
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Firestore_to_BigQuery_Xlang
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Firestore_to_BigQuery_Xlang"

### Required
export OUTPUT_TABLE_SPEC=<outputTableSpec>
export BIG_QUERY_LOADING_TEMPORARY_DIRECTORY=<bigQueryLoadingTemporaryDirectory>
export FIRESTORE_READ_GQL_QUERY=<firestoreReadGqlQuery>
export FIRESTORE_READ_PROJECT_ID=<firestoreReadProjectId>

### Optional
export BIG_QUERY_SCHEMA_PATH=<bigQuerySchemaPath>
export FIRESTORE_READ_NAMESPACE=<firestoreReadNamespace>
export PYTHON_EXTERNAL_TEXT_TRANSFORM_GCS_PATH=<pythonExternalTextTransformGcsPath>
export PYTHON_EXTERNAL_TEXT_TRANSFORM_FUNCTION_NAME=<pythonExternalTextTransformFunctionName>
export USE_STORAGE_WRITE_API=false
export USE_STORAGE_WRITE_API_AT_LEAST_ONCE=false
export WRITE_DISPOSITION=WRITE_APPEND
export CREATE_DISPOSITION=CREATE_IF_NEEDED

gcloud dataflow flex-template run "firestore-to-bigquery-xlang-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "outputTableSpec=$OUTPUT_TABLE_SPEC" \
  --parameters "bigQueryLoadingTemporaryDirectory=$BIG_QUERY_LOADING_TEMPORARY_DIRECTORY" \
  --parameters "bigQuerySchemaPath=$BIG_QUERY_SCHEMA_PATH" \
  --parameters "firestoreReadGqlQuery=$FIRESTORE_READ_GQL_QUERY" \
  --parameters "firestoreReadProjectId=$FIRESTORE_READ_PROJECT_ID" \
  --parameters "firestoreReadNamespace=$FIRESTORE_READ_NAMESPACE" \
  --parameters "pythonExternalTextTransformGcsPath=$PYTHON_EXTERNAL_TEXT_TRANSFORM_GCS_PATH" \
  --parameters "pythonExternalTextTransformFunctionName=$PYTHON_EXTERNAL_TEXT_TRANSFORM_FUNCTION_NAME" \
  --parameters "useStorageWriteApi=$USE_STORAGE_WRITE_API" \
  --parameters "useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE" \
  --parameters "writeDisposition=$WRITE_DISPOSITION" \
  --parameters "createDisposition=$CREATE_DISPOSITION"
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
export OUTPUT_TABLE_SPEC=<outputTableSpec>
export BIG_QUERY_LOADING_TEMPORARY_DIRECTORY=<bigQueryLoadingTemporaryDirectory>
export FIRESTORE_READ_GQL_QUERY=<firestoreReadGqlQuery>
export FIRESTORE_READ_PROJECT_ID=<firestoreReadProjectId>

### Optional
export BIG_QUERY_SCHEMA_PATH=<bigQuerySchemaPath>
export FIRESTORE_READ_NAMESPACE=<firestoreReadNamespace>
export PYTHON_EXTERNAL_TEXT_TRANSFORM_GCS_PATH=<pythonExternalTextTransformGcsPath>
export PYTHON_EXTERNAL_TEXT_TRANSFORM_FUNCTION_NAME=<pythonExternalTextTransformFunctionName>
export USE_STORAGE_WRITE_API=false
export USE_STORAGE_WRITE_API_AT_LEAST_ONCE=false
export WRITE_DISPOSITION=WRITE_APPEND
export CREATE_DISPOSITION=CREATE_IF_NEEDED

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="firestore-to-bigquery-xlang-job" \
-DtemplateName="Firestore_to_BigQuery_Xlang" \
-Dparameters="outputTableSpec=$OUTPUT_TABLE_SPEC,bigQueryLoadingTemporaryDirectory=$BIG_QUERY_LOADING_TEMPORARY_DIRECTORY,bigQuerySchemaPath=$BIG_QUERY_SCHEMA_PATH,firestoreReadGqlQuery=$FIRESTORE_READ_GQL_QUERY,firestoreReadProjectId=$FIRESTORE_READ_PROJECT_ID,firestoreReadNamespace=$FIRESTORE_READ_NAMESPACE,pythonExternalTextTransformGcsPath=$PYTHON_EXTERNAL_TEXT_TRANSFORM_GCS_PATH,pythonExternalTextTransformFunctionName=$PYTHON_EXTERNAL_TEXT_TRANSFORM_FUNCTION_NAME,useStorageWriteApi=$USE_STORAGE_WRITE_API,useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE,writeDisposition=$WRITE_DISPOSITION,createDisposition=$CREATE_DISPOSITION" \
-f v2/googlecloud-to-googlecloud
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
see [dataflow_flex_template_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_flex_template_job).

Terraform modules have been generated for most templates in this repository. This includes the relevant parameters
specific to the template. If available, they may be used instead of
[dataflow_flex_template_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_flex_template_job)
directly.

To use the autogenerated module, execute the standard
[terraform workflow](https://developer.hashicorp.com/terraform/intro/core-workflow):

```shell
cd v2/googlecloud-to-googlecloud/terraform/Firestore_to_BigQuery_Xlang
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

resource "google_dataflow_flex_template_job" "firestore_to_bigquery_xlang" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Firestore_to_BigQuery_Xlang"
  name              = "firestore-to-bigquery-xlang"
  region            = var.region
  parameters        = {
    outputTableSpec = "<outputTableSpec>"
    bigQueryLoadingTemporaryDirectory = "gs://your-bucket/your-files/temp_dir"
    firestoreReadGqlQuery = "<firestoreReadGqlQuery>"
    firestoreReadProjectId = "<firestoreReadProjectId>"
    # bigQuerySchemaPath = "gs://your-bucket/your-schema.json"
    # firestoreReadNamespace = "<firestoreReadNamespace>"
    # pythonExternalTextTransformGcsPath = "gs://your-bucket/your-function.py"
    # pythonExternalTextTransformFunctionName = "'transform' or 'transform_udf1'"
    # useStorageWriteApi = "false"
    # useStorageWriteApiAtLeastOnce = "false"
    # writeDisposition = "WRITE_APPEND"
    # createDisposition = "CREATE_IF_NEEDED"
  }
}
```
