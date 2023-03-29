Firestore (Datastore mode) to BigQuery Template
---
Batch pipeline. Reads Firestore entities and writes them to BigQuery.

:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided-templates)
on how to use it without having to build from sources.

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Mandatory Parameters

* **outputTableSpec** (BigQuery output table): BigQuery table location to write the output to. The name should be in the format <project>:<dataset>.<table_name>. The table's schema must match input objects.
* **bigQueryLoadingTemporaryDirectory** (Temporary directory for BigQuery loading process): Temporary directory for BigQuery loading process (Example: gs://your-bucket/your-files/temp_dir).
* **firestoreReadGqlQuery** (GQL Query): Specifies which Firestore entities to read. Ex: ‘SELECT * FROM MyKind’.
* **firestoreReadProjectId** (Project ID): The Google Cloud project ID of the Firestore instance to read from.

### Optional Parameters

* **firestoreReadNamespace** (Namespace): Namespace of requested Firestore entities. Leave blank to use default namespace.
* **javascriptTextTransformGcsPath** (Cloud Storage path to Javascript UDF source): The Cloud Storage path pattern for the JavaScript code containing your user-defined functions. (Example: gs://your-bucket/your-function.js).
* **javascriptTextTransformFunctionName** (UDF Javascript Function Name): The name of the function to call from your JavaScript file. Use only letters, digits, and underscores. (Example: 'transform' or 'transform_udf1').
* **useStorageWriteApi** (Use BigQuery Storage Write API): If enabled (set to true) the pipeline will use Storage Write API when writing the data to BigQuery (see https://cloud.google.com/blog/products/data-analytics/streaming-data-into-bigquery-using-storage-write-api). Defaults to: false.
* **useStorageWriteApiAtLeastOnce** (Use at at-least-once semantics in BigQuery Storage Write API): This parameter takes effect only if "Use BigQuery Storage Write API" is enabled. If enabled the at-least-once semantics will be used for Storage Write API, otherwise exactly-once semantics will be used. Defaults to: false.

## Getting Started

### Requirements

* Java 11
* Maven
* Valid resources for mandatory parameters.
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
    * `gcloud auth login`
    * `gcloud auth application-default login`

The following instructions use the
[Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates#templates-plugin)
. Install the plugin with the following command to proceed:

```shell
mvn clean install -pl plugins/templates-maven-plugin -am
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

mvn clean package -PtemplatesStage  \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-DstagePrefix="templates" \
-DtemplateName="Firestore_to_BigQuery_Flex" \
-pl v2/googlecloud-to-googlecloud \
-am
```

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Firestore_to_BigQuery_Flex
```

The specific path should be copied as it will be used in the following steps.

#### Running the Template

**Using the staged template**:

You can use the path above run the template (or share with others for execution).

To start a job with that template at any time using `gcloud`, you can use:

```shell
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>
export REGION=us-central1
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Firestore_to_BigQuery_Flex"

### Mandatory
export OUTPUT_TABLE_SPEC=<outputTableSpec>
export BIG_QUERY_LOADING_TEMPORARY_DIRECTORY=<bigQueryLoadingTemporaryDirectory>
export FIRESTORE_READ_GQL_QUERY=<firestoreReadGqlQuery>
export FIRESTORE_READ_PROJECT_ID=<firestoreReadProjectId>

### Optional
export FIRESTORE_READ_NAMESPACE=<firestoreReadNamespace>
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>
export USE_STORAGE_WRITE_API=false
export USE_STORAGE_WRITE_API_AT_LEAST_ONCE=false

gcloud dataflow flex-template run "firestore-to-bigquery-flex-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "outputTableSpec=$OUTPUT_TABLE_SPEC" \
  --parameters "bigQueryLoadingTemporaryDirectory=$BIG_QUERY_LOADING_TEMPORARY_DIRECTORY" \
  --parameters "firestoreReadGqlQuery=$FIRESTORE_READ_GQL_QUERY" \
  --parameters "firestoreReadProjectId=$FIRESTORE_READ_PROJECT_ID" \
  --parameters "firestoreReadNamespace=$FIRESTORE_READ_NAMESPACE" \
  --parameters "javascriptTextTransformGcsPath=$JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH" \
  --parameters "javascriptTextTransformFunctionName=$JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME" \
  --parameters "useStorageWriteApi=$USE_STORAGE_WRITE_API" \
  --parameters "useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE"
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

### Mandatory
export OUTPUT_TABLE_SPEC=<outputTableSpec>
export BIG_QUERY_LOADING_TEMPORARY_DIRECTORY=<bigQueryLoadingTemporaryDirectory>
export FIRESTORE_READ_GQL_QUERY=<firestoreReadGqlQuery>
export FIRESTORE_READ_PROJECT_ID=<firestoreReadProjectId>

### Optional
export FIRESTORE_READ_NAMESPACE=<firestoreReadNamespace>
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>
export USE_STORAGE_WRITE_API=false
export USE_STORAGE_WRITE_API_AT_LEAST_ONCE=false

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="firestore-to-bigquery-flex-job" \
-DtemplateName="Firestore_to_BigQuery_Flex" \
-Dparameters="outputTableSpec=$OUTPUT_TABLE_SPEC,bigQueryLoadingTemporaryDirectory=$BIG_QUERY_LOADING_TEMPORARY_DIRECTORY,firestoreReadGqlQuery=$FIRESTORE_READ_GQL_QUERY,firestoreReadProjectId=$FIRESTORE_READ_PROJECT_ID,firestoreReadNamespace=$FIRESTORE_READ_NAMESPACE,javascriptTextTransformGcsPath=$JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH,javascriptTextTransformFunctionName=$JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME,useStorageWriteApi=$USE_STORAGE_WRITE_API,useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE" \
-pl v2/googlecloud-to-googlecloud \
-am
```
