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
* **datastoreReadGqlQuery** (GQL Query): Specifies which Datastore entities to read. Ex: ‘SELECT * FROM MyKind’.
* **datastoreReadProjectId** (Project ID): The Google Cloud project ID of the Datastore instance to read from.
* **datastoreReadNamespace** (Namespace): Namespace of requested Datastore entities. Leave blank to use default namespace.
* **firestoreReadGqlQuery** (GQL Query): Specifies which Firestore entities to read. Ex: ‘SELECT * FROM MyKind’.
* **firestoreReadProjectId** (Project ID): The Google Cloud project ID of the Firestore instance to read from.

### Optional Parameters

* **firestoreReadNamespace** (Namespace): Namespace of requested Firestore entities. Leave blank to use default namespace.
* **javascriptTextTransformGcsPath** (Cloud Storage path to Javascript UDF source): The Cloud Storage path pattern for the JavaScript code containing your user-defined functions. (Example: gs://your-bucket/your-function.js).
* **javascriptTextTransformFunctionName** (UDF Javascript Function Name): The name of the function to call from your JavaScript file. Use only letters, digits, and underscores. (Example: 'transform' or 'transform_udf1').

## Getting Started

### Requirements

* Java 11
* Maven
* Valid resources for mandatory parameters.
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following command:
    * `gcloud auth login`

This README uses
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates#templates-plugin)
. Install the plugin with the following command to proceed:

```shell
mvn clean install -pl plugins/templates-maven-plugin -am
```

### Building Template

This template is a Flex Template, meaning that the pipeline code will be
containerized and the container will be executed on Dataflow. Please
check [Use Flex Templates](https://cloud.google.com/dataflow/docs/guides/templates/using-flex-templates)
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
-pl v2/googlecloud-to-googlecloud -am
```

The command should print what is the template location on Cloud Storage:

```
Flex Template was staged! gs://{BUCKET}/{PATH}
```


#### Running the Template

**Using the staged template**:

You can use the path above to share or run the template.

To start a job with the template at any time using `gcloud`, you can use:

```shell
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Firestore_to_BigQuery_Flex"
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>
export REGION=us-central1

### Mandatory
export OUTPUT_TABLE_SPEC=<outputTableSpec>
export BIG_QUERY_LOADING_TEMPORARY_DIRECTORY=<bigQueryLoadingTemporaryDirectory>
export DATASTORE_READ_GQL_QUERY=<datastoreReadGqlQuery>
export DATASTORE_READ_PROJECT_ID=<datastoreReadProjectId>
export DATASTORE_READ_NAMESPACE=<datastoreReadNamespace>
export FIRESTORE_READ_GQL_QUERY=<firestoreReadGqlQuery>
export FIRESTORE_READ_PROJECT_ID=<firestoreReadProjectId>

### Optional
export FIRESTORE_READ_NAMESPACE=<firestoreReadNamespace>
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>

gcloud dataflow flex-template run "firestore-to-bigquery-flex-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "outputTableSpec=$OUTPUT_TABLE_SPEC" \
  --parameters "bigQueryLoadingTemporaryDirectory=$BIG_QUERY_LOADING_TEMPORARY_DIRECTORY" \
  --parameters "datastoreReadGqlQuery=$DATASTORE_READ_GQL_QUERY" \
  --parameters "datastoreReadProjectId=$DATASTORE_READ_PROJECT_ID" \
  --parameters "datastoreReadNamespace=$DATASTORE_READ_NAMESPACE" \
  --parameters "firestoreReadGqlQuery=$FIRESTORE_READ_GQL_QUERY" \
  --parameters "firestoreReadProjectId=$FIRESTORE_READ_PROJECT_ID" \
  --parameters "firestoreReadNamespace=$FIRESTORE_READ_NAMESPACE" \
  --parameters "javascriptTextTransformGcsPath=$JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH" \
  --parameters "javascriptTextTransformFunctionName=$JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME"
```


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
export DATASTORE_READ_GQL_QUERY=<datastoreReadGqlQuery>
export DATASTORE_READ_PROJECT_ID=<datastoreReadProjectId>
export DATASTORE_READ_NAMESPACE=<datastoreReadNamespace>
export FIRESTORE_READ_GQL_QUERY=<firestoreReadGqlQuery>
export FIRESTORE_READ_PROJECT_ID=<firestoreReadProjectId>

### Optional
export FIRESTORE_READ_NAMESPACE=<firestoreReadNamespace>
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="firestore-to-bigquery-flex-job" \
-DtemplateName="Firestore_to_BigQuery_Flex" \
-Dparameters="outputTableSpec=$OUTPUT_TABLE_SPEC,bigQueryLoadingTemporaryDirectory=$BIG_QUERY_LOADING_TEMPORARY_DIRECTORY,datastoreReadGqlQuery=$DATASTORE_READ_GQL_QUERY,datastoreReadProjectId=$DATASTORE_READ_PROJECT_ID,datastoreReadNamespace=$DATASTORE_READ_NAMESPACE,firestoreReadGqlQuery=$FIRESTORE_READ_GQL_QUERY,firestoreReadProjectId=$FIRESTORE_READ_PROJECT_ID,firestoreReadNamespace=$FIRESTORE_READ_NAMESPACE,javascriptTextTransformGcsPath=$JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH,javascriptTextTransformFunctionName=$JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME" \
-pl v2/googlecloud-to-googlecloud -am
```
