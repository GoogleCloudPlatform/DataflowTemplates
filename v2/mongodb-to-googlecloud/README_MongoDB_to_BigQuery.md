MongoDB to BigQuery Template
---
A batch pipeline which reads data documents from MongoDB and writes them to BigQuery.

:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided-templates)
on how to use it without having to build from sources.

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Mandatory Parameters

* **mongoDbUri** (MongoDB Connection URI): URI to connect to MongoDB Atlas. Defaults to: mongouri.
* **database** (MongoDB database): Database in MongoDB to read the collection from. (Example: my-db).
* **collection** (MongoDB collection): Name of the collection inside MongoDB database. (Example: my-collection). Defaults to: collection.
* **userOption** (User option): User option: FLATTEN or NONE. FLATTEN will flatten the documents for 1 level. NONE will store the whole document as json string. Defaults to: NONE.
* **outputTableSpec** (BigQuery output table): BigQuery table location to write the output to. The name should be in the format <project>:<dataset>.<table_name>. The table's schema must match input objects. Defaults to: bqtable.

### Optional Parameters

* **useStorageWriteApi** (Use BigQuery Storage Write API): If enabled (set to true) the pipeline will use Storage Write API when writing the data to BigQuery (see https://cloud.google.com/blog/products/data-analytics/streaming-data-into-bigquery-using-storage-write-api). Defaults to: false.
* **useStorageWriteApiAtLeastOnce** (Use at at-least-once semantics in BigQuery Storage Write API): This parameter takes effect only if "Use BigQuery Storage Write API" is enabled. If enabled the at-least-once semantics will be used for Storage Write API, otherwise exactly-once semantics will be used. Defaults to: false.
* **javascriptDocumentTransformGcsPath** (Path to the UDF stored in the GCS bucket.): Enter the gcs path in format gs://<bucket-name>/<js-file>.js . (Example: gs://test-bucket/test.js).
* **javascriptDocumentTransformFunctionName** (UDF function name stored in the GCS bucket.): Enter the Name of the User defined function . (Example: transform).

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
-DtemplateName="MongoDB_to_BigQuery" \
-pl v2/mongodb-to-googlecloud -am
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/MongoDB_to_BigQuery"
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>
export REGION=us-central1

### Mandatory
export MONGO_DB_URI="mongouri"
export DATABASE=<database>
export COLLECTION="collection"
export USER_OPTION="NONE"
export OUTPUT_TABLE_SPEC="bqtable"

### Optional
export USE_STORAGE_WRITE_API=false
export USE_STORAGE_WRITE_API_AT_LEAST_ONCE=false
export JAVASCRIPT_DOCUMENT_TRANSFORM_GCS_PATH=<javascriptDocumentTransformGcsPath>
export JAVASCRIPT_DOCUMENT_TRANSFORM_FUNCTION_NAME=<javascriptDocumentTransformFunctionName>

gcloud dataflow flex-template run "mongodb-to-bigquery-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "mongoDbUri=$MONGO_DB_URI" \
  --parameters "database=$DATABASE" \
  --parameters "collection=$COLLECTION" \
  --parameters "userOption=$USER_OPTION" \
  --parameters "useStorageWriteApi=$USE_STORAGE_WRITE_API" \
  --parameters "useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE" \
  --parameters "outputTableSpec=$OUTPUT_TABLE_SPEC" \
  --parameters "javascriptDocumentTransformGcsPath=$JAVASCRIPT_DOCUMENT_TRANSFORM_GCS_PATH" \
  --parameters "javascriptDocumentTransformFunctionName=$JAVASCRIPT_DOCUMENT_TRANSFORM_FUNCTION_NAME"
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
export MONGO_DB_URI="mongouri"
export DATABASE=<database>
export COLLECTION="collection"
export USER_OPTION="NONE"
export OUTPUT_TABLE_SPEC="bqtable"

### Optional
export USE_STORAGE_WRITE_API=false
export USE_STORAGE_WRITE_API_AT_LEAST_ONCE=false
export JAVASCRIPT_DOCUMENT_TRANSFORM_GCS_PATH=<javascriptDocumentTransformGcsPath>
export JAVASCRIPT_DOCUMENT_TRANSFORM_FUNCTION_NAME=<javascriptDocumentTransformFunctionName>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="mongodb-to-bigquery-job" \
-DtemplateName="MongoDB_to_BigQuery" \
-Dparameters="mongoDbUri=$MONGO_DB_URI,database=$DATABASE,collection=$COLLECTION,userOption=$USER_OPTION,useStorageWriteApi=$USE_STORAGE_WRITE_API,useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE,outputTableSpec=$OUTPUT_TABLE_SPEC,javascriptDocumentTransformGcsPath=$JAVASCRIPT_DOCUMENT_TRANSFORM_GCS_PATH,javascriptDocumentTransformFunctionName=$JAVASCRIPT_DOCUMENT_TRANSFORM_FUNCTION_NAME" \
-pl v2/mongodb-to-googlecloud -am
```
