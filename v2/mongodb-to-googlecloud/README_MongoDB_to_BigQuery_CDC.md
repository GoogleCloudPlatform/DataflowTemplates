
MongoDB to BigQuery (CDC) template
---
The MongoDB to BigQuery CDC (Change Data Capture) template is a streaming
pipeline that works together with MongoDB change streams. The pipeline reads the
JSON records pushed to Pub/Sub via a MongoDB change stream and writes them to
BigQuery as specified by the <code>userOption</code> parameter.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/mongodb-change-stream-to-bigquery)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=MongoDB_to_BigQuery_CDC).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required Parameters

* **mongoDbUri** (MongoDB Connection URI): MongoDB connection URI in the format `mongodb+srv://:@`. Defaults to: mongouri.
* **database** (MongoDB database): Database in MongoDB to read the collection from. (Example: my-db).
* **collection** (MongoDB collection): Name of the collection inside MongoDB database. (Example: my-collection). Defaults to: collection.
* **userOption** (User option): User option: `FLATTEN` or `NONE`. `FLATTEN` flattens the documents to the single level. `NONE` stores the whole document as a JSON string. Defaults to: NONE.
* **inputTopic** (Pub/Sub input topic): Pub/Sub topic to read the input from, in the format of 'projects/your-project-id/topics/your-topic-name' (Example: projects/your-project-id/topics/your-topic-name).
* **outputTableSpec** (BigQuery output table): BigQuery table location to write the output to. The name should be in the format `<project>:<dataset>.<table_name>`. The table's schema must match input objects. Defaults to: bqtable.

### Optional Parameters

* **useStorageWriteApi** (Use BigQuery Storage Write API): If enabled (set to true) the pipeline will use Storage Write API when writing the data to BigQuery (see https://cloud.google.com/blog/products/data-analytics/streaming-data-into-bigquery-using-storage-write-api). If this is enabled and at-least-once semantics (useStorageWriteApiAtLeastOnce) option is off then "Number of streams for BigQuery Storage Write API" and "Triggering frequency in seconds for BigQuery Storage Write API" must be provided. Defaults to: false.
* **useStorageWriteApiAtLeastOnce** (Use at at-least-once semantics in BigQuery Storage Write API): This parameter takes effect only if "Use BigQuery Storage Write API" is enabled. If enabled the at-least-once semantics will be used for Storage Write API, otherwise exactly-once semantics will be used. Defaults to: false.
* **numStorageWriteApiStreams** (Number of streams for BigQuery Storage Write API): Number of streams defines the parallelism of the BigQueryIO’s Write transform and roughly corresponds to the number of Storage Write API’s streams which will be used by the pipeline. See https://cloud.google.com/blog/products/data-analytics/streaming-data-into-bigquery-using-storage-write-api for the recommended values. Defaults to: 0.
* **storageWriteApiTriggeringFrequencySec** (Triggering frequency in seconds for BigQuery Storage Write API): Triggering frequency will determine how soon the data will be visible for querying in BigQuery. See https://cloud.google.com/blog/products/data-analytics/streaming-data-into-bigquery-using-storage-write-api for the recommended values.
* **javascriptDocumentTransformGcsPath** (JavaScript UDF path in Cloud Storage.): The Cloud Storage path pattern for the JavaScript code containing your user-defined functions. (Example: gs://your-bucket/your-transforms/*.js).
* **javascriptDocumentTransformFunctionName** (The name of the JavaScript function to call as your UDF.): The function name should only contain letters, digits and underscores. Example: 'transform' or 'transform_udf1'. (Example: transform).



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!
[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/mongodb-to-googlecloud/src/main/java/com/google/cloud/teleport/v2/mongodb/templates/MongoDbToBigQueryCdc.java)

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates#templates-plugin)
. Install the plugin with the following command before proceeding:

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
-DtemplateName="MongoDB_to_BigQuery_CDC" \
-pl v2/mongodb-to-googlecloud \
-am
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/MongoDB_to_BigQuery_CDC
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/MongoDB_to_BigQuery_CDC"

### Required
export MONGO_DB_URI=mongouri
export DATABASE=<database>
export COLLECTION=collection
export USER_OPTION=NONE
export INPUT_TOPIC=<inputTopic>
export OUTPUT_TABLE_SPEC=bqtable

### Optional
export USE_STORAGE_WRITE_API=false
export USE_STORAGE_WRITE_API_AT_LEAST_ONCE=false
export NUM_STORAGE_WRITE_API_STREAMS=0
export STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC=<storageWriteApiTriggeringFrequencySec>
export JAVASCRIPT_DOCUMENT_TRANSFORM_GCS_PATH=<javascriptDocumentTransformGcsPath>
export JAVASCRIPT_DOCUMENT_TRANSFORM_FUNCTION_NAME=<javascriptDocumentTransformFunctionName>

gcloud dataflow flex-template run "mongodb-to-bigquery-cdc-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "mongoDbUri=$MONGO_DB_URI" \
  --parameters "database=$DATABASE" \
  --parameters "collection=$COLLECTION" \
  --parameters "userOption=$USER_OPTION" \
  --parameters "useStorageWriteApi=$USE_STORAGE_WRITE_API" \
  --parameters "useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE" \
  --parameters "numStorageWriteApiStreams=$NUM_STORAGE_WRITE_API_STREAMS" \
  --parameters "storageWriteApiTriggeringFrequencySec=$STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC" \
  --parameters "inputTopic=$INPUT_TOPIC" \
  --parameters "outputTableSpec=$OUTPUT_TABLE_SPEC" \
  --parameters "javascriptDocumentTransformGcsPath=$JAVASCRIPT_DOCUMENT_TRANSFORM_GCS_PATH" \
  --parameters "javascriptDocumentTransformFunctionName=$JAVASCRIPT_DOCUMENT_TRANSFORM_FUNCTION_NAME"
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
export MONGO_DB_URI=mongouri
export DATABASE=<database>
export COLLECTION=collection
export USER_OPTION=NONE
export INPUT_TOPIC=<inputTopic>
export OUTPUT_TABLE_SPEC=bqtable

### Optional
export USE_STORAGE_WRITE_API=false
export USE_STORAGE_WRITE_API_AT_LEAST_ONCE=false
export NUM_STORAGE_WRITE_API_STREAMS=0
export STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC=<storageWriteApiTriggeringFrequencySec>
export JAVASCRIPT_DOCUMENT_TRANSFORM_GCS_PATH=<javascriptDocumentTransformGcsPath>
export JAVASCRIPT_DOCUMENT_TRANSFORM_FUNCTION_NAME=<javascriptDocumentTransformFunctionName>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="mongodb-to-bigquery-cdc-job" \
-DtemplateName="MongoDB_to_BigQuery_CDC" \
-Dparameters="mongoDbUri=$MONGO_DB_URI,database=$DATABASE,collection=$COLLECTION,userOption=$USER_OPTION,useStorageWriteApi=$USE_STORAGE_WRITE_API,useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE,numStorageWriteApiStreams=$NUM_STORAGE_WRITE_API_STREAMS,storageWriteApiTriggeringFrequencySec=$STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC,inputTopic=$INPUT_TOPIC,outputTableSpec=$OUTPUT_TABLE_SPEC,javascriptDocumentTransformGcsPath=$JAVASCRIPT_DOCUMENT_TRANSFORM_GCS_PATH,javascriptDocumentTransformFunctionName=$JAVASCRIPT_DOCUMENT_TRANSFORM_FUNCTION_NAME" \
-pl v2/mongodb-to-googlecloud \
-am
```
