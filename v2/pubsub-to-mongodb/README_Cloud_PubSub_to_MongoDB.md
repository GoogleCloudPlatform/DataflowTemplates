Pub/Sub to MongoDB Template
---
Streaming pipeline that reads JSON encoded messages from a Pub/Sub subscription, transforms them using a JavaScript user-defined function (UDF), and writes them to a MongoDB as documents.

:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/pubsub-to-mongodb)
on how to use it without having to build from sources.

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Mandatory Parameters

* **inputSubscription** (Pub/Sub input subscription): Pub/Sub subscription to read the input from, in the format of 'projects/your-project-id/subscriptions/your-subscription-name' (Example: projects/your-project-id/subscriptions/your-subscription-name).
* **mongoDBUri** (MongoDB Connection URI): List of Mongo DB nodes separated by comma. (Example: host1:port,host2:port,host3:port).
* **database** (MongoDB Database): Database in MongoDB to store the collection. (Example: my-db).
* **collection** (MongoDB collection): Name of the collection inside MongoDB database to put the documents to. (Example: my-collection).
* **deadletterTable** (The dead-letter table name to output failed messages to BigQuery): Messages failed to reach the output table for all kind of reasons (e.g., mismatched schema, malformed json) are written to this table. If it doesn't exist, it will be created during pipeline execution. If not specified, "outputTableSpec_error_records" is used instead. (Example: your-project-id:your-dataset.your-table-name).

### Optional Parameters

* **batchSize** (Batch Size): Batch Size used for batch insertion of documents into MongoDB. Defaults to: 1000.
* **batchSizeBytes** (Batch Size in Bytes): Batch Size in bytes used for batch insertion of documents into MongoDB. Default: 5242880 (5mb).
* **maxConnectionIdleTime** (Max Connection idle time): Maximum idle time allowed in seconds before connection timeout occurs. Defaults to: 60000.
* **sslEnabled** (SSL Enabled): Indicates whether connection to MongoDB is ssl enabled. Defaults to: true.
* **ignoreSSLCertificate** (Ignore SSL Certificate): Indicates whether SSL certificate should be ignored. Defaults to: true.
* **withOrdered** (withOrdered): Enables ordered bulk insertions into MongoDB. Defaults to: true.
* **withSSLInvalidHostNameAllowed** (withSSLInvalidHostNameAllowed): Indicates whether invalid host name is allowed for ssl connection. Defaults to: true.
* **javascriptTextTransformGcsPath** (Cloud Storage path to Javascript UDF source): The Cloud Storage path pattern for the JavaScript code containing your user-defined functions. (Example: gs://your-bucket/your-function.js).
* **javascriptTextTransformFunctionName** (UDF Javascript Function Name): The name of the function to call from your JavaScript file. Use only letters, digits, and underscores. (Example: 'transform' or 'transform_udf1').

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
-DtemplateName="Cloud_PubSub_to_MongoDB" \
-pl v2/pubsub-to-mongodb \
-am
```

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Cloud_PubSub_to_MongoDB
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Cloud_PubSub_to_MongoDB"

### Mandatory
export INPUT_SUBSCRIPTION=<inputSubscription>
export MONGO_DBURI=<mongoDBUri>
export DATABASE=<database>
export COLLECTION=<collection>
export DEADLETTER_TABLE=<deadletterTable>

### Optional
export BATCH_SIZE=1000
export BATCH_SIZE_BYTES=5242880
export MAX_CONNECTION_IDLE_TIME=60000
export SSL_ENABLED=true
export IGNORE_SSLCERTIFICATE=true
export WITH_ORDERED=true
export WITH_SSLINVALID_HOST_NAME_ALLOWED=true
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>

gcloud dataflow flex-template run "cloud-pubsub-to-mongodb-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "inputSubscription=$INPUT_SUBSCRIPTION" \
  --parameters "mongoDBUri=$MONGO_DBURI" \
  --parameters "database=$DATABASE" \
  --parameters "collection=$COLLECTION" \
  --parameters "deadletterTable=$DEADLETTER_TABLE" \
  --parameters "batchSize=$BATCH_SIZE" \
  --parameters "batchSizeBytes=$BATCH_SIZE_BYTES" \
  --parameters "maxConnectionIdleTime=$MAX_CONNECTION_IDLE_TIME" \
  --parameters "sslEnabled=$SSL_ENABLED" \
  --parameters "ignoreSSLCertificate=$IGNORE_SSLCERTIFICATE" \
  --parameters "withOrdered=$WITH_ORDERED" \
  --parameters "withSSLInvalidHostNameAllowed=$WITH_SSLINVALID_HOST_NAME_ALLOWED" \
  --parameters "javascriptTextTransformGcsPath=$JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH" \
  --parameters "javascriptTextTransformFunctionName=$JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME"
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
export INPUT_SUBSCRIPTION=<inputSubscription>
export MONGO_DBURI=<mongoDBUri>
export DATABASE=<database>
export COLLECTION=<collection>
export DEADLETTER_TABLE=<deadletterTable>

### Optional
export BATCH_SIZE=1000
export BATCH_SIZE_BYTES=5242880
export MAX_CONNECTION_IDLE_TIME=60000
export SSL_ENABLED=true
export IGNORE_SSLCERTIFICATE=true
export WITH_ORDERED=true
export WITH_SSLINVALID_HOST_NAME_ALLOWED=true
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="cloud-pubsub-to-mongodb-job" \
-DtemplateName="Cloud_PubSub_to_MongoDB" \
-Dparameters="inputSubscription=$INPUT_SUBSCRIPTION,mongoDBUri=$MONGO_DBURI,database=$DATABASE,collection=$COLLECTION,deadletterTable=$DEADLETTER_TABLE,batchSize=$BATCH_SIZE,batchSizeBytes=$BATCH_SIZE_BYTES,maxConnectionIdleTime=$MAX_CONNECTION_IDLE_TIME,sslEnabled=$SSL_ENABLED,ignoreSSLCertificate=$IGNORE_SSLCERTIFICATE,withOrdered=$WITH_ORDERED,withSSLInvalidHostNameAllowed=$WITH_SSLINVALID_HOST_NAME_ALLOWED,javascriptTextTransformGcsPath=$JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH,javascriptTextTransformFunctionName=$JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME" \
-pl v2/pubsub-to-mongodb \
-am
```
