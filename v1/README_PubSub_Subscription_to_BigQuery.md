Pub/Sub Subscription to BigQuery Template
---
Streaming pipeline. Ingests JSON-encoded messages from a Pub/Sub subscription, transforms them using a JavaScript user-defined function (UDF), and writes them to a pre-existing BigQuery table as BigQuery elements.

:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/pubsub-subscription-to-bigquery)
on how to use it without having to build from sources.

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Mandatory Parameters

* **outputTableSpec** (BigQuery output table): BigQuery table location to write the output to. The table’s schema must match the input JSON objects.
* **inputSubscription** (Pub/Sub input subscription): Pub/Sub subscription to read the input from, in the format of 'projects/your-project-id/subscriptions/your-subscription-name'.

### Optional Parameters

* **outputDeadletterTable** (Table for messages failed to reach the output table (i.e., Deadletter table)): Messages failed to reach the output table for all kind of reasons (e.g., mismatched schema, malformed json) are written to this table. It should be in the format of "your-project-id:your-dataset.your-table-name". If it doesn't exist, it will be created during pipeline execution. If not specified, "{outputTableSpec}_error_records" is used instead.
* **javascriptTextTransformGcsPath** (JavaScript UDF path in Cloud Storage): The Cloud Storage path pattern for the JavaScript code containing your user-defined functions.
* **javascriptTextTransformFunctionName** (JavaScript UDF name): The name of the function to call from your JavaScript file. Use only letters, digits, and underscores. (Example: transform_udf1).

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
-DtemplateName="PubSub_Subscription_to_BigQuery" \
-pl v1 \
-am
```

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Classic Template was staged! gs://<bucket-name>/templates/PubSub_Subscription_to_BigQuery
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/PubSub_Subscription_to_BigQuery"

### Mandatory
export OUTPUT_TABLE_SPEC=<outputTableSpec>
export INPUT_SUBSCRIPTION=<inputSubscription>

### Optional
export OUTPUT_DEADLETTER_TABLE=<outputDeadletterTable>
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>

gcloud dataflow jobs run "pubsub-subscription-to-bigquery-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "outputTableSpec=$OUTPUT_TABLE_SPEC" \
  --parameters "inputSubscription=$INPUT_SUBSCRIPTION" \
  --parameters "outputDeadletterTable=$OUTPUT_DEADLETTER_TABLE" \
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

### Mandatory
export OUTPUT_TABLE_SPEC=<outputTableSpec>
export INPUT_SUBSCRIPTION=<inputSubscription>

### Optional
export OUTPUT_DEADLETTER_TABLE=<outputDeadletterTable>
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="pubsub-subscription-to-bigquery-job" \
-DtemplateName="PubSub_Subscription_to_BigQuery" \
-Dparameters="outputTableSpec=$OUTPUT_TABLE_SPEC,inputSubscription=$INPUT_SUBSCRIPTION,outputDeadletterTable=$OUTPUT_DEADLETTER_TABLE,javascriptTextTransformGcsPath=$JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH,javascriptTextTransformFunctionName=$JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME" \
-pl v1 \
-am
```
