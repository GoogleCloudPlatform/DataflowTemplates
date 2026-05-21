
MongoDB to MongoDB template
---
Copy data from one MongoDB database to another.



:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **sourceUri**: URI to connect to the source MongoDB cluster.
* **targetUri**: URI to connect to the target MongoDB cluster.
* **sourceDatabase**: Database in the source MongoDB to read from.
* **targetDatabase**: Database in the target MongoDB to write to.

### Optional parameters

* **sourceCollection**: Collection in the source MongoDB to read from. If not provided, all collections in the database will be migrated.
* **targetCollection**: Collection in the target MongoDB to write to. If not provided, source collection names will be used.
* **useBucketAuto**: Enable withBucketAuto for Atlas compatibility. Defaults to: false.
* **numSplits**: Suggest a specific number of partitions for reading.
* **batchSize**: Number of documents in a bulk write. Defaults to: 5000.
* **dlqDirectory**: Base path to store failed events. Events will be grouped by date and time, and separated into 'retryable' and 'permanent' subdirectories.
* **maxConcurrentAsyncWrites**: Maximum number of concurrent asynchronous batch writes per worker. Defaults to: 10.
* **maxWriteRetries**: Maximum number of retry attempts for transient failures during write. Defaults to: 3.
* **dlqMaxRetries**: Maximum number of times to retry events from DLQ. Defaults to: 3.
* **reconsumeDlqPath**: Path to read files from DLQ for reprocessing. If not provided, write DLQ path will be used.
* **readFromDlq**: If true, reads only from DLQ for retry. If false, reads from MongoDB. Defaults to: false.
* **javascriptTextTransformGcsPath**: The Cloud Storage URI of the .js file that defines the JavaScript user-defined function (UDF) to use. For example, `gs://my-bucket/my-udfs/my_file.js`.
* **javascriptTextTransformFunctionName**: The name of the JavaScript user-defined function (UDF) to use. For example, if your JavaScript function code is `myTransform(inJson) { /*...do stuff...*/ }`, then the function name is `myTransform`. For sample JavaScript UDFs, see UDF Examples (https://github.com/GoogleCloudPlatform/DataflowTemplates#udf-examples).
* **javascriptTextTransformReloadIntervalMinutes**: Specifies how frequently to reload the UDF, in minutes. If the value is greater than 0, Dataflow periodically checks the UDF file in Cloud Storage, and reloads the UDF if the file is modified. This parameter allows you to update the UDF while the pipeline is running, without needing to restart the job. If the value is `0`, UDF reloading is disabled. The default value is `0`.


## User-Defined functions (UDFs)

The MongoDB to MongoDB Template supports User-Defined functions (UDFs).
UDFs allow you to customize functionality by providing a JavaScript function
without having to maintain or build the entire template code.

Check [Create user-defined functions for Dataflow templates](https://cloud.google.com/dataflow/docs/guides/templates/create-template-udf)
and [Using UDFs](https://github.com/GoogleCloudPlatform/DataflowTemplates#using-udfs)
for more information about how to create and test those functions.


## Getting Started

### Requirements

* Java 17
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/mongodb-to-mongodb/src/main/java/com/google/cloud/teleport/v2/templates/MongoDbToMongoDb.java)

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#templates-plugin).

#### Validating the Template

This template has a validation command that is used to check code quality.

```shell
mvn clean install -PtemplatesValidate \
-DskipTests -am \
-pl v2/mongodb-to-mongodb
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
-DtemplateName="Mongodb_To_Mongodb" \
-pl v2/mongodb-to-mongodb -am
```

The `-DartifactRegistry` parameter can be specified to set the artifact registry repository of the Flex Templates image.
If not provided, it defaults to `gcr.io/<project>`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Mongodb_To_Mongodb
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Mongodb_To_Mongodb"

### Required
export SOURCE_URI=<sourceUri>
export TARGET_URI=<targetUri>
export SOURCE_DATABASE=<sourceDatabase>
export TARGET_DATABASE=<targetDatabase>

### Optional
export SOURCE_COLLECTION=<sourceCollection>
export TARGET_COLLECTION=<targetCollection>
export USE_BUCKET_AUTO=false
export NUM_SPLITS=<numSplits>
export BATCH_SIZE=5000
export DLQ_DIRECTORY=<dlqDirectory>
export MAX_CONCURRENT_ASYNC_WRITES=10
export MAX_WRITE_RETRIES=3
export DLQ_MAX_RETRIES=3
export RECONSUME_DLQ_PATH=<reconsumeDlqPath>
export READ_FROM_DLQ=false
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>
export JAVASCRIPT_TEXT_TRANSFORM_RELOAD_INTERVAL_MINUTES=0

gcloud dataflow flex-template run "mongodb-to-mongodb-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "sourceUri=$SOURCE_URI" \
  --parameters "targetUri=$TARGET_URI" \
  --parameters "sourceDatabase=$SOURCE_DATABASE" \
  --parameters "targetDatabase=$TARGET_DATABASE" \
  --parameters "sourceCollection=$SOURCE_COLLECTION" \
  --parameters "targetCollection=$TARGET_COLLECTION" \
  --parameters "useBucketAuto=$USE_BUCKET_AUTO" \
  --parameters "numSplits=$NUM_SPLITS" \
  --parameters "batchSize=$BATCH_SIZE" \
  --parameters "dlqDirectory=$DLQ_DIRECTORY" \
  --parameters "maxConcurrentAsyncWrites=$MAX_CONCURRENT_ASYNC_WRITES" \
  --parameters "maxWriteRetries=$MAX_WRITE_RETRIES" \
  --parameters "dlqMaxRetries=$DLQ_MAX_RETRIES" \
  --parameters "reconsumeDlqPath=$RECONSUME_DLQ_PATH" \
  --parameters "readFromDlq=$READ_FROM_DLQ" \
  --parameters "javascriptTextTransformGcsPath=$JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH" \
  --parameters "javascriptTextTransformFunctionName=$JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME" \
  --parameters "javascriptTextTransformReloadIntervalMinutes=$JAVASCRIPT_TEXT_TRANSFORM_RELOAD_INTERVAL_MINUTES"
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
export SOURCE_URI=<sourceUri>
export TARGET_URI=<targetUri>
export SOURCE_DATABASE=<sourceDatabase>
export TARGET_DATABASE=<targetDatabase>

### Optional
export SOURCE_COLLECTION=<sourceCollection>
export TARGET_COLLECTION=<targetCollection>
export USE_BUCKET_AUTO=false
export NUM_SPLITS=<numSplits>
export BATCH_SIZE=5000
export DLQ_DIRECTORY=<dlqDirectory>
export MAX_CONCURRENT_ASYNC_WRITES=10
export MAX_WRITE_RETRIES=3
export DLQ_MAX_RETRIES=3
export RECONSUME_DLQ_PATH=<reconsumeDlqPath>
export READ_FROM_DLQ=false
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>
export JAVASCRIPT_TEXT_TRANSFORM_RELOAD_INTERVAL_MINUTES=0

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="mongodb-to-mongodb-job" \
-DtemplateName="Mongodb_To_Mongodb" \
-Dparameters="sourceUri=$SOURCE_URI,targetUri=$TARGET_URI,sourceDatabase=$SOURCE_DATABASE,targetDatabase=$TARGET_DATABASE,sourceCollection=$SOURCE_COLLECTION,targetCollection=$TARGET_COLLECTION,useBucketAuto=$USE_BUCKET_AUTO,numSplits=$NUM_SPLITS,batchSize=$BATCH_SIZE,dlqDirectory=$DLQ_DIRECTORY,maxConcurrentAsyncWrites=$MAX_CONCURRENT_ASYNC_WRITES,maxWriteRetries=$MAX_WRITE_RETRIES,dlqMaxRetries=$DLQ_MAX_RETRIES,reconsumeDlqPath=$RECONSUME_DLQ_PATH,readFromDlq=$READ_FROM_DLQ,javascriptTextTransformGcsPath=$JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH,javascriptTextTransformFunctionName=$JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME,javascriptTextTransformReloadIntervalMinutes=$JAVASCRIPT_TEXT_TRANSFORM_RELOAD_INTERVAL_MINUTES" \
-f v2/mongodb-to-mongodb
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
cd v2/mongodb-to-mongodb/terraform/Mongodb_To_Mongodb
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

resource "google_dataflow_flex_template_job" "mongodb_to_mongodb" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Mongodb_To_Mongodb"
  name              = "mongodb-to-mongodb"
  region            = var.region
  parameters        = {
    sourceUri = "<sourceUri>"
    targetUri = "<targetUri>"
    sourceDatabase = "<sourceDatabase>"
    targetDatabase = "<targetDatabase>"
    # sourceCollection = "<sourceCollection>"
    # targetCollection = "<targetCollection>"
    # useBucketAuto = "false"
    # numSplits = "<numSplits>"
    # batchSize = "5000"
    # dlqDirectory = "<dlqDirectory>"
    # maxConcurrentAsyncWrites = "10"
    # maxWriteRetries = "3"
    # dlqMaxRetries = "3"
    # reconsumeDlqPath = "<reconsumeDlqPath>"
    # readFromDlq = "false"
    # javascriptTextTransformGcsPath = "<javascriptTextTransformGcsPath>"
    # javascriptTextTransformFunctionName = "<javascriptTextTransformFunctionName>"
    # javascriptTextTransformReloadIntervalMinutes = "0"
  }
}
```
