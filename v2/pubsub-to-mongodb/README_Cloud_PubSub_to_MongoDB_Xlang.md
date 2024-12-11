
Pub/Sub to MongoDB with Python UDFs template
---
The Pub/Sub to MongoDB template is a streaming pipeline that reads JSON-encoded
messages from a Pub/Sub subscription and writes them to MongoDB as documents. If
required, this pipeline supports additional transforms that can be included using
a Python user-defined function (UDF). Any errors occurred due to schema mismatch,
malformed JSON, or while executing transforms are recorded in a BigQuery table
for unprocessed messages along with input message. If a table for unprocessed
records does not exist prior to execution, the pipeline automatically creates
this table.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/pubsub-to-mongodb)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Cloud_PubSub_to_MongoDB_Xlang).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **inputSubscription**: Name of the Pub/Sub subscription. For example, `projects/your-project-id/subscriptions/your-subscription-name`.
* **mongoDBUri**: Comma separated list of MongoDB servers. For example, `host1:port,host2:port,host3:port`.
* **database**: Database in MongoDB to store the collection. For example, `my-db`.
* **collection**: Name of the collection in the MongoDB database. For example, `my-collection`.
* **deadletterTable**: The BigQuery table that stores messages caused by failures, such as mismatched schema, malformed JSON, and so on. For example, `your-project-id:your-dataset.your-table-name`.

### Optional parameters

* **batchSize**: Batch size used for batch insertion of documents into MongoDB. Defaults to: 1000.
* **batchSizeBytes**: Batch size in bytes. Defaults to: 5242880.
* **maxConnectionIdleTime**: Maximum idle time allowed in seconds before connection timeout occurs. Defaults to: 60000.
* **sslEnabled**: Boolean value indicating whether the connection to MongoDB is SSL enabled. Defaults to: true.
* **ignoreSSLCertificate**: Boolean value indicating whether to ignore the SSL certificate. Defaults to: true.
* **withOrdered**: Boolean value enabling ordered bulk insertions into MongoDB. Defaults to: true.
* **withSSLInvalidHostNameAllowed**: Boolean value indicating whether an invalid hostname is allowed for the SSL connection. Defaults to: true.
* **pythonExternalTextTransformGcsPath**: The Cloud Storage path pattern for the Python code containing your user-defined functions. For example, `gs://your-bucket/your-function.py`.
* **pythonExternalTextTransformFunctionName**: The name of the function to call from your Python file. Use only letters, digits, and underscores. For example, `'transform' or 'transform_udf1'`.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/pubsub-to-mongodb/src/main/java/com/google/cloud/teleport/v2/templates/PubSubToMongoDB.java)

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates#templates-plugin).

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
-DtemplateName="Cloud_PubSub_to_MongoDB_Xlang" \
-f v2/pubsub-to-mongodb
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Cloud_PubSub_to_MongoDB_Xlang
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Cloud_PubSub_to_MongoDB_Xlang"

### Required
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
export PYTHON_EXTERNAL_TEXT_TRANSFORM_GCS_PATH=<pythonExternalTextTransformGcsPath>
export PYTHON_EXTERNAL_TEXT_TRANSFORM_FUNCTION_NAME=<pythonExternalTextTransformFunctionName>

gcloud dataflow flex-template run "cloud-pubsub-to-mongodb-xlang-job" \
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
  --parameters "pythonExternalTextTransformGcsPath=$PYTHON_EXTERNAL_TEXT_TRANSFORM_GCS_PATH" \
  --parameters "pythonExternalTextTransformFunctionName=$PYTHON_EXTERNAL_TEXT_TRANSFORM_FUNCTION_NAME"
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
export PYTHON_EXTERNAL_TEXT_TRANSFORM_GCS_PATH=<pythonExternalTextTransformGcsPath>
export PYTHON_EXTERNAL_TEXT_TRANSFORM_FUNCTION_NAME=<pythonExternalTextTransformFunctionName>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="cloud-pubsub-to-mongodb-xlang-job" \
-DtemplateName="Cloud_PubSub_to_MongoDB_Xlang" \
-Dparameters="inputSubscription=$INPUT_SUBSCRIPTION,mongoDBUri=$MONGO_DBURI,database=$DATABASE,collection=$COLLECTION,deadletterTable=$DEADLETTER_TABLE,batchSize=$BATCH_SIZE,batchSizeBytes=$BATCH_SIZE_BYTES,maxConnectionIdleTime=$MAX_CONNECTION_IDLE_TIME,sslEnabled=$SSL_ENABLED,ignoreSSLCertificate=$IGNORE_SSLCERTIFICATE,withOrdered=$WITH_ORDERED,withSSLInvalidHostNameAllowed=$WITH_SSLINVALID_HOST_NAME_ALLOWED,pythonExternalTextTransformGcsPath=$PYTHON_EXTERNAL_TEXT_TRANSFORM_GCS_PATH,pythonExternalTextTransformFunctionName=$PYTHON_EXTERNAL_TEXT_TRANSFORM_FUNCTION_NAME" \
-f v2/pubsub-to-mongodb
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
cd v2/pubsub-to-mongodb/terraform/Cloud_PubSub_to_MongoDB_Xlang
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

resource "google_dataflow_flex_template_job" "cloud_pubsub_to_mongodb_xlang" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Cloud_PubSub_to_MongoDB_Xlang"
  name              = "cloud-pubsub-to-mongodb-xlang"
  region            = var.region
  parameters        = {
    inputSubscription = "<inputSubscription>"
    mongoDBUri = "<mongoDBUri>"
    database = "<database>"
    collection = "<collection>"
    deadletterTable = "<deadletterTable>"
    # batchSize = "1000"
    # batchSizeBytes = "5242880"
    # maxConnectionIdleTime = "60000"
    # sslEnabled = "true"
    # ignoreSSLCertificate = "true"
    # withOrdered = "true"
    # withSSLInvalidHostNameAllowed = "true"
    # pythonExternalTextTransformGcsPath = "<pythonExternalTextTransformGcsPath>"
    # pythonExternalTextTransformFunctionName = "<pythonExternalTextTransformFunctionName>"
  }
}
```
