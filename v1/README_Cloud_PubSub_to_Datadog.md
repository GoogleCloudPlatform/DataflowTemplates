
Pub/Sub to Datadog template
---
The Pub/Sub to Datadog template is a streaming pipeline that reads messages from
a Pub/Sub subscription and writes the message payload to Datadog by using a
Datadog endpoint. The most common use case for this template is to export log
files to Datadog.

Before writing to Datadog, you can apply a JavaScript user-defined function to
the message payload. Any messages that experience processing failures are
forwarded to a Pub/Sub unprocessed topic for further troubleshooting and
reprocessing.

As an extra layer of protection for your API keys and secrets, you can also pass
in a Cloud KMS key along with the base64-encoded API key parameter encrypted with
the Cloud KMS key. For additional details about encrypting your API key
parameter, see the <a
href="https://cloud.google.com/kms/docs/reference/rest/v1/projects.locations.keyRings.cryptoKeys/encrypt">Cloud
KMS API encryption endpoint</a>.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/pubsub-to-datadog)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Cloud_PubSub_to_Datadog).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **inputSubscription** : The Pub/Sub subscription to read the input from. (Example: projects/your-project-id/subscriptions/your-subscription-name).
* **url** : The Datadog Logs API URL. This URL must be routable from the VPC that the pipeline runs in. See Send logs (https://docs.datadoghq.com/api/latest/logs/#send-logs) in the Datadog documentation for more information. (Example: https://http-intake.logs.datadoghq.com).
* **outputDeadletterTopic** : The Pub/Sub topic to forward undeliverable messages to. For example, projects/<PROJECT_ID>/topics/<TOPIC_NAME>.

### Optional parameters

* **apiKey** : The Datadog API key. You must provide this value if the `apiKeySource` is set to `PLAINTEXT` or `KMS`. For more information, see API and Application Keys (https://docs.datadoghq.com/account_management/api-app-keys/) in the Datadog documentation.
* **batchCount** : The batch size for sending multiple events to Datadog. The default is `1` (no batching).
* **parallelism** : The maximum number of parallel requests. The default is `1` (no parallelism).
* **includePubsubMessage** : Whether to include the full Pub/Sub message in the payload. The default is `true` (all elements, including the data element, are included in the payload).
* **apiKeyKMSEncryptionKey** : The Cloud KMS key to use to decrypt the API Key. You must provide this parameter if the `apiKeySource` is set to `KMS`. If the Cloud KMS key is provided, you must pass in an encrypted API Key. (Example: projects/your-project-id/locations/global/keyRings/your-keyring/cryptoKeys/your-key-name).
* **apiKeySecretId** : The Secret Manager secret ID for the API Key. You must provide this parameter if the `apiKeySource` is set to `SECRET_MANAGER`. (Example: projects/your-project-id/secrets/your-secret/versions/your-secret-version).
* **apiKeySource** : The source of the API key. The following values are supported: `PLAINTEXT`, `KMS`, and `SECRET_MANAGER`. You must provide this parameter if you're using Secret Manager. If `apiKeySource` is set to `KMS`, you must also provide `apiKeyKMSEncryptionKey` and encrypted `API Key`. If `apiKeySource` is set to `SECRET_MANAGER`, you must also provide `apiKeySecretId`. If `apiKeySource` is set to `PLAINTEXT`, you must also provide `apiKey`.
* **javascriptTextTransformGcsPath** : The Cloud Storage URI of the .js file that defines the JavaScript user-defined function (UDF) to use. For example, `gs://my-bucket/my-udfs/my_file.js`.
* **javascriptTextTransformFunctionName** : The name of the JavaScript user-defined function (UDF) to use. For example, if your JavaScript function code is `myTransform(inJson) { /*...do stuff...*/ }`, then the function name is `myTransform`. For sample JavaScript UDFs, see UDF Examples (https://github.com/GoogleCloudPlatform/DataflowTemplates#udf-examples).
* **javascriptTextTransformReloadIntervalMinutes** : Define the interval that workers may check for JavaScript UDF changes to reload the files. Defaults to: 0.


## User-Defined functions (UDFs)

The Pub/Sub to Datadog Template supports User-Defined functions (UDFs).
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

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v1/src/main/java/com/google/cloud/teleport/templates/PubSubToDatadog.java)

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#templates-plugin).

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
-DtemplateName="Cloud_PubSub_to_Datadog" \
-f v1
```

The `-DgcpTempLocation=<temp-bucket-name>` parameter can be specified to set the GCS bucket used by the DataflowRunner to write
temp files to during serialization. The path used will be `gs://<temp-bucket-name>/temp/`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Classic Template was staged! gs://<bucket-name>/templates/Cloud_PubSub_to_Datadog
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/Cloud_PubSub_to_Datadog"

### Required
export INPUT_SUBSCRIPTION=<inputSubscription>
export URL=<url>
export OUTPUT_DEADLETTER_TOPIC=<outputDeadletterTopic>

### Optional
export API_KEY=<apiKey>
export BATCH_COUNT=<batchCount>
export PARALLELISM=<parallelism>
export INCLUDE_PUBSUB_MESSAGE=true
export API_KEY_KMSENCRYPTION_KEY=<apiKeyKMSEncryptionKey>
export API_KEY_SECRET_ID=<apiKeySecretId>
export API_KEY_SOURCE=<apiKeySource>
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>
export JAVASCRIPT_TEXT_TRANSFORM_RELOAD_INTERVAL_MINUTES=0

gcloud dataflow jobs run "cloud-pubsub-to-datadog-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "inputSubscription=$INPUT_SUBSCRIPTION" \
  --parameters "apiKey=$API_KEY" \
  --parameters "url=$URL" \
  --parameters "batchCount=$BATCH_COUNT" \
  --parameters "parallelism=$PARALLELISM" \
  --parameters "includePubsubMessage=$INCLUDE_PUBSUB_MESSAGE" \
  --parameters "apiKeyKMSEncryptionKey=$API_KEY_KMSENCRYPTION_KEY" \
  --parameters "apiKeySecretId=$API_KEY_SECRET_ID" \
  --parameters "apiKeySource=$API_KEY_SOURCE" \
  --parameters "javascriptTextTransformGcsPath=$JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH" \
  --parameters "javascriptTextTransformFunctionName=$JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME" \
  --parameters "javascriptTextTransformReloadIntervalMinutes=$JAVASCRIPT_TEXT_TRANSFORM_RELOAD_INTERVAL_MINUTES" \
  --parameters "outputDeadletterTopic=$OUTPUT_DEADLETTER_TOPIC"
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
export INPUT_SUBSCRIPTION=<inputSubscription>
export URL=<url>
export OUTPUT_DEADLETTER_TOPIC=<outputDeadletterTopic>

### Optional
export API_KEY=<apiKey>
export BATCH_COUNT=<batchCount>
export PARALLELISM=<parallelism>
export INCLUDE_PUBSUB_MESSAGE=true
export API_KEY_KMSENCRYPTION_KEY=<apiKeyKMSEncryptionKey>
export API_KEY_SECRET_ID=<apiKeySecretId>
export API_KEY_SOURCE=<apiKeySource>
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>
export JAVASCRIPT_TEXT_TRANSFORM_RELOAD_INTERVAL_MINUTES=0

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="cloud-pubsub-to-datadog-job" \
-DtemplateName="Cloud_PubSub_to_Datadog" \
-Dparameters="inputSubscription=$INPUT_SUBSCRIPTION,apiKey=$API_KEY,url=$URL,batchCount=$BATCH_COUNT,parallelism=$PARALLELISM,includePubsubMessage=$INCLUDE_PUBSUB_MESSAGE,apiKeyKMSEncryptionKey=$API_KEY_KMSENCRYPTION_KEY,apiKeySecretId=$API_KEY_SECRET_ID,apiKeySource=$API_KEY_SOURCE,javascriptTextTransformGcsPath=$JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH,javascriptTextTransformFunctionName=$JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME,javascriptTextTransformReloadIntervalMinutes=$JAVASCRIPT_TEXT_TRANSFORM_RELOAD_INTERVAL_MINUTES,outputDeadletterTopic=$OUTPUT_DEADLETTER_TOPIC" \
-f v1
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
see [dataflow_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_job).

Terraform modules have been generated for most templates in this repository. This includes the relevant parameters
specific to the template. If available, they may be used instead of
[dataflow_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_job)
directly.

To use the autogenerated module, execute the standard
[terraform workflow](https://developer.hashicorp.com/terraform/intro/core-workflow):

```shell
cd v1/terraform/Cloud_PubSub_to_Datadog
terraform init
terraform apply
```

To use
[dataflow_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_job)
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

resource "google_dataflow_job" "cloud_pubsub_to_datadog" {

  provider          = google-beta
  template_gcs_path = "gs://dataflow-templates-${var.region}/latest/Cloud_PubSub_to_Datadog"
  name              = "cloud-pubsub-to-datadog"
  region            = var.region
  temp_gcs_location = "gs://bucket-name-here/temp"
  parameters        = {
    inputSubscription = "projects/your-project-id/subscriptions/your-subscription-name"
    url = "https://http-intake.logs.datadoghq.com"
    outputDeadletterTopic = "<outputDeadletterTopic>"
    # apiKey = "<apiKey>"
    # batchCount = "<batchCount>"
    # parallelism = "<parallelism>"
    # includePubsubMessage = "true"
    # apiKeyKMSEncryptionKey = "projects/your-project-id/locations/global/keyRings/your-keyring/cryptoKeys/your-key-name"
    # apiKeySecretId = "projects/your-project-id/secrets/your-secret/versions/your-secret-version"
    # apiKeySource = "<apiKeySource>"
    # javascriptTextTransformGcsPath = "<javascriptTextTransformGcsPath>"
    # javascriptTextTransformFunctionName = "<javascriptTextTransformFunctionName>"
    # javascriptTextTransformReloadIntervalMinutes = "0"
  }
}
```
