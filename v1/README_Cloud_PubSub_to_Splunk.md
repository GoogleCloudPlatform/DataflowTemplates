
Pub/Sub to Splunk template
---
The Pub/Sub to Splunk template is a streaming pipeline that reads messages from a
Pub/Sub subscription and writes the message payload to Splunk via Splunk's HTTP
Event Collector (HEC). The most common use case of this template is to export
logs to Splunk. To see an example of the underlying workflow, see <a
href="https://cloud.google.com/architecture/deploying-production-ready-log-exports-to-splunk-using-dataflow">Deploying
production-ready log exports to Splunk using Dataflow</a>.

Before writing to Splunk, you can also apply a JavaScript user-defined function
to the message payload. Any messages that experience processing failures are
forwarded to a Pub/Sub unprocessed topic for further troubleshooting and
reprocessing.

As an extra layer of protection for your HEC token, you can also pass in a Cloud
KMS key along with the base64-encoded HEC token parameter encrypted with the
Cloud KMS key. See the <a
href="https://cloud.google.com/kms/docs/reference/rest/v1/projects.locations.keyRings.cryptoKeys/encrypt">Cloud
KMS API encryption endpoint</a> for additional details on encrypting your HEC
token parameter.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/pubsub-to-splunk)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Cloud_PubSub_to_Splunk).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **inputSubscription** : The Pub/Sub subscription to read the input from. (Example: projects/your-project-id/subscriptions/your-subscription-name).
* **url** : The Splunk HEC URL. The URL must be routable from the VPC that the pipeline runs in. (Example: https://splunk-hec-host:8088).
* **outputDeadletterTopic** : The Pub/Sub topic to forward undeliverable messages to. For example, `projects/<PROJECT_ID>/topics/<TOPIC_NAME>`.

### Optional parameters

* **token** : The Splunk HEC authentication token. Must be provided if the `tokenSource` parameter is set to `PLAINTEXT` or `KMS`.
* **batchCount** : The batch size for sending multiple events to Splunk. Defaults to `1` (no batching).
* **disableCertificateValidation** : Disable SSL certificate validation. Default `false` (validation enabled). If `true`, the certificates are not validated (all certificates are trusted) and `rootCaCertificatePath` parameter is ignored.
* **parallelism** : The maximum number of parallel requests. Defaults to `1` (no parallelism).
* **includePubsubMessage** : Include the full Pub/Sub message in the payload. Default `false` (only the data element is included in the payload).
* **tokenKMSEncryptionKey** : The Cloud KMS key to use to decrypt the HEC token string. This parameter must be provided when tokenSource is set to KMS. If the Cloud KMS key is provided, the HEC token string must be passed in encrypted. (Example: projects/your-project-id/locations/global/keyRings/your-keyring/cryptoKeys/your-key-name).
* **tokenSecretId** : The Secret Manager secret ID for the token. This parameter must provided when the tokenSource is set to `SECRET_MANAGER`. (Example: projects/your-project-id/secrets/your-secret/versions/your-secret-version).
* **tokenSource** : The source of the token. The following values are allowed: `PLAINTEXT`, `KMS`, and `SECRET_MANAGER`. You must provide this parameter when Secret Manager is used. If `tokenSource` is set to `KMS`, `tokenKMSEncryptionKey`, and encrypted, then `token` must be provided. If `tokenSource` is set to `SECRET_MANAGER`, then `tokenSecretId` must be provided. If `tokenSource` is set to `PLAINTEXT`, then `token` must be provided.
* **rootCaCertificatePath** : The full URL to the root CA certificate in Cloud Storage. The certificate provided in Cloud Storage must be DER-encoded and can be supplied in binary or printable (Base64) encoding. If the certificate is provided in Base64 encoding, it must be bounded at the beginning by -----BEGIN CERTIFICATE-----, and must be bounded at the end by -----END CERTIFICATE-----. If this parameter is provided, this private CA certificate file is fetched and added to the Dataflow worker's trust store in order to verify the Splunk HEC endpoint's SSL certificate. If this parameter is not provided, the default trust store is used. (Example: gs://mybucket/mycerts/privateCA.crt).
* **enableBatchLogs** : Specifies whether logs should be enabled for batches written to Splunk. Default: `true`.
* **enableGzipHttpCompression** : Specifies whether HTTP requests sent to Splunk HEC should be compressed (gzip content encoded). Default: `true`.
* **javascriptTextTransformGcsPath** : The Cloud Storage URI of the .js file that defines the JavaScript user-defined function (UDF) to use. For example, `gs://my-bucket/my-udfs/my_file.js`.
* **javascriptTextTransformFunctionName** : The name of the JavaScript user-defined function (UDF) to use. For example, if your JavaScript function code is `myTransform(inJson) { /*...do stuff...*/ }`, then the function name is `myTransform`. For sample JavaScript UDFs, see UDF Examples (https://github.com/GoogleCloudPlatform/DataflowTemplates#udf-examples).
* **javascriptTextTransformReloadIntervalMinutes** : Define the interval that workers may check for JavaScript UDF changes to reload the files. Defaults to: 0.


## User-Defined functions (UDFs)

The Pub/Sub to Splunk Template supports User-Defined functions (UDFs).
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

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v1/src/main/java/com/google/cloud/teleport/templates/PubSubToSplunk.java)

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates#templates-plugin).

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
-DtemplateName="Cloud_PubSub_to_Splunk" \
-f v1
```

The `-DgcpTempLocation=<temp-bucket-name>` parameter can be specified to set the GCS bucket used by the DataflowRunner to write
temp files to during serialization. The path used will be `gs://<temp-bucket-name>/temp/`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Classic Template was staged! gs://<bucket-name>/templates/Cloud_PubSub_to_Splunk
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/Cloud_PubSub_to_Splunk"

### Required
export INPUT_SUBSCRIPTION=<inputSubscription>
export URL=<url>
export OUTPUT_DEADLETTER_TOPIC=<outputDeadletterTopic>

### Optional
export TOKEN=<token>
export BATCH_COUNT=<batchCount>
export DISABLE_CERTIFICATE_VALIDATION=<disableCertificateValidation>
export PARALLELISM=<parallelism>
export INCLUDE_PUBSUB_MESSAGE=<includePubsubMessage>
export TOKEN_KMSENCRYPTION_KEY=<tokenKMSEncryptionKey>
export TOKEN_SECRET_ID=<tokenSecretId>
export TOKEN_SOURCE=<tokenSource>
export ROOT_CA_CERTIFICATE_PATH=<rootCaCertificatePath>
export ENABLE_BATCH_LOGS=true
export ENABLE_GZIP_HTTP_COMPRESSION=true
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>
export JAVASCRIPT_TEXT_TRANSFORM_RELOAD_INTERVAL_MINUTES=0

gcloud dataflow jobs run "cloud-pubsub-to-splunk-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "inputSubscription=$INPUT_SUBSCRIPTION" \
  --parameters "token=$TOKEN" \
  --parameters "url=$URL" \
  --parameters "batchCount=$BATCH_COUNT" \
  --parameters "disableCertificateValidation=$DISABLE_CERTIFICATE_VALIDATION" \
  --parameters "parallelism=$PARALLELISM" \
  --parameters "includePubsubMessage=$INCLUDE_PUBSUB_MESSAGE" \
  --parameters "tokenKMSEncryptionKey=$TOKEN_KMSENCRYPTION_KEY" \
  --parameters "tokenSecretId=$TOKEN_SECRET_ID" \
  --parameters "tokenSource=$TOKEN_SOURCE" \
  --parameters "rootCaCertificatePath=$ROOT_CA_CERTIFICATE_PATH" \
  --parameters "enableBatchLogs=$ENABLE_BATCH_LOGS" \
  --parameters "enableGzipHttpCompression=$ENABLE_GZIP_HTTP_COMPRESSION" \
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
export TOKEN=<token>
export BATCH_COUNT=<batchCount>
export DISABLE_CERTIFICATE_VALIDATION=<disableCertificateValidation>
export PARALLELISM=<parallelism>
export INCLUDE_PUBSUB_MESSAGE=<includePubsubMessage>
export TOKEN_KMSENCRYPTION_KEY=<tokenKMSEncryptionKey>
export TOKEN_SECRET_ID=<tokenSecretId>
export TOKEN_SOURCE=<tokenSource>
export ROOT_CA_CERTIFICATE_PATH=<rootCaCertificatePath>
export ENABLE_BATCH_LOGS=true
export ENABLE_GZIP_HTTP_COMPRESSION=true
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>
export JAVASCRIPT_TEXT_TRANSFORM_RELOAD_INTERVAL_MINUTES=0

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="cloud-pubsub-to-splunk-job" \
-DtemplateName="Cloud_PubSub_to_Splunk" \
-Dparameters="inputSubscription=$INPUT_SUBSCRIPTION,token=$TOKEN,url=$URL,batchCount=$BATCH_COUNT,disableCertificateValidation=$DISABLE_CERTIFICATE_VALIDATION,parallelism=$PARALLELISM,includePubsubMessage=$INCLUDE_PUBSUB_MESSAGE,tokenKMSEncryptionKey=$TOKEN_KMSENCRYPTION_KEY,tokenSecretId=$TOKEN_SECRET_ID,tokenSource=$TOKEN_SOURCE,rootCaCertificatePath=$ROOT_CA_CERTIFICATE_PATH,enableBatchLogs=$ENABLE_BATCH_LOGS,enableGzipHttpCompression=$ENABLE_GZIP_HTTP_COMPRESSION,javascriptTextTransformGcsPath=$JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH,javascriptTextTransformFunctionName=$JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME,javascriptTextTransformReloadIntervalMinutes=$JAVASCRIPT_TEXT_TRANSFORM_RELOAD_INTERVAL_MINUTES,outputDeadletterTopic=$OUTPUT_DEADLETTER_TOPIC" \
-f v1
```

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
cd v1/terraform/Cloud_PubSub_to_Splunk
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

resource "google_dataflow_job" "cloud_pubsub_to_splunk" {

  provider          = google-beta
  template_gcs_path = "gs://dataflow-templates-${var.region}/latest/Cloud_PubSub_to_Splunk"
  name              = "cloud-pubsub-to-splunk"
  region            = var.region
  temp_gcs_location = "gs://bucket-name-here/temp"
  parameters        = {
    inputSubscription = "projects/your-project-id/subscriptions/your-subscription-name"
    url = "https://splunk-hec-host:8088"
    outputDeadletterTopic = "<outputDeadletterTopic>"
    # token = "<token>"
    # batchCount = "<batchCount>"
    # disableCertificateValidation = "<disableCertificateValidation>"
    # parallelism = "<parallelism>"
    # includePubsubMessage = "<includePubsubMessage>"
    # tokenKMSEncryptionKey = "projects/your-project-id/locations/global/keyRings/your-keyring/cryptoKeys/your-key-name"
    # tokenSecretId = "projects/your-project-id/secrets/your-secret/versions/your-secret-version"
    # tokenSource = "<tokenSource>"
    # rootCaCertificatePath = "gs://mybucket/mycerts/privateCA.crt"
    # enableBatchLogs = "true"
    # enableGzipHttpCompression = "true"
    # javascriptTextTransformGcsPath = "<javascriptTextTransformGcsPath>"
    # javascriptTextTransformFunctionName = "<javascriptTextTransformFunctionName>"
    # javascriptTextTransformReloadIntervalMinutes = "0"
  }
}
```
