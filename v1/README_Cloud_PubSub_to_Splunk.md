Pub/Sub to Splunk Template
---
A pipeline that reads from a Pub/Sub subscription and writes to Splunk's HTTP Event Collector (HEC).

:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/pubsub-to-splunk)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Cloud_PubSub_to_Splunk).


:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required Parameters

* **inputSubscription** (Pub/Sub input subscription): Pub/Sub subscription to read the input from, in the format of 'projects/your-project-id/subscriptions/your-subscription-name' (Example: projects/your-project-id/subscriptions/your-subscription-name).
* **url** (Splunk HEC URL.): Splunk Http Event Collector (HEC) url. This should be routable from the VPC in which the pipeline runs. (Example: https://splunk-hec-host:8088).
* **outputDeadletterTopic** (Output deadletter Pub/Sub topic): The Pub/Sub topic to publish deadletter records to. The name should be in the format of projects/your-project-id/topics/your-topic-name.

### Optional Parameters

* **token** (HEC Authentication token.): Splunk Http Event Collector (HEC) authentication token. Must be provided if the tokenSource is set to PLAINTEXT or KMS.
* **batchCount** (Batch size for sending multiple events to Splunk HEC.): Batch size for sending multiple events to Splunk HEC. Defaults to 10.
* **disableCertificateValidation** (Disable SSL certificate validation.): Disable SSL certificate validation (true/false). Default false (validation enabled). If true, the certificates are not validated (all certificates are trusted) and  `rootCaCertificatePath` parameter is ignored.
* **parallelism** (Maximum number of parallel requests.): Maximum number of parallel requests. Default 1 (no parallelism).
* **includePubsubMessage** (Include full Pub/Sub message in the payload.): Include full Pub/Sub message in the payload (true/false). Defaults to false (only data element is included in the payload).
* **tokenKMSEncryptionKey** (Google Cloud KMS encryption key for the token): The Cloud KMS key to decrypt the HEC token string. This parameter must be provided if the tokenSource is set to KMS. If this parameter is provided, token string should be passed in encrypted. Encrypt parameters using the KMS API encrypt endpoint. The Key should be in the format projects/{gcp_project}/locations/{key_region}/keyRings/{key_ring}/cryptoKeys/{kms_key_name}. See: https://cloud.google.com/kms/docs/reference/rest/v1/projects.locations.keyRings.cryptoKeys/encrypt  (Example: projects/your-project-id/locations/global/keyRings/your-keyring/cryptoKeys/your-key-name).
* **tokenSecretId** (Google Cloud Secret Manager ID.): Secret Manager secret ID for the token. This parameter should be provided if the tokenSource is set to SECRET_MANAGER. Should be in the format projects/{project}/secrets/{secret}/versions/{secret_version}. (Example: projects/your-project-id/secrets/your-secret/versions/your-secret-version).
* **tokenSource** (Source of the token passed. One of PLAINTEXT, KMS or SECRET_MANAGER.): Source of the token. One of PLAINTEXT, KMS or SECRET_MANAGER. This parameter must be provided if secret manager is used. If tokenSource is set to KMS, tokenKMSEncryptionKey and encrypted token must be provided. If tokenSource is set to SECRET_MANAGER, tokenSecretId must be provided. If tokenSource is set to PLAINTEXT, token must be provided.
* **rootCaCertificatePath** (Cloud Storage path to root CA certificate.): The full URL to root CA certificate in Cloud Storage. The certificate provided in Cloud Storage must be DER-encoded and may be supplied in binary or printable (Base64) encoding. If the certificate is provided in Base64 encoding, it must be bounded at the beginning by -----BEGIN CERTIFICATE-----, and must be bounded at the end by -----END CERTIFICATE-----. If this parameter is provided, this private CA certificate file will be fetched and added to Dataflow worker's trust store in order to verify Splunk HEC endpoint's SSL certificate which is signed by that private CA. If this parameter is not provided, the default trust store is used. (Example: gs://mybucket/mycerts/privateCA.crt).
* **enableBatchLogs** (Enable logs for batches written to Splunk.): Parameter which specifies if logs should be enabled for batches written to Splunk. Defaults to: true.
* **enableGzipHttpCompression** (Enable compression (gzip content encoding) in HTTP requests sent to Splunk HEC.): Parameter which specifies if HTTP requests sent to Splunk HEC should be GZIP encoded. Defaults to: true.
* **javascriptTextTransformGcsPath** (JavaScript UDF path in Cloud Storage): The Cloud Storage path pattern for the JavaScript code containing your user-defined functions.
* **javascriptTextTransformFunctionName** (JavaScript UDF name): The name of the function to call from your JavaScript file. Use only letters, digits, and underscores. (Example: transform_udf1).


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
[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=/v1/src/main/java/com/google/cloud/teleport/templates/PubSubToSplunk.java)

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates#templates-plugin)
. Install the plugin with the following command before proceeding:

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
-DtemplateName="Cloud_PubSub_to_Splunk" \
-pl v1 \
-am
```

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

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="cloud-pubsub-to-splunk-job" \
-DtemplateName="Cloud_PubSub_to_Splunk" \
-Dparameters="inputSubscription=$INPUT_SUBSCRIPTION,token=$TOKEN,url=$URL,batchCount=$BATCH_COUNT,disableCertificateValidation=$DISABLE_CERTIFICATE_VALIDATION,parallelism=$PARALLELISM,includePubsubMessage=$INCLUDE_PUBSUB_MESSAGE,tokenKMSEncryptionKey=$TOKEN_KMSENCRYPTION_KEY,tokenSecretId=$TOKEN_SECRET_ID,tokenSource=$TOKEN_SOURCE,rootCaCertificatePath=$ROOT_CA_CERTIFICATE_PATH,enableBatchLogs=$ENABLE_BATCH_LOGS,enableGzipHttpCompression=$ENABLE_GZIP_HTTP_COMPRESSION,javascriptTextTransformGcsPath=$JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH,javascriptTextTransformFunctionName=$JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME,outputDeadletterTopic=$OUTPUT_DEADLETTER_TOPIC" \
-pl v1 \
-am
```
