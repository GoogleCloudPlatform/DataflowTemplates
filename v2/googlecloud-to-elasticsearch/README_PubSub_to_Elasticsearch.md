
Pub/Sub to Elasticsearch template
---
The Pub/Sub to Elasticsearch template is a streaming pipeline that reads messages
from a Pub/Sub subscription, executes a user-defined function (UDF), and writes
them to Elasticsearch as documents. The Dataflow template uses Elasticsearch's <a
href="https://www.elastic.co/guide/en/elasticsearch/reference/master/data-streams.html">data
streams</a> feature to store time series data across multiple indices while
giving you a single named resource for requests. Data streams are well-suited for
logs, metrics, traces, and other continuously generated data stored in Pub/Sub.

The template creates a datastream named <code>logs-gcp.DATASET-NAMESPACE</code>,
where:
- <code>DATASET</code> is the value of the <code>dataset</code> template
parameter, or <code>pubsub</code> if not specified.
- <code>NAMESPACE</code> is the value of the <code>namespace</code> template
parameter, or <code>default</code> if not specified.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/pubsub-to-elasticsearch)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=PubSub_to_Elasticsearch).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required Parameters

* **inputSubscription** (Pub/Sub input subscription): Pub/Sub subscription to read the input from, in the format of 'projects/your-project-id/subscriptions/your-subscription-name' (Example: projects/your-project-id/subscriptions/your-subscription-name).
* **errorOutputTopic** (Output deadletter Pub/Sub topic): The Pub/Sub topic to publish deadletter records to. The name should be in the format of `projects/your-project-id/topics/your-topic-name`.
* **connectionUrl** (Elasticsearch URL or CloudID if using Elastic Cloud): Elasticsearch URL in the format https://hostname:[port] or specify CloudID if using Elastic Cloud (Example: https://elasticsearch-host:9200).
* **apiKey** (Base64 Encoded API Key for access without requiring basic authentication): Base64 Encoded API Key for access without requiring basic authentication. Refer to: https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-create-api-key.html#security-api-create-api-key-request.

### Optional Parameters

* **dataset** (Dataset, the type of logs that are sent to Pub/Sub): The type of logs sent via Pub/Sub for which we have out of the box dashboard. Known log types values are audit, vpcflow, and firewall. If no known log type is detected, we default to 'pubsub'.
* **namespace** (The namespace for dataset.): The namespace for dataset. Default is default.
* **elasticsearchTemplateVersion** (Template Version.): Dataflow Template Version Identifier, usually defined by Google Cloud. Defaults to: 1.0.0.
* **javascriptTextTransformGcsPath** (Cloud Storage path to Javascript UDF source): The Cloud Storage path pattern for the JavaScript code containing your user-defined functions. (Example: gs://your-bucket/your-function.js).
* **javascriptTextTransformFunctionName** (UDF Javascript Function Name): The name of the function to call from your JavaScript file. Use only letters, digits, and underscores. (Example: 'transform' or 'transform_udf1').
* **javascriptTextTransformReloadIntervalMinutes** (JavaScript UDF auto-reload interval (minutes)): Define the interval that workers may check for JavaScript UDF changes to reload the files. Defaults to: 0.
* **elasticsearchUsername** (Username for Elasticsearch endpoint): Username for Elasticsearch endpoint. Overrides ApiKey option if specified.
* **elasticsearchPassword** (Password for Elasticsearch endpoint): Password for Elasticsearch endpoint. Overrides ApiKey option if specified.
* **batchSize** (Batch Size): Batch Size used for batch insertion of messages into Elasticsearch. Defaults to: 1000.
* **batchSizeBytes** (Batch Size in Bytes): Batch Size in bytes used for batch insertion of messages into elasticsearch. Default: 5242880 (5mb).
* **maxRetryAttempts** (Max retry attempts.): Max retry attempts, must be > 0. Default: no retries.
* **maxRetryDuration** (Max retry duration.): Max retry duration in milliseconds, must be > 0. Default: no retries.
* **propertyAsIndex** (Document property to specify _index metadata): A property in the document being indexed whose value will specify _index metadata to be included with document in bulk request (takes precedence over an _index UDF).
* **javaScriptIndexFnGcsPath** (Cloud Storage path to JavaScript UDF source for _index metadata): Cloud Storage path to JavaScript UDF source for function that will specify _index metadata to be included with document in bulk request.
* **javaScriptIndexFnName** (UDF JavaScript Function Name for _index metadata): UDF JavaScript Function Name for function that will specify _index metadata to be included with document in bulk request.
* **propertyAsId** (Document property to specify _id metadata): A property in the document being indexed whose value will specify _id metadata to be included with document in bulk request (takes precedence over an _id UDF).
* **javaScriptIdFnGcsPath** (Cloud Storage path to JavaScript UDF source for _id metadata): Cloud Storage path to JavaScript UDF source for function that will specify _id metadata to be included with document in bulk request.
* **javaScriptIdFnName** (UDF JavaScript Function Name for _id metadata): UDF JavaScript Function Name for function that will specify _id metadata to be included with document in bulk request.
* **javaScriptTypeFnGcsPath** (Cloud Storage path to JavaScript UDF source for _type metadata): Cloud Storage path to JavaScript UDF source for function that will specify _type metadata to be included with document in bulk request.
* **javaScriptTypeFnName** (UDF JavaScript Function Name for _type metadata): UDF JavaScript Function Name for function that will specify _type metadata to be included with document in bulk request.
* **javaScriptIsDeleteFnGcsPath** (Cloud Storage path to JavaScript UDF source for isDelete function): Cloud Storage path to JavaScript UDF source for function that will determine if document should be deleted rather than inserted or updated, function should return string value "true" or "false".
* **javaScriptIsDeleteFnName** (UDF JavaScript Function Name for isDelete): UDF JavaScript Function Name for function that will determine if document should be deleted rather than inserted or updated, function should return string value "true" or "false".
* **usePartialUpdate** (Use partial updates): Whether to use partial updates (update rather than create or index, allowing partial docs) with Elasticsearch requests. Defaults to: false.
* **bulkInsertMethod** (Build insert method): Whether to use INDEX (index, allows upsert) or CREATE (create, errors on duplicate _id) with Elasticsearch bulk requests. Defaults to: CREATE.
* **trustSelfSignedCerts** (Trust self-signed certificate): Whether to trust self-signed certificate or not. An Elasticsearch instance installed might have a self-signed certificate, Enable this to True to by-pass the validation on SSL certificate. (default is False).
* **disableCertificateValidation** (Disable SSL certificate validation.): Disable SSL certificate validation (true/false). Default false (validation enabled). If true, all certificates are considered trusted.
* **apiKeyKMSEncryptionKey** (Google Cloud KMS encryption key for the API key): The Cloud KMS key to decrypt the API key. This parameter must be provided if the apiKeySource is set to KMS. If this parameter is provided, apiKey string should be passed in encrypted. Encrypt parameters using the KMS API encrypt endpoint. The Key should be in the format projects/{gcp_project}/locations/{key_region}/keyRings/{key_ring}/cryptoKeys/{kms_key_name}. See: https://cloud.google.com/kms/docs/reference/rest/v1/projects.locations.keyRings.cryptoKeys/encrypt  (Example: projects/your-project-id/locations/global/keyRings/your-keyring/cryptoKeys/your-key-name).
* **apiKeySecretId** (Google Cloud Secret Manager ID.): Secret Manager secret ID for the apiKey. This parameter should be provided if the apiKeySource is set to SECRET_MANAGER. Should be in the format projects/{project}/secrets/{secret}/versions/{secret_version}. (Example: projects/your-project-id/secrets/your-secret/versions/your-secret-version).
* **apiKeySource** (Source of the API key passed. One of PLAINTEXT, KMS or SECRET_MANAGER.): Source of the API key. One of PLAINTEXT, KMS or SECRET_MANAGER. This parameter must be provided if secret manager or KMS is used. If apiKeySource is set to KMS, apiKeyKMSEncryptionKey and encrypted apiKey must be provided. If apiKeySource is set to SECRET_MANAGER, apiKeySecretId must be provided. If apiKeySource is set to PLAINTEXT, apiKey must be provided. Defaults to: PLAINTEXT.


## User-Defined functions (UDFs)

The Pub/Sub to Elasticsearch Template supports User-Defined functions (UDFs).
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
[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/googlecloud-to-elasticsearch/src/main/java/com/google/cloud/teleport/v2/elasticsearch/templates/PubSubToElasticsearch.java)

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
-DtemplateName="PubSub_to_Elasticsearch" \
-pl v2/googlecloud-to-elasticsearch \
-am
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/PubSub_to_Elasticsearch
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/PubSub_to_Elasticsearch"

### Required
export INPUT_SUBSCRIPTION=<inputSubscription>
export ERROR_OUTPUT_TOPIC=<errorOutputTopic>
export CONNECTION_URL=<connectionUrl>
export API_KEY=<apiKey>

### Optional
export DATASET=PUBSUB
export NAMESPACE=default
export ELASTICSEARCH_TEMPLATE_VERSION=1.0.0
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>
export JAVASCRIPT_TEXT_TRANSFORM_RELOAD_INTERVAL_MINUTES=0
export ELASTICSEARCH_USERNAME=<elasticsearchUsername>
export ELASTICSEARCH_PASSWORD=<elasticsearchPassword>
export BATCH_SIZE=1000
export BATCH_SIZE_BYTES=5242880
export MAX_RETRY_ATTEMPTS=<maxRetryAttempts>
export MAX_RETRY_DURATION=<maxRetryDuration>
export PROPERTY_AS_INDEX=<propertyAsIndex>
export JAVA_SCRIPT_INDEX_FN_GCS_PATH=<javaScriptIndexFnGcsPath>
export JAVA_SCRIPT_INDEX_FN_NAME=<javaScriptIndexFnName>
export PROPERTY_AS_ID=<propertyAsId>
export JAVA_SCRIPT_ID_FN_GCS_PATH=<javaScriptIdFnGcsPath>
export JAVA_SCRIPT_ID_FN_NAME=<javaScriptIdFnName>
export JAVA_SCRIPT_TYPE_FN_GCS_PATH=<javaScriptTypeFnGcsPath>
export JAVA_SCRIPT_TYPE_FN_NAME=<javaScriptTypeFnName>
export JAVA_SCRIPT_IS_DELETE_FN_GCS_PATH=<javaScriptIsDeleteFnGcsPath>
export JAVA_SCRIPT_IS_DELETE_FN_NAME=<javaScriptIsDeleteFnName>
export USE_PARTIAL_UPDATE=false
export BULK_INSERT_METHOD=CREATE
export TRUST_SELF_SIGNED_CERTS=false
export DISABLE_CERTIFICATE_VALIDATION=false
export API_KEY_KMSENCRYPTION_KEY=<apiKeyKMSEncryptionKey>
export API_KEY_SECRET_ID=<apiKeySecretId>
export API_KEY_SOURCE=PLAINTEXT

gcloud dataflow flex-template run "pubsub-to-elasticsearch-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "inputSubscription=$INPUT_SUBSCRIPTION" \
  --parameters "dataset=$DATASET" \
  --parameters "namespace=$NAMESPACE" \
  --parameters "errorOutputTopic=$ERROR_OUTPUT_TOPIC" \
  --parameters "elasticsearchTemplateVersion=$ELASTICSEARCH_TEMPLATE_VERSION" \
  --parameters "javascriptTextTransformGcsPath=$JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH" \
  --parameters "javascriptTextTransformFunctionName=$JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME" \
  --parameters "javascriptTextTransformReloadIntervalMinutes=$JAVASCRIPT_TEXT_TRANSFORM_RELOAD_INTERVAL_MINUTES" \
  --parameters "connectionUrl=$CONNECTION_URL" \
  --parameters "apiKey=$API_KEY" \
  --parameters "elasticsearchUsername=$ELASTICSEARCH_USERNAME" \
  --parameters "elasticsearchPassword=$ELASTICSEARCH_PASSWORD" \
  --parameters "batchSize=$BATCH_SIZE" \
  --parameters "batchSizeBytes=$BATCH_SIZE_BYTES" \
  --parameters "maxRetryAttempts=$MAX_RETRY_ATTEMPTS" \
  --parameters "maxRetryDuration=$MAX_RETRY_DURATION" \
  --parameters "propertyAsIndex=$PROPERTY_AS_INDEX" \
  --parameters "javaScriptIndexFnGcsPath=$JAVA_SCRIPT_INDEX_FN_GCS_PATH" \
  --parameters "javaScriptIndexFnName=$JAVA_SCRIPT_INDEX_FN_NAME" \
  --parameters "propertyAsId=$PROPERTY_AS_ID" \
  --parameters "javaScriptIdFnGcsPath=$JAVA_SCRIPT_ID_FN_GCS_PATH" \
  --parameters "javaScriptIdFnName=$JAVA_SCRIPT_ID_FN_NAME" \
  --parameters "javaScriptTypeFnGcsPath=$JAVA_SCRIPT_TYPE_FN_GCS_PATH" \
  --parameters "javaScriptTypeFnName=$JAVA_SCRIPT_TYPE_FN_NAME" \
  --parameters "javaScriptIsDeleteFnGcsPath=$JAVA_SCRIPT_IS_DELETE_FN_GCS_PATH" \
  --parameters "javaScriptIsDeleteFnName=$JAVA_SCRIPT_IS_DELETE_FN_NAME" \
  --parameters "usePartialUpdate=$USE_PARTIAL_UPDATE" \
  --parameters "bulkInsertMethod=$BULK_INSERT_METHOD" \
  --parameters "trustSelfSignedCerts=$TRUST_SELF_SIGNED_CERTS" \
  --parameters "disableCertificateValidation=$DISABLE_CERTIFICATE_VALIDATION" \
  --parameters "apiKeyKMSEncryptionKey=$API_KEY_KMSENCRYPTION_KEY" \
  --parameters "apiKeySecretId=$API_KEY_SECRET_ID" \
  --parameters "apiKeySource=$API_KEY_SOURCE"
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
export ERROR_OUTPUT_TOPIC=<errorOutputTopic>
export CONNECTION_URL=<connectionUrl>
export API_KEY=<apiKey>

### Optional
export DATASET=PUBSUB
export NAMESPACE=default
export ELASTICSEARCH_TEMPLATE_VERSION=1.0.0
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>
export JAVASCRIPT_TEXT_TRANSFORM_RELOAD_INTERVAL_MINUTES=0
export ELASTICSEARCH_USERNAME=<elasticsearchUsername>
export ELASTICSEARCH_PASSWORD=<elasticsearchPassword>
export BATCH_SIZE=1000
export BATCH_SIZE_BYTES=5242880
export MAX_RETRY_ATTEMPTS=<maxRetryAttempts>
export MAX_RETRY_DURATION=<maxRetryDuration>
export PROPERTY_AS_INDEX=<propertyAsIndex>
export JAVA_SCRIPT_INDEX_FN_GCS_PATH=<javaScriptIndexFnGcsPath>
export JAVA_SCRIPT_INDEX_FN_NAME=<javaScriptIndexFnName>
export PROPERTY_AS_ID=<propertyAsId>
export JAVA_SCRIPT_ID_FN_GCS_PATH=<javaScriptIdFnGcsPath>
export JAVA_SCRIPT_ID_FN_NAME=<javaScriptIdFnName>
export JAVA_SCRIPT_TYPE_FN_GCS_PATH=<javaScriptTypeFnGcsPath>
export JAVA_SCRIPT_TYPE_FN_NAME=<javaScriptTypeFnName>
export JAVA_SCRIPT_IS_DELETE_FN_GCS_PATH=<javaScriptIsDeleteFnGcsPath>
export JAVA_SCRIPT_IS_DELETE_FN_NAME=<javaScriptIsDeleteFnName>
export USE_PARTIAL_UPDATE=false
export BULK_INSERT_METHOD=CREATE
export TRUST_SELF_SIGNED_CERTS=false
export DISABLE_CERTIFICATE_VALIDATION=false
export API_KEY_KMSENCRYPTION_KEY=<apiKeyKMSEncryptionKey>
export API_KEY_SECRET_ID=<apiKeySecretId>
export API_KEY_SOURCE=PLAINTEXT

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="pubsub-to-elasticsearch-job" \
-DtemplateName="PubSub_to_Elasticsearch" \
-Dparameters="inputSubscription=$INPUT_SUBSCRIPTION,dataset=$DATASET,namespace=$NAMESPACE,errorOutputTopic=$ERROR_OUTPUT_TOPIC,elasticsearchTemplateVersion=$ELASTICSEARCH_TEMPLATE_VERSION,javascriptTextTransformGcsPath=$JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH,javascriptTextTransformFunctionName=$JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME,javascriptTextTransformReloadIntervalMinutes=$JAVASCRIPT_TEXT_TRANSFORM_RELOAD_INTERVAL_MINUTES,connectionUrl=$CONNECTION_URL,apiKey=$API_KEY,elasticsearchUsername=$ELASTICSEARCH_USERNAME,elasticsearchPassword=$ELASTICSEARCH_PASSWORD,batchSize=$BATCH_SIZE,batchSizeBytes=$BATCH_SIZE_BYTES,maxRetryAttempts=$MAX_RETRY_ATTEMPTS,maxRetryDuration=$MAX_RETRY_DURATION,propertyAsIndex=$PROPERTY_AS_INDEX,javaScriptIndexFnGcsPath=$JAVA_SCRIPT_INDEX_FN_GCS_PATH,javaScriptIndexFnName=$JAVA_SCRIPT_INDEX_FN_NAME,propertyAsId=$PROPERTY_AS_ID,javaScriptIdFnGcsPath=$JAVA_SCRIPT_ID_FN_GCS_PATH,javaScriptIdFnName=$JAVA_SCRIPT_ID_FN_NAME,javaScriptTypeFnGcsPath=$JAVA_SCRIPT_TYPE_FN_GCS_PATH,javaScriptTypeFnName=$JAVA_SCRIPT_TYPE_FN_NAME,javaScriptIsDeleteFnGcsPath=$JAVA_SCRIPT_IS_DELETE_FN_GCS_PATH,javaScriptIsDeleteFnName=$JAVA_SCRIPT_IS_DELETE_FN_NAME,usePartialUpdate=$USE_PARTIAL_UPDATE,bulkInsertMethod=$BULK_INSERT_METHOD,trustSelfSignedCerts=$TRUST_SELF_SIGNED_CERTS,disableCertificateValidation=$DISABLE_CERTIFICATE_VALIDATION,apiKeyKMSEncryptionKey=$API_KEY_KMSENCRYPTION_KEY,apiKeySecretId=$API_KEY_SECRET_ID,apiKeySource=$API_KEY_SOURCE" \
-pl v2/googlecloud-to-elasticsearch \
-am
```

## Terraform

Dataflow supports the utilization of Terraform to manage template jobs,
see [dataflow_flex_template_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_flex_template_job).

Here is an example of Terraform configuration:


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

resource "google_dataflow_flex_template_job" "pubsub_to_elasticsearch" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/PubSub_to_Elasticsearch"
  name              = "pubsub-to-elasticsearch"
  region            = var.region
  parameters        = {
    inputSubscription = "projects/your-project-id/subscriptions/your-subscription-name"
    errorOutputTopic = "<errorOutputTopic>"
    connectionUrl = "https://elasticsearch-host:9200"
    apiKey = "<apiKey>"
    # dataset = "PUBSUB"
    # namespace = "default"
    # elasticsearchTemplateVersion = "1.0.0"
    # javascriptTextTransformGcsPath = "gs://your-bucket/your-function.js"
    # javascriptTextTransformFunctionName = "'transform' or 'transform_udf1'"
    # javascriptTextTransformReloadIntervalMinutes = "0"
    # elasticsearchUsername = "<elasticsearchUsername>"
    # elasticsearchPassword = "<elasticsearchPassword>"
    # batchSize = "1000"
    # batchSizeBytes = "5242880"
    # maxRetryAttempts = "<maxRetryAttempts>"
    # maxRetryDuration = "<maxRetryDuration>"
    # propertyAsIndex = "<propertyAsIndex>"
    # javaScriptIndexFnGcsPath = "<javaScriptIndexFnGcsPath>"
    # javaScriptIndexFnName = "<javaScriptIndexFnName>"
    # propertyAsId = "<propertyAsId>"
    # javaScriptIdFnGcsPath = "<javaScriptIdFnGcsPath>"
    # javaScriptIdFnName = "<javaScriptIdFnName>"
    # javaScriptTypeFnGcsPath = "<javaScriptTypeFnGcsPath>"
    # javaScriptTypeFnName = "<javaScriptTypeFnName>"
    # javaScriptIsDeleteFnGcsPath = "<javaScriptIsDeleteFnGcsPath>"
    # javaScriptIsDeleteFnName = "<javaScriptIsDeleteFnName>"
    # usePartialUpdate = "false"
    # bulkInsertMethod = "CREATE"
    # trustSelfSignedCerts = "false"
    # disableCertificateValidation = "false"
    # apiKeyKMSEncryptionKey = "projects/your-project-id/locations/global/keyRings/your-keyring/cryptoKeys/your-key-name"
    # apiKeySecretId = "projects/your-project-id/secrets/your-secret/versions/your-secret-version"
    # apiKeySource = "PLAINTEXT"
  }
}
```
