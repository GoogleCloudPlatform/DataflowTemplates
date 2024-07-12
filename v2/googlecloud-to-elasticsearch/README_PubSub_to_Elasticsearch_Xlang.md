
Pub/Sub to Elasticsearch With Python UDFs template
---
The Pub/Sub to Elasticsearch template is a streaming pipeline that reads messages
from a Pub/Sub subscription, executes a Python user-defined function (UDF), and
writes them to Elasticsearch as documents. The Dataflow template uses
Elasticsearch's <a
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
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=PubSub_to_Elasticsearch_Xlang).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **inputSubscription** : Pub/Sub subscription to consume the input from. (Example: projects/<PROJECT_ID>/subscriptions/<SUBSCRIPTION_NAME>).
* **errorOutputTopic** : The Pub/Sub output topic for publishing failed records, in the format of `projects/<PROJECT_ID>/topics/<TOPIC_NAME>`.
* **connectionUrl** : The Elasticsearch URL in the format `https://hostname:[port]`. If using Elastic Cloud, specify the CloudID. (Example: https://elasticsearch-host:9200).
* **apiKey** : The Base64-encoded API key to use for authentication.

### Optional parameters

* **dataset** : The type of logs sent using Pub/Sub, for which we have an out-of-the-box dashboard. Known log types values are `audit`, `vpcflow`, and `firewall`. Defaults to: `pubsub`.
* **namespace** : An arbitrary grouping, such as an environment (dev, prod, or qa), a team, or a strategic business unit. Defaults to: `default`.
* **elasticsearchTemplateVersion** : Dataflow Template Version Identifier, usually defined by Google Cloud. Defaults to: 1.0.0.
* **pythonExternalTextTransformGcsPath** : The Cloud Storage path pattern for the Python code containing your user-defined functions. (Example: gs://your-bucket/your-function.py).
* **pythonExternalTextTransformFunctionName** : The name of the function to call from your Python file. Use only letters, digits, and underscores. (Example: 'transform' or 'transform_udf1').
* **elasticsearchUsername** : The Elasticsearch username to authenticate with. If specified, the value of `apiKey` is ignored.
* **elasticsearchPassword** : The Elasticsearch password to authenticate with. If specified, the value of `apiKey` is ignored.
* **batchSize** : The batch size in number of documents. Defaults to: `1000`.
* **batchSizeBytes** : The batch size in number of bytes. Defaults to: `5242880` (5mb).
* **maxRetryAttempts** : The maximum number of retry attempts. Must be greater than zero. Defaults to: `no retries`.
* **maxRetryDuration** : The maximum retry duration in milliseconds. Must be greater than zero. Defaults to: `no retries`.
* **propertyAsIndex** : The property in the document being indexed whose value specifies `_index` metadata to include with the document in bulk requests. Takes precedence over an `_index` UDF. Defaults to: `none`.
* **javaScriptIndexFnGcsPath** : The Cloud Storage path to the JavaScript UDF source for a function that specifies `_index` metadata to include with the document in bulk requests. Defaults to: `none`.
* **javaScriptIndexFnName** : The name of the UDF JavaScript function that specifies `_index` metadata to include with the document in bulk requests. Defaults to: `none`.
* **propertyAsId** : A property in the document being indexed whose value specifies `_id` metadata to include with the document in bulk requests. Takes precedence over an `_id` UDF. Defaults to: `none`.
* **javaScriptIdFnGcsPath** : The Cloud Storage path to the JavaScript UDF source for the function that specifies `_id` metadata to include with the document in bulk requests. Defaults to: `none`.
* **javaScriptIdFnName** : The name of the UDF JavaScript function that specifies the `_id` metadata to include with the document in bulk requests. Defaults to: `none`.
* **javaScriptTypeFnGcsPath** : The Cloud Storage path to the JavaScript UDF source for a function that specifies `_type` metadata to include with documents in bulk requests. Default: `none`.
* **javaScriptTypeFnName** : The name of the UDF JavaScript function that specifies the `_type` metadata to include with the document in bulk requests. Defaults to: `none`.
* **javaScriptIsDeleteFnGcsPath** : The Cloud Storage path to the JavaScript UDF source for the function that determines whether to delete the document instead of inserting or updating it. The function returns a string value of `true` or `false`. Defaults to: `none`.
* **javaScriptIsDeleteFnName** : The name of the UDF JavaScript function that determines whether to delete the document instead of inserting or updating it. The function returns a string value of `true` or `false`. Defaults to: `none`.
* **usePartialUpdate** : Whether to use partial updates (update rather than create or index, allowing partial documents) with Elasticsearch requests. Defaults to: `false`.
* **bulkInsertMethod** : Whether to use `INDEX` (index, allows upserts) or `CREATE` (create, errors on duplicate _id) with Elasticsearch bulk requests. Defaults to: `CREATE`.
* **trustSelfSignedCerts** : Whether to trust self-signed certificate or not. An Elasticsearch instance installed might have a self-signed certificate, Enable this to true to by-pass the validation on SSL certificate. (Defaults to: `false`).
* **disableCertificateValidation** : If `true`, trust the self-signed SSL certificate. An Elasticsearch instance might have a self-signed certificate. To bypass validation for the certificate, set this parameter to `true`. Default: `false`.
* **apiKeyKMSEncryptionKey** : The Cloud KMS key to decrypt the API key. This parameter must be provided if the apiKeySource is set to KMS. If this parameter is provided, apiKey string should be passed in encrypted. Encrypt parameters using the KMS API encrypt endpoint. For the key, use the format `projects/<PROJECT_ID>/locations/<KEY_REGION>/keyRings/<KEY_RING>/cryptoKeys/<KMS_KEY_NAME>`. See: https://cloud.google.com/kms/docs/reference/rest/v1/projects.locations.keyRings.cryptoKeys/encrypt  (Example: projects/your-project-id/locations/global/keyRings/your-keyring/cryptoKeys/your-key-name).
* **apiKeySecretId** : Secret Manager secret ID for the apiKey. If the `apiKeySource` is set to `SECRET_MANAGER`, provide this parameter. Use the format `projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>. (Example: projects/your-project-id/secrets/your-secret/versions/your-secret-version).
* **apiKeySource** : Source of the API key. One of `PLAINTEXT`, `KMS` or `SECRET_MANAGER`. This parameter must be provided if secret manager or KMS is used. If `apiKeySource` is set to `KMS`, `apiKeyKMSEncryptionKey` and encrypted apiKey must be provided. If `apiKeySource` is set to `SECRET_MANAGER`, `apiKeySecretId` must be provided. If `apiKeySource` is set to `PLAINTEXT`, apiKey must be provided. Defaults to: PLAINTEXT.



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
-DtemplateName="PubSub_to_Elasticsearch_Xlang" \
-f v2/googlecloud-to-elasticsearch
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/PubSub_to_Elasticsearch_Xlang
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/PubSub_to_Elasticsearch_Xlang"

### Required
export INPUT_SUBSCRIPTION=<inputSubscription>
export ERROR_OUTPUT_TOPIC=<errorOutputTopic>
export CONNECTION_URL=<connectionUrl>
export API_KEY=<apiKey>

### Optional
export DATASET=PUBSUB
export NAMESPACE=default
export ELASTICSEARCH_TEMPLATE_VERSION=1.0.0
export PYTHON_EXTERNAL_TEXT_TRANSFORM_GCS_PATH=<pythonExternalTextTransformGcsPath>
export PYTHON_EXTERNAL_TEXT_TRANSFORM_FUNCTION_NAME=<pythonExternalTextTransformFunctionName>
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

gcloud dataflow flex-template run "pubsub-to-elasticsearch-xlang-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "inputSubscription=$INPUT_SUBSCRIPTION" \
  --parameters "dataset=$DATASET" \
  --parameters "namespace=$NAMESPACE" \
  --parameters "errorOutputTopic=$ERROR_OUTPUT_TOPIC" \
  --parameters "elasticsearchTemplateVersion=$ELASTICSEARCH_TEMPLATE_VERSION" \
  --parameters "pythonExternalTextTransformGcsPath=$PYTHON_EXTERNAL_TEXT_TRANSFORM_GCS_PATH" \
  --parameters "pythonExternalTextTransformFunctionName=$PYTHON_EXTERNAL_TEXT_TRANSFORM_FUNCTION_NAME" \
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
export PYTHON_EXTERNAL_TEXT_TRANSFORM_GCS_PATH=<pythonExternalTextTransformGcsPath>
export PYTHON_EXTERNAL_TEXT_TRANSFORM_FUNCTION_NAME=<pythonExternalTextTransformFunctionName>
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
-DjobName="pubsub-to-elasticsearch-xlang-job" \
-DtemplateName="PubSub_to_Elasticsearch_Xlang" \
-Dparameters="inputSubscription=$INPUT_SUBSCRIPTION,dataset=$DATASET,namespace=$NAMESPACE,errorOutputTopic=$ERROR_OUTPUT_TOPIC,elasticsearchTemplateVersion=$ELASTICSEARCH_TEMPLATE_VERSION,pythonExternalTextTransformGcsPath=$PYTHON_EXTERNAL_TEXT_TRANSFORM_GCS_PATH,pythonExternalTextTransformFunctionName=$PYTHON_EXTERNAL_TEXT_TRANSFORM_FUNCTION_NAME,connectionUrl=$CONNECTION_URL,apiKey=$API_KEY,elasticsearchUsername=$ELASTICSEARCH_USERNAME,elasticsearchPassword=$ELASTICSEARCH_PASSWORD,batchSize=$BATCH_SIZE,batchSizeBytes=$BATCH_SIZE_BYTES,maxRetryAttempts=$MAX_RETRY_ATTEMPTS,maxRetryDuration=$MAX_RETRY_DURATION,propertyAsIndex=$PROPERTY_AS_INDEX,javaScriptIndexFnGcsPath=$JAVA_SCRIPT_INDEX_FN_GCS_PATH,javaScriptIndexFnName=$JAVA_SCRIPT_INDEX_FN_NAME,propertyAsId=$PROPERTY_AS_ID,javaScriptIdFnGcsPath=$JAVA_SCRIPT_ID_FN_GCS_PATH,javaScriptIdFnName=$JAVA_SCRIPT_ID_FN_NAME,javaScriptTypeFnGcsPath=$JAVA_SCRIPT_TYPE_FN_GCS_PATH,javaScriptTypeFnName=$JAVA_SCRIPT_TYPE_FN_NAME,javaScriptIsDeleteFnGcsPath=$JAVA_SCRIPT_IS_DELETE_FN_GCS_PATH,javaScriptIsDeleteFnName=$JAVA_SCRIPT_IS_DELETE_FN_NAME,usePartialUpdate=$USE_PARTIAL_UPDATE,bulkInsertMethod=$BULK_INSERT_METHOD,trustSelfSignedCerts=$TRUST_SELF_SIGNED_CERTS,disableCertificateValidation=$DISABLE_CERTIFICATE_VALIDATION,apiKeyKMSEncryptionKey=$API_KEY_KMSENCRYPTION_KEY,apiKeySecretId=$API_KEY_SECRET_ID,apiKeySource=$API_KEY_SOURCE" \
-f v2/googlecloud-to-elasticsearch
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
cd v2/googlecloud-to-elasticsearch/terraform/PubSub_to_Elasticsearch_Xlang
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

resource "google_dataflow_flex_template_job" "pubsub_to_elasticsearch_xlang" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/PubSub_to_Elasticsearch_Xlang"
  name              = "pubsub-to-elasticsearch-xlang"
  region            = var.region
  parameters        = {
    inputSubscription = "projects/<PROJECT_ID>/subscriptions/<SUBSCRIPTION_NAME>"
    errorOutputTopic = "<errorOutputTopic>"
    connectionUrl = "https://elasticsearch-host:9200"
    apiKey = "<apiKey>"
    # dataset = "PUBSUB"
    # namespace = "default"
    # elasticsearchTemplateVersion = "1.0.0"
    # pythonExternalTextTransformGcsPath = "gs://your-bucket/your-function.py"
    # pythonExternalTextTransformFunctionName = "'transform' or 'transform_udf1'"
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
