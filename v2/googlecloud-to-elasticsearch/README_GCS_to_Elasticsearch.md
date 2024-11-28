
Cloud Storage to Elasticsearch template
---
The Cloud Storage to Elasticsearch template is a batch pipeline that reads data
from CSV files stored in a Cloud Storage bucket and writes the data into
Elasticsearch as JSON documents.

If the CSV files contain headers, set the <code>containsHeaders</code> template
parameter to <code>true</code>.
Otherwise, create a JSON schema file that describes the data. Specify the Cloud
Storage URI of the schema file in the jsonSchemaPath template parameter. The
following example shows a JSON schema:
<code>[{"name":"id", "type":"text"}, {"name":"age", "type":"integer"}]</code>
Alternatively, you can provide a Javascript user-defined function (UDF) that
parses the CSV text and outputs Elasticsearch documents.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/cloud-storage-to-elasticsearch)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=GCS_to_Elasticsearch).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **deadletterTable**: The BigQuery dead-letter table to send failed inserts to. For example, `your-project:your-dataset.your-table-name`.
* **inputFileSpec**: The Cloud Storage file pattern to search for CSV files. For example, `gs://mybucket/test-*.csv`.
* **connectionUrl**: The Elasticsearch URL in the format `https://hostname:[port]`. If using Elastic Cloud, specify the CloudID. For example, `https://elasticsearch-host:9200`.
* **apiKey**: The Base64-encoded API key to use for authentication.
* **index**: The Elasticsearch index that the requests are issued to. For example, `my-index`.

### Optional parameters

* **inputFormat**: The input file format. Defaults to `CSV`.
* **containsHeaders**: Input CSV files contain a header record (true/false). Only required if reading CSV files. Defaults to: false.
* **delimiter**: The column delimiter of the input text files. Default: `,` For example, `,`.
* **csvFormat**: CSV format specification to use for parsing records. Default is: `Default`. See https://commons.apache.org/proper/commons-csv/apidocs/org/apache/commons/csv/CSVFormat.html for more details. Must match format names exactly found at: https://commons.apache.org/proper/commons-csv/apidocs/org/apache/commons/csv/CSVFormat.Predefined.html.
* **jsonSchemaPath**: The path to the JSON schema. Defaults to `null`. For example, `gs://path/to/schema`.
* **largeNumFiles**: Set to true if number of files is in the tens of thousands. Defaults to `false`.
* **csvFileEncoding**: The CSV file character encoding format. Allowed values are `US-ASCII`, `ISO-8859-1`, `UTF-8`, and `UTF-16`. Defaults to: UTF-8.
* **logDetailedCsvConversionErrors**: Set to `true` to enable detailed error logging when CSV parsing fails. Note that this may expose sensitive data in the logs (e.g., if the CSV file contains passwords). Default: `false`.
* **elasticsearchUsername**: The Elasticsearch username to authenticate with. If specified, the value of `apiKey` is ignored.
* **elasticsearchPassword**: The Elasticsearch password to authenticate with. If specified, the value of `apiKey` is ignored.
* **batchSize**: The batch size in number of documents. Defaults to `1000`.
* **batchSizeBytes**: The batch size in number of bytes. Defaults to `5242880` (5mb).
* **maxRetryAttempts**: The maximum number of retry attempts. Must be greater than zero. Defaults to `no retries`.
* **maxRetryDuration**: The maximum retry duration in milliseconds. Must be greater than zero. Defaults to `no retries`.
* **propertyAsIndex**: The property in the document being indexed whose value specifies `_index` metadata to include with the document in bulk requests. Takes precedence over an `_index` UDF. Defaults to `none`.
* **javaScriptIndexFnGcsPath**: The Cloud Storage path to the JavaScript UDF source for a function that specifies `_index` metadata to include with the document in bulk requests. Defaults to `none`.
* **javaScriptIndexFnName**: The name of the UDF JavaScript function that specifies `_index` metadata to include with the document in bulk requests. Defaults to `none`.
* **propertyAsId**: A property in the document being indexed whose value specifies `_id` metadata to include with the document in bulk requests. Takes precedence over an `_id` UDF. Defaults to `none`.
* **javaScriptIdFnGcsPath**: The Cloud Storage path to the JavaScript UDF source for the function that specifies `_id` metadata to include with the document in bulk requests. Defaults to `none`.
* **javaScriptIdFnName**: The name of the UDF JavaScript function that specifies the `_id` metadata to include with the document in bulk requests. Defaults to `none`.
* **javaScriptTypeFnGcsPath**: The Cloud Storage path to the JavaScript UDF source for a function that specifies `_type` metadata to include with documents in bulk requests. Defaults to `none`.
* **javaScriptTypeFnName**: The name of the UDF JavaScript function that specifies the `_type` metadata to include with the document in bulk requests. Defaults to `none`.
* **javaScriptIsDeleteFnGcsPath**: The Cloud Storage path to the JavaScript UDF source for the function that determines whether to delete the document instead of inserting or updating it. The function returns a string value of `true` or `false`. Defaults to `none`.
* **javaScriptIsDeleteFnName**: The name of the UDF JavaScript function that determines whether to delete the document instead of inserting or updating it. The function returns a string value of `true` or `false`. Defaults to `none`.
* **usePartialUpdate**: Whether to use partial updates (update rather than create or index, allowing partial documents) with Elasticsearch requests. Defaults to `false`.
* **bulkInsertMethod**: Whether to use `INDEX` (index, allows upserts) or `CREATE` (create, errors on duplicate _id) with Elasticsearch bulk requests. Defaults to `CREATE`.
* **trustSelfSignedCerts**: Whether to trust self-signed certificate or not. An Elasticsearch instance installed might have a self-signed certificate, Enable this to true to by-pass the validation on SSL certificate. (Defaults to: `false`).
* **disableCertificateValidation**: If `true`, trust the self-signed SSL certificate. An Elasticsearch instance might have a self-signed certificate. To bypass validation for the certificate, set this parameter to `true`. Defaults to `false`.
* **apiKeyKMSEncryptionKey**: The Cloud KMS key to decrypt the API key. This parameter is required if the `apiKeySource` is set to `KMS`. If this parameter is provided, pass in an encrypted `apiKey` string. Encrypt parameters using the KMS API encrypt endpoint. For the key, use the format `projects/<PROJECT_ID>/locations/<KEY_REGION>/keyRings/<KEY_RING>/cryptoKeys/<KMS_KEY_NAME>`. See: https://cloud.google.com/kms/docs/reference/rest/v1/projects.locations.keyRings.cryptoKeys/encrypt  For example, `projects/your-project-id/locations/global/keyRings/your-keyring/cryptoKeys/your-key-name`.
* **apiKeySecretId**: The Secret Manager secret ID for the apiKey. If the `apiKeySource` is set to `SECRET_MANAGER`, provide this parameter. Use the format `projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>. For example, `projects/your-project-id/secrets/your-secret/versions/your-secret-version`.
* **apiKeySource**: The source of the API key. Allowed values are `PLAINTEXT`, `KMS` orand `SECRET_MANAGER`. This parameter is required when you use Secret Manager or KMS. If `apiKeySource` is set to `KMS`, `apiKeyKMSEncryptionKey` and encrypted apiKey must be provided. If `apiKeySource` is set to `SECRET_MANAGER`, `apiKeySecretId` must be provided. If `apiKeySource` is set to `PLAINTEXT`, `apiKey` must be provided. Defaults to: PLAINTEXT.
* **socketTimeout**: If set, overwrites the default max retry timeout and default socket timeout (30000ms) in the Elastic RestClient.
* **javascriptTextTransformGcsPath**: The Cloud Storage URI of the .js file that defines the JavaScript user-defined function (UDF) to use. For example, `gs://my-bucket/my-udfs/my_file.js`.
* **javascriptTextTransformFunctionName**: The name of the JavaScript user-defined function (UDF) to use. For example, if your JavaScript function code is `myTransform(inJson) { /*...do stuff...*/ }`, then the function name is `myTransform`. For sample JavaScript UDFs, see UDF Examples (https://github.com/GoogleCloudPlatform/DataflowTemplates#udf-examples).


## User-Defined functions (UDFs)

The Cloud Storage to Elasticsearch Template supports User-Defined functions (UDFs).
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

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/googlecloud-to-elasticsearch/src/main/java/com/google/cloud/teleport/v2/elasticsearch/templates/GCSToElasticsearch.java)

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
-DtemplateName="GCS_to_Elasticsearch" \
-f v2/googlecloud-to-elasticsearch
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/GCS_to_Elasticsearch
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/GCS_to_Elasticsearch"

### Required
export DEADLETTER_TABLE=<deadletterTable>
export INPUT_FILE_SPEC=<inputFileSpec>
export CONNECTION_URL=<connectionUrl>
export API_KEY=<apiKey>
export INDEX=<index>

### Optional
export INPUT_FORMAT=csv
export CONTAINS_HEADERS=false
export DELIMITER=<delimiter>
export CSV_FORMAT=Default
export JSON_SCHEMA_PATH=<jsonSchemaPath>
export LARGE_NUM_FILES=false
export CSV_FILE_ENCODING=UTF-8
export LOG_DETAILED_CSV_CONVERSION_ERRORS=false
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
export SOCKET_TIMEOUT=<socketTimeout>
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>

gcloud dataflow flex-template run "gcs-to-elasticsearch-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "deadletterTable=$DEADLETTER_TABLE" \
  --parameters "inputFormat=$INPUT_FORMAT" \
  --parameters "inputFileSpec=$INPUT_FILE_SPEC" \
  --parameters "containsHeaders=$CONTAINS_HEADERS" \
  --parameters "delimiter=$DELIMITER" \
  --parameters "csvFormat=$CSV_FORMAT" \
  --parameters "jsonSchemaPath=$JSON_SCHEMA_PATH" \
  --parameters "largeNumFiles=$LARGE_NUM_FILES" \
  --parameters "csvFileEncoding=$CSV_FILE_ENCODING" \
  --parameters "logDetailedCsvConversionErrors=$LOG_DETAILED_CSV_CONVERSION_ERRORS" \
  --parameters "connectionUrl=$CONNECTION_URL" \
  --parameters "apiKey=$API_KEY" \
  --parameters "elasticsearchUsername=$ELASTICSEARCH_USERNAME" \
  --parameters "elasticsearchPassword=$ELASTICSEARCH_PASSWORD" \
  --parameters "index=$INDEX" \
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
  --parameters "apiKeySource=$API_KEY_SOURCE" \
  --parameters "socketTimeout=$SOCKET_TIMEOUT" \
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

### Required
export DEADLETTER_TABLE=<deadletterTable>
export INPUT_FILE_SPEC=<inputFileSpec>
export CONNECTION_URL=<connectionUrl>
export API_KEY=<apiKey>
export INDEX=<index>

### Optional
export INPUT_FORMAT=csv
export CONTAINS_HEADERS=false
export DELIMITER=<delimiter>
export CSV_FORMAT=Default
export JSON_SCHEMA_PATH=<jsonSchemaPath>
export LARGE_NUM_FILES=false
export CSV_FILE_ENCODING=UTF-8
export LOG_DETAILED_CSV_CONVERSION_ERRORS=false
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
export SOCKET_TIMEOUT=<socketTimeout>
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="gcs-to-elasticsearch-job" \
-DtemplateName="GCS_to_Elasticsearch" \
-Dparameters="deadletterTable=$DEADLETTER_TABLE,inputFormat=$INPUT_FORMAT,inputFileSpec=$INPUT_FILE_SPEC,containsHeaders=$CONTAINS_HEADERS,delimiter=$DELIMITER,csvFormat=$CSV_FORMAT,jsonSchemaPath=$JSON_SCHEMA_PATH,largeNumFiles=$LARGE_NUM_FILES,csvFileEncoding=$CSV_FILE_ENCODING,logDetailedCsvConversionErrors=$LOG_DETAILED_CSV_CONVERSION_ERRORS,connectionUrl=$CONNECTION_URL,apiKey=$API_KEY,elasticsearchUsername=$ELASTICSEARCH_USERNAME,elasticsearchPassword=$ELASTICSEARCH_PASSWORD,index=$INDEX,batchSize=$BATCH_SIZE,batchSizeBytes=$BATCH_SIZE_BYTES,maxRetryAttempts=$MAX_RETRY_ATTEMPTS,maxRetryDuration=$MAX_RETRY_DURATION,propertyAsIndex=$PROPERTY_AS_INDEX,javaScriptIndexFnGcsPath=$JAVA_SCRIPT_INDEX_FN_GCS_PATH,javaScriptIndexFnName=$JAVA_SCRIPT_INDEX_FN_NAME,propertyAsId=$PROPERTY_AS_ID,javaScriptIdFnGcsPath=$JAVA_SCRIPT_ID_FN_GCS_PATH,javaScriptIdFnName=$JAVA_SCRIPT_ID_FN_NAME,javaScriptTypeFnGcsPath=$JAVA_SCRIPT_TYPE_FN_GCS_PATH,javaScriptTypeFnName=$JAVA_SCRIPT_TYPE_FN_NAME,javaScriptIsDeleteFnGcsPath=$JAVA_SCRIPT_IS_DELETE_FN_GCS_PATH,javaScriptIsDeleteFnName=$JAVA_SCRIPT_IS_DELETE_FN_NAME,usePartialUpdate=$USE_PARTIAL_UPDATE,bulkInsertMethod=$BULK_INSERT_METHOD,trustSelfSignedCerts=$TRUST_SELF_SIGNED_CERTS,disableCertificateValidation=$DISABLE_CERTIFICATE_VALIDATION,apiKeyKMSEncryptionKey=$API_KEY_KMSENCRYPTION_KEY,apiKeySecretId=$API_KEY_SECRET_ID,apiKeySource=$API_KEY_SOURCE,socketTimeout=$SOCKET_TIMEOUT,javascriptTextTransformGcsPath=$JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH,javascriptTextTransformFunctionName=$JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME" \
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
cd v2/googlecloud-to-elasticsearch/terraform/GCS_to_Elasticsearch
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

resource "google_dataflow_flex_template_job" "gcs_to_elasticsearch" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/GCS_to_Elasticsearch"
  name              = "gcs-to-elasticsearch"
  region            = var.region
  parameters        = {
    deadletterTable = "<deadletterTable>"
    inputFileSpec = "<inputFileSpec>"
    connectionUrl = "<connectionUrl>"
    apiKey = "<apiKey>"
    index = "<index>"
    # inputFormat = "csv"
    # containsHeaders = "false"
    # delimiter = "<delimiter>"
    # csvFormat = "Default"
    # jsonSchemaPath = "<jsonSchemaPath>"
    # largeNumFiles = "false"
    # csvFileEncoding = "UTF-8"
    # logDetailedCsvConversionErrors = "false"
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
    # apiKeyKMSEncryptionKey = "<apiKeyKMSEncryptionKey>"
    # apiKeySecretId = "<apiKeySecretId>"
    # apiKeySource = "PLAINTEXT"
    # socketTimeout = "<socketTimeout>"
    # javascriptTextTransformGcsPath = "<javascriptTextTransformGcsPath>"
    # javascriptTextTransformFunctionName = "<javascriptTextTransformFunctionName>"
  }
}
```
