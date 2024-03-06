
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
Alternatively, you can provide a user-defined function (UDF) that parses the CSV
text and outputs Elasticsearch documents.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/cloud-storage-to-elasticsearch)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=GCS_to_Elasticsearch).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required Parameters

* **deadletterTable** (BigQuery Deadletter table to send failed inserts.): Messages failed to reach the target for all kind of reasons (e.g., mismatched schema, malformed json) are written to this table. (Example: your-project:your-dataset.your-table-name).
* **inputFileSpec** (The input filepattern to read from.): Cloud storage file pattern glob to read from. ex: gs://your-bucket/path/*.csv.
* **connectionUrl** (Elasticsearch URL or CloudID if using Elastic Cloud): Elasticsearch URL in the format https://hostname:[port] or specify CloudID if using Elastic Cloud (Example: https://elasticsearch-host:9200).
* **apiKey** (Base64 Encoded API Key for access without requiring basic authentication): Base64 Encoded API Key for access without requiring basic authentication. Refer to: https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-create-api-key.html#security-api-create-api-key-request.
* **index** (Elasticsearch index): The index toward which the requests will be issued (Example: my-index).

### Optional Parameters

* **inputFormat** (Input file format): Input file format. Default is: CSV.
* **containsHeaders** (Input CSV files contain a header record.): Input CSV files contain a header record (true/false). Only required if reading CSV files. Defaults to: false.
* **delimiter** (Column delimiter of the data files.): The column delimiter of the input text files. Default: use delimiter provided in csvFormat (Example: ,).
* **csvFormat** (CSV Format to use for parsing records.): CSV format specification to use for parsing records. Default is: Default. See https://commons.apache.org/proper/commons-csv/apidocs/org/apache/commons/csv/CSVFormat.html for more details. Must match format names exactly found at: https://commons.apache.org/proper/commons-csv/apidocs/org/apache/commons/csv/CSVFormat.Predefined.html.
* **jsonSchemaPath** (Path to JSON schema): Path to JSON schema. Default: null. (Example: gs://path/to/schema).
* **largeNumFiles** (Set to true if number of files is in the tens of thousands): Set to true if number of files is in the tens of thousands. Defaults to: false.
* **csvFileEncoding** (CSV file encoding): CSV file character encoding format. Allowed Values are US-ASCII, ISO-8859-1, UTF-8, UTF-16. Defaults to: UTF-8.
* **logDetailedCsvConversionErrors** (Log detailed CSV conversion errors): Set to true to enable detailed error logging when CSV parsing fails. Note that this may expose sensitive data in the logs (e.g., if the CSV file contains passwords). Default: false.
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
* **javascriptTextTransformGcsPath** (Cloud Storage path to Javascript UDF source): The Cloud Storage path pattern for the JavaScript code containing your user-defined functions. (Example: gs://your-bucket/your-function.js).
* **javascriptTextTransformFunctionName** (UDF Javascript Function Name): The name of the function to call from your JavaScript file. Use only letters, digits, and underscores. (Example: 'transform' or 'transform_udf1').


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
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="gcs-to-elasticsearch-job" \
-DtemplateName="GCS_to_Elasticsearch" \
-Dparameters="deadletterTable=$DEADLETTER_TABLE,inputFormat=$INPUT_FORMAT,inputFileSpec=$INPUT_FILE_SPEC,containsHeaders=$CONTAINS_HEADERS,delimiter=$DELIMITER,csvFormat=$CSV_FORMAT,jsonSchemaPath=$JSON_SCHEMA_PATH,largeNumFiles=$LARGE_NUM_FILES,csvFileEncoding=$CSV_FILE_ENCODING,logDetailedCsvConversionErrors=$LOG_DETAILED_CSV_CONVERSION_ERRORS,connectionUrl=$CONNECTION_URL,apiKey=$API_KEY,elasticsearchUsername=$ELASTICSEARCH_USERNAME,elasticsearchPassword=$ELASTICSEARCH_PASSWORD,index=$INDEX,batchSize=$BATCH_SIZE,batchSizeBytes=$BATCH_SIZE_BYTES,maxRetryAttempts=$MAX_RETRY_ATTEMPTS,maxRetryDuration=$MAX_RETRY_DURATION,propertyAsIndex=$PROPERTY_AS_INDEX,javaScriptIndexFnGcsPath=$JAVA_SCRIPT_INDEX_FN_GCS_PATH,javaScriptIndexFnName=$JAVA_SCRIPT_INDEX_FN_NAME,propertyAsId=$PROPERTY_AS_ID,javaScriptIdFnGcsPath=$JAVA_SCRIPT_ID_FN_GCS_PATH,javaScriptIdFnName=$JAVA_SCRIPT_ID_FN_NAME,javaScriptTypeFnGcsPath=$JAVA_SCRIPT_TYPE_FN_GCS_PATH,javaScriptTypeFnName=$JAVA_SCRIPT_TYPE_FN_NAME,javaScriptIsDeleteFnGcsPath=$JAVA_SCRIPT_IS_DELETE_FN_GCS_PATH,javaScriptIsDeleteFnName=$JAVA_SCRIPT_IS_DELETE_FN_NAME,usePartialUpdate=$USE_PARTIAL_UPDATE,bulkInsertMethod=$BULK_INSERT_METHOD,trustSelfSignedCerts=$TRUST_SELF_SIGNED_CERTS,disableCertificateValidation=$DISABLE_CERTIFICATE_VALIDATION,apiKeyKMSEncryptionKey=$API_KEY_KMSENCRYPTION_KEY,apiKeySecretId=$API_KEY_SECRET_ID,apiKeySource=$API_KEY_SOURCE,javascriptTextTransformGcsPath=$JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH,javascriptTextTransformFunctionName=$JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME" \
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
    deadletterTable = "your-project:your-dataset.your-table-name"
    inputFileSpec = "<inputFileSpec>"
    connectionUrl = "https://elasticsearch-host:9200"
    apiKey = "<apiKey>"
    index = "my-index"
    # inputFormat = "csv"
    # containsHeaders = "false"
    # delimiter = ","
    # csvFormat = "Default"
    # jsonSchemaPath = "gs://path/to/schema"
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
    # apiKeyKMSEncryptionKey = "projects/your-project-id/locations/global/keyRings/your-keyring/cryptoKeys/your-key-name"
    # apiKeySecretId = "projects/your-project-id/secrets/your-secret/versions/your-secret-version"
    # apiKeySource = "PLAINTEXT"
    # javascriptTextTransformGcsPath = "gs://your-bucket/your-function.js"
    # javascriptTextTransformFunctionName = "'transform' or 'transform_udf1'"
  }
}
```
