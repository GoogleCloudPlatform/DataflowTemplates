
Cloud Storage To Splunk template
---
A pipeline that reads a set of Text (CSV) files in Cloud Storage and writes to
Splunk's HTTP Event Collector (HEC).

The template creates the Splunk payload as a JSON element using either CSV
headers (default), JSON schema or JavaScript UDF. If a Javascript UDF and JSON
schema are both inputted as parameters, only the Javascript UDF will be executed.



:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **invalidOutputPath** : Cloud Storage path where to write objects that could not be converted to Splunk objects or pushed to Splunk. (Example: gs://your-bucket/your-path).
* **inputFileSpec** : The Cloud Storage file pattern to search for CSV files. Example: gs://mybucket/test-*.csv.
* **deadletterTable** : Messages failed to reach the target for all kind of reasons (e.g., mismatched schema, malformed json) are written to this table. (Example: your-project:your-dataset.your-table-name).
* **url** : Splunk Http Event Collector (HEC) url. This should be routable from the VPC in which the pipeline runs. (Example: https://splunk-hec-host:8088).
* **tokenSource** : Source of the token. One of PLAINTEXT, KMS or SECRET_MANAGER. If tokenSource is set to KMS, tokenKMSEncryptionKey and encrypted token must be provided. If tokenSource is set to SECRET_MANAGER, tokenSecretId must be provided. If tokenSource is set to PLAINTEXT, token must be provided.

### Optional parameters

* **containsHeaders** : Input CSV files contain a header record (true/false). Only required if reading CSV files. Defaults to: false.
* **delimiter** : The column delimiter of the input text files. Default: use delimiter provided in csvFormat (Example: ,).
* **csvFormat** : CSV format specification to use for parsing records. Default is: Default. See https://commons.apache.org/proper/commons-csv/apidocs/org/apache/commons/csv/CSVFormat.html for more details. Must match format names exactly found at: https://commons.apache.org/proper/commons-csv/apidocs/org/apache/commons/csv/CSVFormat.Predefined.html.
* **jsonSchemaPath** : The path to the JSON schema. Defaults to: null. (Example: gs://path/to/schema).
* **largeNumFiles** : Set to true if number of files is in the tens of thousands. Defaults to: false.
* **csvFileEncoding** : The CSV file character encoding format. Allowed Values are US-ASCII, ISO-8859-1, UTF-8, and UTF-16. Defaults to: UTF-8.
* **logDetailedCsvConversionErrors** : Set to true to enable detailed error logging when CSV parsing fails. Note that this may expose sensitive data in the logs (e.g., if the CSV file contains passwords). Default: false.
* **token** : Splunk Http Event Collector (HEC) authentication token. Must be provided if the tokenSource is set to PLAINTEXT or KMS.
* **batchCount** : Batch size for sending multiple events to Splunk HEC. Default 1 (no batching).
* **disableCertificateValidation** : Disable SSL certificate validation (true/false). Default false (validation enabled). If true, the certificates are not validated (all certificates are trusted) and  `rootCaCertificatePath` parameter is ignored.
* **parallelism** : Maximum number of parallel requests. Default: 1 (no parallelism).
* **tokenKMSEncryptionKey** : The Cloud KMS key to decrypt the HEC token string. This parameter must be provided if the tokenSource is set to KMS. If this parameter is provided, token string should be passed in encrypted. Encrypt parameters using the KMS API encrypt endpoint. The Key should be in the format projects/{gcp_project}/locations/{key_region}/keyRings/{key_ring}/cryptoKeys/{kms_key_name}. See: https://cloud.google.com/kms/docs/reference/rest/v1/projects.locations.keyRings.cryptoKeys/encrypt  (Example: projects/your-project-id/locations/global/keyRings/your-keyring/cryptoKeys/your-key-name).
* **tokenSecretId** : Secret Manager secret ID for the token. This parameter should be provided if the tokenSource is set to SECRET_MANAGER. Should be in the format projects/{project}/secrets/{secret}/versions/{secret_version}. (Example: projects/your-project-id/secrets/your-secret/versions/your-secret-version).
* **rootCaCertificatePath** : The full URL to root CA certificate in Cloud Storage. The certificate provided in Cloud Storage must be DER-encoded and may be supplied in binary or printable (Base64) encoding. If the certificate is provided in Base64 encoding, it must be bounded at the beginning by -----BEGIN CERTIFICATE-----, and must be bounded at the end by -----END CERTIFICATE-----. If this parameter is provided, this private CA certificate file will be fetched and added to Dataflow worker's trust store in order to verify Splunk HEC endpoint's SSL certificate which is signed by that private CA. If this parameter is not provided, the default trust store is used. (Example: gs://mybucket/mycerts/privateCA.crt).
* **enableBatchLogs** : Parameter which specifies if logs should be enabled for batches written to Splunk. Defaults to: true.
* **enableGzipHttpCompression** : Parameter which specifies if HTTP requests sent to Splunk HEC should be GZIP encoded. Defaults to: true.
* **javascriptTextTransformGcsPath** : The Cloud Storage URI of the .js file that defines the JavaScript user-defined function (UDF) to use. (Example: gs://my-bucket/my-udfs/my_file.js).
* **javascriptTextTransformFunctionName** : The name of the JavaScript user-defined function (UDF) to use. For example, if your JavaScript function code is `myTransform(inJson) { /*...do stuff...*/ }`, then the function name is `myTransform`. For sample JavaScript UDFs, see UDF Examples (https://github.com/GoogleCloudPlatform/DataflowTemplates#udf-examples).


## User-Defined functions (UDFs)

The Cloud Storage To Splunk Template supports User-Defined functions (UDFs).
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

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/googlecloud-to-splunk/src/main/java/com/google/cloud/teleport/v2/templates/GCSToSplunk.java)

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
-DtemplateName="GCS_To_Splunk" \
-f v2/googlecloud-to-splunk
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/GCS_To_Splunk
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/GCS_To_Splunk"

### Required
export INVALID_OUTPUT_PATH=<invalidOutputPath>
export INPUT_FILE_SPEC=<inputFileSpec>
export DEADLETTER_TABLE=<deadletterTable>
export URL=<url>
export TOKEN_SOURCE=<tokenSource>

### Optional
export CONTAINS_HEADERS=false
export DELIMITER=<delimiter>
export CSV_FORMAT=Default
export JSON_SCHEMA_PATH=<jsonSchemaPath>
export LARGE_NUM_FILES=false
export CSV_FILE_ENCODING=UTF-8
export LOG_DETAILED_CSV_CONVERSION_ERRORS=false
export TOKEN=<token>
export BATCH_COUNT=<batchCount>
export DISABLE_CERTIFICATE_VALIDATION=<disableCertificateValidation>
export PARALLELISM=<parallelism>
export TOKEN_KMSENCRYPTION_KEY=<tokenKMSEncryptionKey>
export TOKEN_SECRET_ID=<tokenSecretId>
export ROOT_CA_CERTIFICATE_PATH=<rootCaCertificatePath>
export ENABLE_BATCH_LOGS=true
export ENABLE_GZIP_HTTP_COMPRESSION=true
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>

gcloud dataflow flex-template run "gcs-to-splunk-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "invalidOutputPath=$INVALID_OUTPUT_PATH" \
  --parameters "inputFileSpec=$INPUT_FILE_SPEC" \
  --parameters "containsHeaders=$CONTAINS_HEADERS" \
  --parameters "deadletterTable=$DEADLETTER_TABLE" \
  --parameters "delimiter=$DELIMITER" \
  --parameters "csvFormat=$CSV_FORMAT" \
  --parameters "jsonSchemaPath=$JSON_SCHEMA_PATH" \
  --parameters "largeNumFiles=$LARGE_NUM_FILES" \
  --parameters "csvFileEncoding=$CSV_FILE_ENCODING" \
  --parameters "logDetailedCsvConversionErrors=$LOG_DETAILED_CSV_CONVERSION_ERRORS" \
  --parameters "token=$TOKEN" \
  --parameters "url=$URL" \
  --parameters "batchCount=$BATCH_COUNT" \
  --parameters "disableCertificateValidation=$DISABLE_CERTIFICATE_VALIDATION" \
  --parameters "parallelism=$PARALLELISM" \
  --parameters "tokenSource=$TOKEN_SOURCE" \
  --parameters "tokenKMSEncryptionKey=$TOKEN_KMSENCRYPTION_KEY" \
  --parameters "tokenSecretId=$TOKEN_SECRET_ID" \
  --parameters "rootCaCertificatePath=$ROOT_CA_CERTIFICATE_PATH" \
  --parameters "enableBatchLogs=$ENABLE_BATCH_LOGS" \
  --parameters "enableGzipHttpCompression=$ENABLE_GZIP_HTTP_COMPRESSION" \
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
export INVALID_OUTPUT_PATH=<invalidOutputPath>
export INPUT_FILE_SPEC=<inputFileSpec>
export DEADLETTER_TABLE=<deadletterTable>
export URL=<url>
export TOKEN_SOURCE=<tokenSource>

### Optional
export CONTAINS_HEADERS=false
export DELIMITER=<delimiter>
export CSV_FORMAT=Default
export JSON_SCHEMA_PATH=<jsonSchemaPath>
export LARGE_NUM_FILES=false
export CSV_FILE_ENCODING=UTF-8
export LOG_DETAILED_CSV_CONVERSION_ERRORS=false
export TOKEN=<token>
export BATCH_COUNT=<batchCount>
export DISABLE_CERTIFICATE_VALIDATION=<disableCertificateValidation>
export PARALLELISM=<parallelism>
export TOKEN_KMSENCRYPTION_KEY=<tokenKMSEncryptionKey>
export TOKEN_SECRET_ID=<tokenSecretId>
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
-DjobName="gcs-to-splunk-job" \
-DtemplateName="GCS_To_Splunk" \
-Dparameters="invalidOutputPath=$INVALID_OUTPUT_PATH,inputFileSpec=$INPUT_FILE_SPEC,containsHeaders=$CONTAINS_HEADERS,deadletterTable=$DEADLETTER_TABLE,delimiter=$DELIMITER,csvFormat=$CSV_FORMAT,jsonSchemaPath=$JSON_SCHEMA_PATH,largeNumFiles=$LARGE_NUM_FILES,csvFileEncoding=$CSV_FILE_ENCODING,logDetailedCsvConversionErrors=$LOG_DETAILED_CSV_CONVERSION_ERRORS,token=$TOKEN,url=$URL,batchCount=$BATCH_COUNT,disableCertificateValidation=$DISABLE_CERTIFICATE_VALIDATION,parallelism=$PARALLELISM,tokenSource=$TOKEN_SOURCE,tokenKMSEncryptionKey=$TOKEN_KMSENCRYPTION_KEY,tokenSecretId=$TOKEN_SECRET_ID,rootCaCertificatePath=$ROOT_CA_CERTIFICATE_PATH,enableBatchLogs=$ENABLE_BATCH_LOGS,enableGzipHttpCompression=$ENABLE_GZIP_HTTP_COMPRESSION,javascriptTextTransformGcsPath=$JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH,javascriptTextTransformFunctionName=$JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME" \
-f v2/googlecloud-to-splunk
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
cd v2/googlecloud-to-splunk/terraform/GCS_To_Splunk
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

resource "google_dataflow_flex_template_job" "gcs_to_splunk" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/GCS_To_Splunk"
  name              = "gcs-to-splunk"
  region            = var.region
  parameters        = {
    invalidOutputPath = "gs://your-bucket/your-path"
    inputFileSpec = "<inputFileSpec>"
    deadletterTable = "your-project:your-dataset.your-table-name"
    url = "https://splunk-hec-host:8088"
    tokenSource = "<tokenSource>"
    # containsHeaders = "false"
    # delimiter = ","
    # csvFormat = "Default"
    # jsonSchemaPath = "gs://path/to/schema"
    # largeNumFiles = "false"
    # csvFileEncoding = "UTF-8"
    # logDetailedCsvConversionErrors = "false"
    # token = "<token>"
    # batchCount = "<batchCount>"
    # disableCertificateValidation = "<disableCertificateValidation>"
    # parallelism = "<parallelism>"
    # tokenKMSEncryptionKey = "projects/your-project-id/locations/global/keyRings/your-keyring/cryptoKeys/your-key-name"
    # tokenSecretId = "projects/your-project-id/secrets/your-secret/versions/your-secret-version"
    # rootCaCertificatePath = "gs://mybucket/mycerts/privateCA.crt"
    # enableBatchLogs = "true"
    # enableGzipHttpCompression = "true"
    # javascriptTextTransformGcsPath = "gs://my-bucket/my-udfs/my_file.js"
    # javascriptTextTransformFunctionName = "<javascriptTextTransformFunctionName>"
  }
}
```
