Cloud Storage To Splunk Template
---
A pipeline that reads a set of Text (CSV) files in Cloud Storage and writes to Splunk's HTTP Event Collector (HEC).

:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided-templates)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=GCS_To_Splunk).

The template creates the Splunk payload as a JSON element using either CSV headers (default), JSON schema or JavaScript UDF. If a Javascript UDF and JSON schema are both inputted as parameters, only the Javascript UDF will be executed.

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required Parameters

* **invalidOutputPath** (Invalid events output path): Cloud Storage path where to write objects that could not be converted to Splunk objects or pushed to Splunk. (Example: gs://your-bucket/your-path).
* **inputFileSpec** (The input filepattern to read from.): Cloud storage file pattern glob to read from. ex: gs://your-bucket/path/*.csv.
* **deadletterTable** (BigQuery Deadletter table to send failed inserts.): Messages failed to reach the target for all kind of reasons (e.g., mismatched schema, malformed json) are written to this table. (Example: your-project:your-dataset.your-table-name).
* **url** (Splunk HEC URL.): Splunk Http Event Collector (HEC) url. This should be routable from the VPC in which the pipeline runs. (Example: https://splunk-hec-host:8088).

### Optional Parameters

* **containsHeaders** (Input CSV files contain a header record.): Input CSV files contain a header record (true/false). Only required if reading CSV files. Defaults to: false.
* **delimiter** (Column delimiter of the data files.): The column delimiter of the input text files. Default: use delimiter provided in csvFormat (Example: ,).
* **csvFormat** (CSV Format to use for parsing records.): CSV format specification to use for parsing records. Default is: Default. See https://commons.apache.org/proper/commons-csv/apidocs/org/apache/commons/csv/CSVFormat.html for more details. Must match format names exactly found at: https://commons.apache.org/proper/commons-csv/apidocs/org/apache/commons/csv/CSVFormat.Predefined.html.
* **jsonSchemaPath** (Path to JSON schema): Path to JSON schema. Default: null. (Example: gs://path/to/schema).
* **largeNumFiles** (Set to true if number of files is in the tens of thousands): Set to true if number of files is in the tens of thousands. Defaults to: false.
* **csvFileEncoding** (CSV file encoding): CSV file character encoding format. Allowed Values are US-ASCII, ISO-8859-1, UTF-8, UTF-16. Defaults to: UTF-8.
* **logDetailedCsvConversionErrors** (Log detailed CSV conversion errors): Set to true to enable detailed error logging when CSV parsing fails. Note that this may expose sensitive data in the logs (e.g., if the CSV file contains passwords). Default: false.
* **token** (HEC Authentication token.): Splunk Http Event Collector (HEC) authentication token. Must be provided if the tokenSource is set to PLAINTEXT or KMS.
* **batchCount** (Batch size for sending multiple events to Splunk HEC.): Batch size for sending multiple events to Splunk HEC. Default 1 (no batching).
* **disableCertificateValidation** (Disable SSL certificate validation.): Disable SSL certificate validation (true/false). Default false (validation enabled). If true, the certificates are not validated (all certificates are trusted) and  `rootCaCertificatePath` parameter is ignored.
* **parallelism** (Maximum number of parallel requests.): Maximum number of parallel requests. Default: 1 (no parallelism).
* **javascriptTextTransformGcsPath** (Cloud Storage path to Javascript UDF source): The Cloud Storage path pattern for the JavaScript code containing your user-defined functions. (Example: gs://your-bucket/your-function.js).
* **javascriptTextTransformFunctionName** (UDF Javascript Function Name): The name of the function to call from your JavaScript file. Use only letters, digits, and underscores. (Example: 'transform' or 'transform_udf1').


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
[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=/v2/googlecloud-to-splunk/src/main/java/com/google/cloud/teleport/v2/templates/GCSToSplunk.java)

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
-DtemplateName="GCS_To_Splunk" \
-pl v2/googlecloud-to-splunk \
-am
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

### Optional
export CONTAINS_HEADERS=false
export DELIMITER=<delimiter>
export CSV_FORMAT="Default"
export JSON_SCHEMA_PATH=<jsonSchemaPath>
export LARGE_NUM_FILES=false
export CSV_FILE_ENCODING="UTF-8"
export LOG_DETAILED_CSV_CONVERSION_ERRORS=false
export TOKEN=<token>
export BATCH_COUNT=<batchCount>
export DISABLE_CERTIFICATE_VALIDATION=<disableCertificateValidation>
export PARALLELISM=<parallelism>
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

### Optional
export CONTAINS_HEADERS=false
export DELIMITER=<delimiter>
export CSV_FORMAT="Default"
export JSON_SCHEMA_PATH=<jsonSchemaPath>
export LARGE_NUM_FILES=false
export CSV_FILE_ENCODING="UTF-8"
export LOG_DETAILED_CSV_CONVERSION_ERRORS=false
export TOKEN=<token>
export BATCH_COUNT=<batchCount>
export DISABLE_CERTIFICATE_VALIDATION=<disableCertificateValidation>
export PARALLELISM=<parallelism>
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="gcs-to-splunk-job" \
-DtemplateName="GCS_To_Splunk" \
-Dparameters="invalidOutputPath=$INVALID_OUTPUT_PATH,inputFileSpec=$INPUT_FILE_SPEC,containsHeaders=$CONTAINS_HEADERS,deadletterTable=$DEADLETTER_TABLE,delimiter=$DELIMITER,csvFormat=$CSV_FORMAT,jsonSchemaPath=$JSON_SCHEMA_PATH,largeNumFiles=$LARGE_NUM_FILES,csvFileEncoding=$CSV_FILE_ENCODING,logDetailedCsvConversionErrors=$LOG_DETAILED_CSV_CONVERSION_ERRORS,token=$TOKEN,url=$URL,batchCount=$BATCH_COUNT,disableCertificateValidation=$DISABLE_CERTIFICATE_VALIDATION,parallelism=$PARALLELISM,javascriptTextTransformGcsPath=$JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH,javascriptTextTransformFunctionName=$JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME" \
-pl v2/googlecloud-to-splunk \
-am
```
