Text Files on Cloud Storage to Cloud Spanner Template
---
A pipeline to import a Cloud Spanner database from a set of Text (CSV) files in Cloud Storage.

:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/cloud-storage-to-cloud-spanner)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=GCS_Text_to_Cloud_Spanner).


:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required Parameters

* **instanceId** (Cloud Spanner instance id): The instance id of the Cloud Spanner database that you want to import to.
* **databaseId** (Cloud Spanner database id): The database id of the Cloud Spanner database that you want to import into (must already exist, and with the destination tables created).
* **importManifest** (Text Import Manifest file): The Cloud Storage path and filename of the text import manifest file. Text Import Manifest file, storing a json-encoded importManifest object. (Example: gs://your-bucket/your-folder/your-manifest.json).

### Optional Parameters

* **spannerHost** (Cloud Spanner Endpoint to call): The Cloud Spanner endpoint to call in the template. Only used for testing. (Example: https://batch-spanner.googleapis.com). Defaults to: https://batch-spanner.googleapis.com.
* **columnDelimiter** (Column delimiter of the data files): The column delimiter of the input text files. Defaults to ',' (Example: ,).
* **fieldQualifier** (Field qualifier used by the source file): The field qualifier used by the source file. It should be used when character needs to be escaped. Field qualifier should be used when character needs to be escaped. The default value is double quotes.
* **trailingDelimiter** (If true, the lines has trailing delimiters): The flag indicating whether or not the input lines have trailing delimiters. The default value is true. If the text file contains trailing delimiter, then set trailingDelimiter parameter to true during pipeline execution to import a Cloud Spanner database from a set of text files, otherwise set it to false.
* **escape** (Escape character): The escape character. The default value is NULL (not using the escape character).
* **nullString** (Null String): The string that represents the NULL value. The default value is null (not using the null string).
* **dateFormat** (Date format): The format used to parse date columns. By default, the pipeline tries to parse the date columns as "yyyy-MM-dd[' 00:00:00']" (e.g., 2019-01-31, or 2019-01-31 00:00:00). If your data format is different, please specify the format using the java.time.format.DateTimeFormatter patterns. For more details, please refer to https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/format/DateTimeFormatter.html.
* **timestampFormat** (Timestamp format): The format used to parse timestamp columns. If the timestamp is a long integer, then it is treated as Unix epoch (the microsecond since 1970-01-01T00:00:00.000Z. Otherwise, it is parsed as a string using the java.time.format.DateTimeFormatter.ISO_INSTANT format. For other cases, please specify you own pattern string, e.g., "MMM dd yyyy HH:mm:ss.SSSVV" for timestamp in the form of "Jan 21 1998 01:02:03.456+08:00". For more details, please refer to https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/format/DateTimeFormatter.html.
* **spannerProjectId** (Cloud Spanner Project Id): The project id of the Cloud Spanner instance.
* **spannerPriority** (Priority for Spanner RPC invocations): The request priority for Cloud Spanner calls. The value must be one of: [HIGH,MEDIUM,LOW].
* **handleNewLine** (Handle new line): If true, run the template in handleNewLine mode, which is slower but handles newline characters inside data. Defaults to: false.
* **invalidOutputPath** (Invalid rows output path): Cloud Storage path where to write rows that cannot be imported. (Example: gs://your-bucket/your-path). Defaults to empty.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!
[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=/v1/src/main/java/com/google/cloud/teleport/spanner/TextImportPipeline.java)

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
-DtemplateName="GCS_Text_to_Cloud_Spanner" \
-pl v1 \
-am
```

The `-DgcpTempLocation=<temp-bucket-name>` parameter can be specified to set the GCS bucket used by the DataflowRunner to write
temp files to during serialization. The path used will be `gs://<temp-bucket-name>/temp/`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Classic Template was staged! gs://<bucket-name>/templates/GCS_Text_to_Cloud_Spanner
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/GCS_Text_to_Cloud_Spanner"

### Required
export INSTANCE_ID=<instanceId>
export DATABASE_ID=<databaseId>
export IMPORT_MANIFEST=<importManifest>

### Optional
export SPANNER_HOST="https://batch-spanner.googleapis.com"
export COLUMN_DELIMITER=","
export FIELD_QUALIFIER="\""
export TRAILING_DELIMITER=true
export ESCAPE=<escape>
export NULL_STRING=<nullString>
export DATE_FORMAT=<dateFormat>
export TIMESTAMP_FORMAT=<timestampFormat>
export SPANNER_PROJECT_ID=<spannerProjectId>
export SPANNER_PRIORITY=<spannerPriority>
export HANDLE_NEW_LINE=false
export INVALID_OUTPUT_PATH=""

gcloud dataflow jobs run "gcs-text-to-cloud-spanner-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "instanceId=$INSTANCE_ID" \
  --parameters "databaseId=$DATABASE_ID" \
  --parameters "spannerHost=$SPANNER_HOST" \
  --parameters "importManifest=$IMPORT_MANIFEST" \
  --parameters "columnDelimiter=$COLUMN_DELIMITER" \
  --parameters "fieldQualifier=$FIELD_QUALIFIER" \
  --parameters "trailingDelimiter=$TRAILING_DELIMITER" \
  --parameters "escape=$ESCAPE" \
  --parameters "nullString=$NULL_STRING" \
  --parameters "dateFormat=$DATE_FORMAT" \
  --parameters "timestampFormat=$TIMESTAMP_FORMAT" \
  --parameters "spannerProjectId=$SPANNER_PROJECT_ID" \
  --parameters "spannerPriority=$SPANNER_PRIORITY" \
  --parameters "handleNewLine=$HANDLE_NEW_LINE" \
  --parameters "invalidOutputPath=$INVALID_OUTPUT_PATH"
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
export INSTANCE_ID=<instanceId>
export DATABASE_ID=<databaseId>
export IMPORT_MANIFEST=<importManifest>

### Optional
export SPANNER_HOST="https://batch-spanner.googleapis.com"
export COLUMN_DELIMITER=","
export FIELD_QUALIFIER="\""
export TRAILING_DELIMITER=true
export ESCAPE=<escape>
export NULL_STRING=<nullString>
export DATE_FORMAT=<dateFormat>
export TIMESTAMP_FORMAT=<timestampFormat>
export SPANNER_PROJECT_ID=<spannerProjectId>
export SPANNER_PRIORITY=<spannerPriority>
export HANDLE_NEW_LINE=false
export INVALID_OUTPUT_PATH=""

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="gcs-text-to-cloud-spanner-job" \
-DtemplateName="GCS_Text_to_Cloud_Spanner" \
-Dparameters="instanceId=$INSTANCE_ID,databaseId=$DATABASE_ID,spannerHost=$SPANNER_HOST,importManifest=$IMPORT_MANIFEST,columnDelimiter=$COLUMN_DELIMITER,fieldQualifier=$FIELD_QUALIFIER,trailingDelimiter=$TRAILING_DELIMITER,escape=$ESCAPE,nullString=$NULL_STRING,dateFormat=$DATE_FORMAT,timestampFormat=$TIMESTAMP_FORMAT,spannerProjectId=$SPANNER_PROJECT_ID,spannerPriority=$SPANNER_PRIORITY,handleNewLine=$HANDLE_NEW_LINE,invalidOutputPath=$INVALID_OUTPUT_PATH" \
-pl v1 \
-am
```
