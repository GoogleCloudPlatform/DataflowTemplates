
Text Files on Cloud Storage to Cloud Spanner template
---
The Cloud Storage Text to Cloud Spanner template is a batch pipeline that reads
CSV text files from Cloud Storage and imports them to a Cloud Spanner database.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/cloud-storage-to-cloud-spanner)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=GCS_Text_to_Cloud_Spanner).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **instanceId**: The instance ID of the Spanner database.
* **databaseId**: The database ID of the Spanner database.
* **importManifest**: The path in Cloud Storage to use when importing manifest files. For example, `gs://your-bucket/your-folder/your-manifest.json`.

### Optional parameters

* **spannerHost**: The Cloud Spanner endpoint to call in the template. Only used for testing. For example, `https://batch-spanner.googleapis.com`. Defaults to: https://batch-spanner.googleapis.com.
* **columnDelimiter**: The column delimiter that the source file uses. The default value is `,`. For example, `,`.
* **fieldQualifier**: The character that must surround any value in the source file that contains the columnDelimiter. The default value is double quotes.
* **trailingDelimiter**: Specifies whether the lines in the source files have trailing delimiters, that is, whether the `columnDelimiter` character appears at the end of each line, after the last column value. The default value is `true`.
* **escape**: The escape character the source file uses. By default, this parameter is not set and the template does not use the escape character.
* **nullString**: The string that represents a `NULL` value. By default, this parameter is not set and the template does not use the null string.
* **dateFormat**: The format used to parse date columns. By default, the pipeline tries to parse the date columns as `yyyy-M-d[' 00:00:00']`, for example, as `2019-01-31` or `2019-1-1 00:00:00`. If your date format is different, specify the format using the java.time.format.DateTimeFormatter (https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/format/DateTimeFormatter.html) patterns.
* **timestampFormat**: The format used to parse timestamp columns. If the timestamp is a long integer, then it is parsed as Unix epoch time. Otherwise, it is parsed as a string using the java.time.format.DateTimeFormatter.ISO_INSTANT (https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/format/DateTimeFormatter.html#ISO_INSTANT) format. For other cases, specify your own pattern string, for example, using `MMM dd yyyy HH:mm:ss.SSSVV` for timestamps in the form of `Jan 21 1998 01:02:03.456+08:00`.
* **spannerProjectId**: The ID of the Google Cloud project that contains the Spanner database. If not set, the project ID of the default Google Cloud project is used.
* **spannerPriority**: The request priority for Spanner calls. Possible values are `HIGH`, `MEDIUM`, and `LOW`. The default value is `MEDIUM`.
* **handleNewLine**: If `true`, the input data can contain newline characters. Otherwise, newline characters cause an error. The default value is `false`. Enabling newline handling can reduce performance.
* **invalidOutputPath**: The Cloud Storage path to use when writing rows that cannot be imported. For example, `gs://your-bucket/your-path`. Defaults to empty.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v1/src/main/java/com/google/cloud/teleport/spanner/TextImportPipeline.java)

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
-DtemplateName="GCS_Text_to_Cloud_Spanner" \
-f v1
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
export SPANNER_HOST=https://batch-spanner.googleapis.com
export COLUMN_DELIMITER=,
export FIELD_QUALIFIER="
export TRAILING_DELIMITER=true
export ESCAPE=<escape>
export NULL_STRING=""
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
export SPANNER_HOST=https://batch-spanner.googleapis.com
export COLUMN_DELIMITER=,
export FIELD_QUALIFIER="
export TRAILING_DELIMITER=true
export ESCAPE=<escape>
export NULL_STRING=""
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
cd v1/terraform/GCS_Text_to_Cloud_Spanner
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

resource "google_dataflow_job" "gcs_text_to_cloud_spanner" {

  provider          = google-beta
  template_gcs_path = "gs://dataflow-templates-${var.region}/latest/GCS_Text_to_Cloud_Spanner"
  name              = "gcs-text-to-cloud-spanner"
  region            = var.region
  temp_gcs_location = "gs://bucket-name-here/temp"
  parameters        = {
    instanceId = "<instanceId>"
    databaseId = "<databaseId>"
    importManifest = "<importManifest>"
    # spannerHost = "https://batch-spanner.googleapis.com"
    # columnDelimiter = ","
    # fieldQualifier = """
    # trailingDelimiter = "true"
    # escape = "<escape>"
    # nullString = ""
    # dateFormat = "<dateFormat>"
    # timestampFormat = "<timestampFormat>"
    # spannerProjectId = "<spannerProjectId>"
    # spannerPriority = "<spannerPriority>"
    # handleNewLine = "false"
    # invalidOutputPath = ""
  }
}
```
