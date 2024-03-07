
CSV Files on Cloud Storage to BigQuery template
---
The Cloud Storage CSV to BigQuery pipeline is a batch pipeline that allows you to
read CSV files stored in Cloud Storage, and append the result to a BigQuery
table. The CSV files can be uncompressed or compressed in formats listed in
https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/Compression.html.



:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required Parameters

* **inputFilePattern** (Cloud Storage Input File(s)): Path of the file pattern glob to read from. (Example: gs://your-bucket/path/*.csv).
* **schemaJSONPath** (Cloud Storage location of your BigQuery schema file, described as a JSON): JSON file with BigQuery Schema description. JSON Example: {
	"BigQuery Schema": [
		{
			"name": "location",
			"type": "STRING"
		},
		{
			"name": "name",
			"type": "STRING"
		},
		{
			"name": "age",
			"type": "STRING"
		},
		{
			"name": "color",
			"type": "STRING"
		},
		{
			"name": "coffee",
			"type": "STRING"
		}
	]
}.
* **outputTable** (BigQuery output table): BigQuery table location to write the output to. The table's schema must match the input objects.
* **bigQueryLoadingTemporaryDirectory** (Temporary directory for BigQuery loading process): Temporary directory for BigQuery loading process (Example: gs://your-bucket/your-files/temp_dir).
* **badRecordsOutputTable** (BigQuery output table for bad records): BigQuery table location to write the bad record. The table's schema must match the {RawContent: STRING, ErrorMsg:STRING}.
* **delimiter** (Column delimiter of the data files.): The column delimiter of the input text files. Default: use delimiter provided in csvFormat (Example: ,).
* **csvFormat** (CSV Format to use for parsing records.): CSV format specification to use for parsing records. See https://commons.apache.org/proper/commons-csv/apidocs/org/apache/commons/csv/CSVFormat.html for more details. Must match format names exactly found at: https://commons.apache.org/proper/commons-csv/apidocs/org/apache/commons/csv/CSVFormat.Predefined.html.

### Optional Parameters

* **containsHeaders** (Whether input CSV files contain a header record.): Input CSV files contain a header record (true/false). Defaults to: false.
* **csvFileEncoding** (CSV file encoding): CSV file character encoding format. Allowed Values are US-ASCII, ISO-8859-1, UTF-8, UTF-16. Defaults to: UTF-8.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v1/src/main/java/com/google/cloud/teleport/templates/CSVToBigQuery.java)

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
-DtemplateName="GCS_CSV_to_BigQuery" \
-f v1
```

The `-DgcpTempLocation=<temp-bucket-name>` parameter can be specified to set the GCS bucket used by the DataflowRunner to write
temp files to during serialization. The path used will be `gs://<temp-bucket-name>/temp/`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Classic Template was staged! gs://<bucket-name>/templates/GCS_CSV_to_BigQuery
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/GCS_CSV_to_BigQuery"

### Required
export INPUT_FILE_PATTERN=<inputFilePattern>
export SCHEMA_JSONPATH=<schemaJSONPath>
export OUTPUT_TABLE=<outputTable>
export BIG_QUERY_LOADING_TEMPORARY_DIRECTORY=<bigQueryLoadingTemporaryDirectory>
export BAD_RECORDS_OUTPUT_TABLE=<badRecordsOutputTable>
export DELIMITER=<delimiter>
export CSV_FORMAT=<csvFormat>

### Optional
export CONTAINS_HEADERS=false
export CSV_FILE_ENCODING=UTF-8

gcloud dataflow jobs run "gcs-csv-to-bigquery-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "inputFilePattern=$INPUT_FILE_PATTERN" \
  --parameters "schemaJSONPath=$SCHEMA_JSONPATH" \
  --parameters "outputTable=$OUTPUT_TABLE" \
  --parameters "bigQueryLoadingTemporaryDirectory=$BIG_QUERY_LOADING_TEMPORARY_DIRECTORY" \
  --parameters "badRecordsOutputTable=$BAD_RECORDS_OUTPUT_TABLE" \
  --parameters "containsHeaders=$CONTAINS_HEADERS" \
  --parameters "delimiter=$DELIMITER" \
  --parameters "csvFormat=$CSV_FORMAT" \
  --parameters "csvFileEncoding=$CSV_FILE_ENCODING"
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
export INPUT_FILE_PATTERN=<inputFilePattern>
export SCHEMA_JSONPATH=<schemaJSONPath>
export OUTPUT_TABLE=<outputTable>
export BIG_QUERY_LOADING_TEMPORARY_DIRECTORY=<bigQueryLoadingTemporaryDirectory>
export BAD_RECORDS_OUTPUT_TABLE=<badRecordsOutputTable>
export DELIMITER=<delimiter>
export CSV_FORMAT=<csvFormat>

### Optional
export CONTAINS_HEADERS=false
export CSV_FILE_ENCODING=UTF-8

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="gcs-csv-to-bigquery-job" \
-DtemplateName="GCS_CSV_to_BigQuery" \
-Dparameters="inputFilePattern=$INPUT_FILE_PATTERN,schemaJSONPath=$SCHEMA_JSONPATH,outputTable=$OUTPUT_TABLE,bigQueryLoadingTemporaryDirectory=$BIG_QUERY_LOADING_TEMPORARY_DIRECTORY,badRecordsOutputTable=$BAD_RECORDS_OUTPUT_TABLE,containsHeaders=$CONTAINS_HEADERS,delimiter=$DELIMITER,csvFormat=$CSV_FORMAT,csvFileEncoding=$CSV_FILE_ENCODING" \
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
cd v1/terraform/GCS_CSV_to_BigQuery
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

resource "google_dataflow_job" "gcs_csv_to_bigquery" {

  provider          = google-beta
  template_gcs_path = "gs://dataflow-templates-${var.region}/latest/GCS_CSV_to_BigQuery"
  name              = "gcs-csv-to-bigquery"
  region            = var.region
  temp_gcs_location = "gs://bucket-name-here/temp"
  parameters        = {
    inputFilePattern = "gs://your-bucket/path/*.csv"
    schemaJSONPath = "<schemaJSONPath>"
    outputTable = "<outputTable>"
    bigQueryLoadingTemporaryDirectory = "gs://your-bucket/your-files/temp_dir"
    badRecordsOutputTable = "<badRecordsOutputTable>"
    delimiter = ","
    csvFormat = "<csvFormat>"
    # containsHeaders = "false"
    # csvFileEncoding = "UTF-8"
  }
}
```
