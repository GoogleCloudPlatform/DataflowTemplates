
Data Masking/Tokenization from Cloud Storage to BigQuery (using Cloud DLP) template
---
The Data Masking/Tokenization from Cloud Storage to BigQuery template uses <a
href="https://cloud.google.com/dlp/docs">Sensitive Data Protection</a> and
creates a streaming pipeline that does the following steps:
1. Reads CSV files from a Cloud Storage bucket.
2. Calls the Cloud Data Loss Prevention API (part of Sensitive Data Protection)
for de-identification.
3. Writes the de-identified data into the specified BigQuery table.

The template supports using both a Sensitive Data Protection <a
href="https://cloud.google.com/dlp/docs/creating-templates">inspection
template</a> and a Sensitive Data Protection <a
href="https://cloud.google.com/dlp/docs/creating-templates-deid">de-identification
template</a>. As a result, the template supports both of the following tasks:
- Inspect for potentially sensitive information and de-identify the data.
- De-identify structured data where columns are specified to be de-identified and
no inspection is needed.

Note: This template does not support a regional path for de-identification
template location. Only a global path is supported.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/dlp-text-to-bigquery)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Stream_DLP_GCS_Text_to_BigQuery).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required Parameters

* **inputFilePattern** (Input Cloud Storage File(s)): The Cloud Storage location of the files you'd like to process. (Example: gs://your-bucket/your-files/*.csv).
* **deidentifyTemplateName** (Cloud DLP deidentify template name): Cloud DLP template to deidentify contents. Must be created here: https://console.cloud.google.com/security/dlp/create/template. (Example: projects/your-project-id/locations/global/deidentifyTemplates/generated_template_id).
* **datasetName** (BigQuery Dataset): BigQuery Dataset to be used. Dataset must exist prior to execution. Ex. pii_dataset.
* **dlpProjectId** (Cloud DLP project ID): Cloud DLP project ID to be used for data masking/tokenization. Ex. your-dlp-project.

### Optional Parameters

* **inspectTemplateName** (Cloud DLP inspect template name): Cloud DLP template to inspect contents. (Example: projects/your-project-id/locations/global/inspectTemplates/generated_template_id).
* **batchSize** (Batch size): Batch size contents (number of rows) to optimize DLP API call. Total size of the rows must not exceed 512 KB and total cell count must not exceed 50,000. Default batch size is set to 100. Ex. 1000.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!
[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v1/src/main/java/com/google/cloud/teleport/templates/DLPTextToBigQueryStreaming.java)

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
-DtemplateName="Stream_DLP_GCS_Text_to_BigQuery" \
-pl v1 \
-am
```

The `-DgcpTempLocation=<temp-bucket-name>` parameter can be specified to set the GCS bucket used by the DataflowRunner to write
temp files to during serialization. The path used will be `gs://<temp-bucket-name>/temp/`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Classic Template was staged! gs://<bucket-name>/templates/Stream_DLP_GCS_Text_to_BigQuery
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/Stream_DLP_GCS_Text_to_BigQuery"

### Required
export INPUT_FILE_PATTERN=<inputFilePattern>
export DEIDENTIFY_TEMPLATE_NAME=<deidentifyTemplateName>
export DATASET_NAME=<datasetName>
export DLP_PROJECT_ID=<dlpProjectId>

### Optional
export INSPECT_TEMPLATE_NAME=<inspectTemplateName>
export BATCH_SIZE=<batchSize>

gcloud dataflow jobs run "stream-dlp-gcs-text-to-bigquery-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "inputFilePattern=$INPUT_FILE_PATTERN" \
  --parameters "deidentifyTemplateName=$DEIDENTIFY_TEMPLATE_NAME" \
  --parameters "inspectTemplateName=$INSPECT_TEMPLATE_NAME" \
  --parameters "batchSize=$BATCH_SIZE" \
  --parameters "datasetName=$DATASET_NAME" \
  --parameters "dlpProjectId=$DLP_PROJECT_ID"
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
export DEIDENTIFY_TEMPLATE_NAME=<deidentifyTemplateName>
export DATASET_NAME=<datasetName>
export DLP_PROJECT_ID=<dlpProjectId>

### Optional
export INSPECT_TEMPLATE_NAME=<inspectTemplateName>
export BATCH_SIZE=<batchSize>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="stream-dlp-gcs-text-to-bigquery-job" \
-DtemplateName="Stream_DLP_GCS_Text_to_BigQuery" \
-Dparameters="inputFilePattern=$INPUT_FILE_PATTERN,deidentifyTemplateName=$DEIDENTIFY_TEMPLATE_NAME,inspectTemplateName=$INSPECT_TEMPLATE_NAME,batchSize=$BATCH_SIZE,datasetName=$DATASET_NAME,dlpProjectId=$DLP_PROJECT_ID" \
-pl v1 \
-am
```
