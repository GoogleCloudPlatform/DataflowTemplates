
Data Masking/Tokenization from Cloud Storage to BigQuery (using Cloud DLP) with BQ Storage Write API support template
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
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Stream_DLP_GCS_Text_to_BigQuery_Flex).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **inputFilePattern** : The Cloud Storage location of the files you'd like to process. (Example: gs://your-bucket/your-files/*.csv).
* **deidentifyTemplateName** : Cloud DLP template to deidentify contents. Must be created here: https://console.cloud.google.com/security/dlp/create/template. (Example: projects/your-project-id/locations/global/deidentifyTemplates/generated_template_id).
* **datasetName** : BigQuery Dataset to be used. Dataset must exist prior to execution. Ex. pii_dataset.
* **dlpProjectId** : Cloud DLP project ID to be used for data masking/tokenization. Ex. your-dlp-project.

### Optional parameters

* **inspectTemplateName** : Cloud DLP template to inspect contents. (Example: projects/your-project-id/locations/global/inspectTemplates/generated_template_id).
* **batchSize** : Batch size contents (number of rows) to optimize DLP API call. Total size of the rows must not exceed 512 KB and total cell count must not exceed 50,000. Default batch size is set to 100. Ex. 1000.
* **useStorageWriteApi** : If true, the pipeline uses the BigQuery Storage Write API (https://cloud.google.com/bigquery/docs/write-api). The default value is `false`. For more information, see Using the Storage Write API (https://beam.apache.org/documentation/io/built-in/google-bigquery/#storage-write-api).
* **useStorageWriteApiAtLeastOnce** :  When using the Storage Write API, specifies the write semantics. To use at-least once semantics (https://beam.apache.org/documentation/io/built-in/google-bigquery/#at-least-once-semantics), set this parameter to `true`. To use exactly-once semantics, set the parameter to `false`. This parameter applies only when `useStorageWriteApi` is `true`. The default value is `false`.
* **numStorageWriteApiStreams** : When using the Storage Write API, specifies the number of write streams. If `useStorageWriteApi` is `true` and `useStorageWriteApiAtLeastOnce` is `false`, then you must set this parameter. Defaults to: 0.
* **storageWriteApiTriggeringFrequencySec** : When using the Storage Write API, specifies the triggering frequency, in seconds. If `useStorageWriteApi` is `true` and `useStorageWriteApiAtLeastOnce` is `false`, then you must set this parameter.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/googlecloud-to-googlecloud/src/main/java/com/google/cloud/teleport/v2/templates/DLPTextToBigQueryStreaming.java)

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#templates-plugin).

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
-DtemplateName="Stream_DLP_GCS_Text_to_BigQuery_Flex" \
-f v2/googlecloud-to-googlecloud
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Stream_DLP_GCS_Text_to_BigQuery_Flex
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Stream_DLP_GCS_Text_to_BigQuery_Flex"

### Required
export INPUT_FILE_PATTERN=<inputFilePattern>
export DEIDENTIFY_TEMPLATE_NAME=<deidentifyTemplateName>
export DATASET_NAME=<datasetName>
export DLP_PROJECT_ID=<dlpProjectId>

### Optional
export INSPECT_TEMPLATE_NAME=<inspectTemplateName>
export BATCH_SIZE=100
export USE_STORAGE_WRITE_API=false
export USE_STORAGE_WRITE_API_AT_LEAST_ONCE=false
export NUM_STORAGE_WRITE_API_STREAMS=0
export STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC=<storageWriteApiTriggeringFrequencySec>

gcloud dataflow flex-template run "stream-dlp-gcs-text-to-bigquery-flex-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "inputFilePattern=$INPUT_FILE_PATTERN" \
  --parameters "deidentifyTemplateName=$DEIDENTIFY_TEMPLATE_NAME" \
  --parameters "inspectTemplateName=$INSPECT_TEMPLATE_NAME" \
  --parameters "batchSize=$BATCH_SIZE" \
  --parameters "datasetName=$DATASET_NAME" \
  --parameters "dlpProjectId=$DLP_PROJECT_ID" \
  --parameters "useStorageWriteApi=$USE_STORAGE_WRITE_API" \
  --parameters "useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE" \
  --parameters "numStorageWriteApiStreams=$NUM_STORAGE_WRITE_API_STREAMS" \
  --parameters "storageWriteApiTriggeringFrequencySec=$STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC"
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
export INPUT_FILE_PATTERN=<inputFilePattern>
export DEIDENTIFY_TEMPLATE_NAME=<deidentifyTemplateName>
export DATASET_NAME=<datasetName>
export DLP_PROJECT_ID=<dlpProjectId>

### Optional
export INSPECT_TEMPLATE_NAME=<inspectTemplateName>
export BATCH_SIZE=100
export USE_STORAGE_WRITE_API=false
export USE_STORAGE_WRITE_API_AT_LEAST_ONCE=false
export NUM_STORAGE_WRITE_API_STREAMS=0
export STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC=<storageWriteApiTriggeringFrequencySec>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="stream-dlp-gcs-text-to-bigquery-flex-job" \
-DtemplateName="Stream_DLP_GCS_Text_to_BigQuery_Flex" \
-Dparameters="inputFilePattern=$INPUT_FILE_PATTERN,deidentifyTemplateName=$DEIDENTIFY_TEMPLATE_NAME,inspectTemplateName=$INSPECT_TEMPLATE_NAME,batchSize=$BATCH_SIZE,datasetName=$DATASET_NAME,dlpProjectId=$DLP_PROJECT_ID,useStorageWriteApi=$USE_STORAGE_WRITE_API,useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE,numStorageWriteApiStreams=$NUM_STORAGE_WRITE_API_STREAMS,storageWriteApiTriggeringFrequencySec=$STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC" \
-f v2/googlecloud-to-googlecloud
```

#### Troubleshooting
If there are compilation errors related to template metadata or template plugin framework,
make sure the plugin dependencies are up-to-date by running:
```
mvn clean install -pl plugins/templates-maven-plugin,metadata -am
```
See [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#templates-plugin)
for more information.



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
cd v2/googlecloud-to-googlecloud/terraform/Stream_DLP_GCS_Text_to_BigQuery_Flex
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

resource "google_dataflow_flex_template_job" "stream_dlp_gcs_text_to_bigquery_flex" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Stream_DLP_GCS_Text_to_BigQuery_Flex"
  name              = "stream-dlp-gcs-text-to-bigquery-flex"
  region            = var.region
  parameters        = {
    inputFilePattern = "gs://your-bucket/your-files/*.csv"
    deidentifyTemplateName = "projects/your-project-id/locations/global/deidentifyTemplates/generated_template_id"
    datasetName = "<datasetName>"
    dlpProjectId = "<dlpProjectId>"
    # inspectTemplateName = "projects/your-project-id/locations/global/inspectTemplates/generated_template_id"
    # batchSize = "100"
    # useStorageWriteApi = "false"
    # useStorageWriteApiAtLeastOnce = "false"
    # numStorageWriteApiStreams = "0"
    # storageWriteApiTriggeringFrequencySec = "<storageWriteApiTriggeringFrequencySec>"
  }
}
```
