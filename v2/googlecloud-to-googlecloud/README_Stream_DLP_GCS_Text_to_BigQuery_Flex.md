Data Masking/Tokenization from Cloud Storage to BigQuery (using Cloud DLP) Template
---
An example pipeline that reads CSV files from Cloud Storage, uses Cloud DLP API to mask and tokenize data based on the DLP templates provided and stores output in BigQuery. Note, not all configuration settings are available in this default template. You may need to deploy a custom template to accommodate your specific environment and data needs. More details here: https://cloud.google.com/solutions/de-identification-re-identification-pii-using-cloud-dlp.

:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided-templates)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Stream_DLP_GCS_Text_to_BigQuery_Flex).


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
* **useStorageWriteApi** (Use BigQuery Storage Write API): If enabled (set to true) the pipeline will use Storage Write API when writing the data to BigQuery (see https://cloud.google.com/blog/products/data-analytics/streaming-data-into-bigquery-using-storage-write-api). If this is enabled and at-least-once semantics (useStorageWriteApiAtLeastOnce) option is off then "Number of streams for BigQuery Storage Write API" and "Triggering frequency in seconds for BigQuery Storage Write API" must be provided. Defaults to: false.
* **useStorageWriteApiAtLeastOnce** (Use at at-least-once semantics in BigQuery Storage Write API): This parameter takes effect only if "Use BigQuery Storage Write API" is enabled. If enabled the at-least-once semantics will be used for Storage Write API, otherwise exactly-once semantics will be used. Defaults to: false.
* **numStorageWriteApiStreams** (Number of streams for BigQuery Storage Write API): Number of streams defines the parallelism of the BigQueryIO’s Write transform and roughly corresponds to the number of Storage Write API’s streams which will be used by the pipeline. See https://cloud.google.com/blog/products/data-analytics/streaming-data-into-bigquery-using-storage-write-api for the recommended values. Defaults to: 0.
* **storageWriteApiTriggeringFrequencySec** (Triggering frequency in seconds for BigQuery Storage Write API): Triggering frequency will determine how soon the data will be visible for querying in BigQuery. See https://cloud.google.com/blog/products/data-analytics/streaming-data-into-bigquery-using-storage-write-api for the recommended values.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!
[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=/v2/googlecloud-to-googlecloud/src/main/java/com/google/cloud/teleport/v2/templates/DLPTextToBigQueryStreaming.java)

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
-DtemplateName="Stream_DLP_GCS_Text_to_BigQuery_Flex" \
-pl v2/googlecloud-to-googlecloud \
-am
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
-pl v2/googlecloud-to-googlecloud \
-am
```
