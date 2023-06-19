BigQuery to TensorFlow Records Template
---
A pipeline that reads rows from BigQuery and writes them as TFRecords in Cloud Storage.

:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/bigquery-to-tfrecords)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Cloud_BigQuery_to_GCS_TensorFlow_Records).

Note: Nested BigQuery columns are currently not supported and should be unnested within the SQL query.

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required Parameters

* **readQuery** (Input SQL query): SQL query in standard SQL to pull data from BigQuery.
* **outputDirectory** (Output Cloud Storage directory.): Cloud Storage directory to store output TFRecord files. (Example: gs://your-bucket/your-path).

### Optional Parameters

* **readIdColumn** (Unique identifier column): Name of the BigQuery column storing the unique identifier of the row.
* **invalidOutputPath** (Invalid rows output path): Cloud Storage path where to write BigQuery rows that cannot be converted to target entities. (Example: gs://your-bucket/your-path).
* **outputSuffix** (The output suffix for TFRecord files): File suffix to append to TFRecord files. Defaults to .tfrecord.
* **trainingPercentage** (Percentage of data to be in the training set ): Defaults to 1 or 100%. Should be decimal between 0 and 1 inclusive.
* **testingPercentage** (Percentage of data to be in the testing set ): Defaults to 0 or 0%. Should be decimal between 0 and 1 inclusive.
* **validationPercentage** (Percentage of data to be in the validation set ): Defaults to 0 or 0%. Should be decimal between 0 and 1 inclusive.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!
[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=/v1/src/main/java/com/google/cloud/teleport/templates/BigQueryToTFRecord.java)

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
-DtemplateName="Cloud_BigQuery_to_GCS_TensorFlow_Records" \
-pl v1 \
-am
```

The `-DgcpTempLocation=<temp-bucket-name>` parameter can be specified to set the GCS bucket used by the DataflowRunner to write
temp files to during serialization. The path used will be `gs://<temp-bucket-name>/temp/`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Classic Template was staged! gs://<bucket-name>/templates/Cloud_BigQuery_to_GCS_TensorFlow_Records
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/Cloud_BigQuery_to_GCS_TensorFlow_Records"

### Required
export READ_QUERY=<readQuery>
export OUTPUT_DIRECTORY=<outputDirectory>

### Optional
export READ_ID_COLUMN=<readIdColumn>
export INVALID_OUTPUT_PATH=<invalidOutputPath>
export OUTPUT_SUFFIX=".tfrecord"
export TRAINING_PERCENTAGE=1
export TESTING_PERCENTAGE=0
export VALIDATION_PERCENTAGE=0

gcloud dataflow jobs run "cloud-bigquery-to-gcs-tensorflow-records-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "readQuery=$READ_QUERY" \
  --parameters "readIdColumn=$READ_ID_COLUMN" \
  --parameters "invalidOutputPath=$INVALID_OUTPUT_PATH" \
  --parameters "outputDirectory=$OUTPUT_DIRECTORY" \
  --parameters "outputSuffix=$OUTPUT_SUFFIX" \
  --parameters "trainingPercentage=$TRAINING_PERCENTAGE" \
  --parameters "testingPercentage=$TESTING_PERCENTAGE" \
  --parameters "validationPercentage=$VALIDATION_PERCENTAGE"
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
export READ_QUERY=<readQuery>
export OUTPUT_DIRECTORY=<outputDirectory>

### Optional
export READ_ID_COLUMN=<readIdColumn>
export INVALID_OUTPUT_PATH=<invalidOutputPath>
export OUTPUT_SUFFIX=".tfrecord"
export TRAINING_PERCENTAGE=1
export TESTING_PERCENTAGE=0
export VALIDATION_PERCENTAGE=0

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="cloud-bigquery-to-gcs-tensorflow-records-job" \
-DtemplateName="Cloud_BigQuery_to_GCS_TensorFlow_Records" \
-Dparameters="readQuery=$READ_QUERY,readIdColumn=$READ_ID_COLUMN,invalidOutputPath=$INVALID_OUTPUT_PATH,outputDirectory=$OUTPUT_DIRECTORY,outputSuffix=$OUTPUT_SUFFIX,trainingPercentage=$TRAINING_PERCENTAGE,testingPercentage=$TESTING_PERCENTAGE,validationPercentage=$VALIDATION_PERCENTAGE" \
-pl v1 \
-am
```
