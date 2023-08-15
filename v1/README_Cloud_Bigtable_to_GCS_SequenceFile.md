Cloud Bigtable to SequenceFile Files on Cloud Storage Template
---
A pipeline which reads in Cloud Bigtable table and writes it to Cloud Storage in SequenceFile format.

:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/bigtable-to-sequencefile)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Cloud_Bigtable_to_GCS_SequenceFile).


:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required Parameters

* **bigtableProject** (Project ID): The ID of the Google Cloud project of the Cloud Bigtable instance that you want to read data from. Defaults to job project.
* **bigtableInstanceId** (Instance ID): The ID of the Cloud Bigtable instance that contains the table.
* **bigtableTableId** (Table ID): The ID of the Cloud Bigtable table to export.
* **destinationPath** (Destination path): Cloud Storage path where data should be written. (Example: gs://your-bucket/your-path/).
* **filenamePrefix** (SequenceFile prefix): The prefix for each shard in destinationPath. (Example: output-). Defaults to: part.

### Optional Parameters

* **bigtableAppProfileId** (Application profile ID): The ID of the Cloud Bigtable application profile to be used for the export.
* **bigtableStartRow** (Bigtable Start Row): The row where to start the export from, defaults to the first row.
* **bigtableStopRow** (Bigtable Stop Row): The row where to stop the export, defaults to the last row.
* **bigtableMaxVersions** (Bigtable Max Versions): Maximum number of cell versions. Defaults to: 2147483647.
* **bigtableFilter** (Bigtable Filter): Filter string. See: http://hbase.apache.org/book.html#thrift. Defaults to empty.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!
[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=/v1/src/main/java/com/google/cloud/bigtable/beam/sequencefiles/ExportJob.java)

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
-DtemplateName="Cloud_Bigtable_to_GCS_SequenceFile" \
-pl v1 \
-am
```

The `-DgcpTempLocation=<temp-bucket-name>` parameter can be specified to set the GCS bucket used by the DataflowRunner to write
temp files to during serialization. The path used will be `gs://<temp-bucket-name>/temp/`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Classic Template was staged! gs://<bucket-name>/templates/Cloud_Bigtable_to_GCS_SequenceFile
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/Cloud_Bigtable_to_GCS_SequenceFile"

### Required
export BIGTABLE_PROJECT=<bigtableProject>
export BIGTABLE_INSTANCE_ID=<bigtableInstanceId>
export BIGTABLE_TABLE_ID=<bigtableTableId>
export DESTINATION_PATH=<destinationPath>
export FILENAME_PREFIX="part"

### Optional
export BIGTABLE_APP_PROFILE_ID=<bigtableAppProfileId>
export BIGTABLE_START_ROW=""
export BIGTABLE_STOP_ROW=""
export BIGTABLE_MAX_VERSIONS="2147483647"
export BIGTABLE_FILTER=""

gcloud dataflow jobs run "cloud-bigtable-to-gcs-sequencefile-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "bigtableProject=$BIGTABLE_PROJECT" \
  --parameters "bigtableInstanceId=$BIGTABLE_INSTANCE_ID" \
  --parameters "bigtableTableId=$BIGTABLE_TABLE_ID" \
  --parameters "bigtableAppProfileId=$BIGTABLE_APP_PROFILE_ID" \
  --parameters "bigtableStartRow=$BIGTABLE_START_ROW" \
  --parameters "bigtableStopRow=$BIGTABLE_STOP_ROW" \
  --parameters "bigtableMaxVersions=$BIGTABLE_MAX_VERSIONS" \
  --parameters "bigtableFilter=$BIGTABLE_FILTER" \
  --parameters "destinationPath=$DESTINATION_PATH" \
  --parameters "filenamePrefix=$FILENAME_PREFIX"
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
export BIGTABLE_PROJECT=<bigtableProject>
export BIGTABLE_INSTANCE_ID=<bigtableInstanceId>
export BIGTABLE_TABLE_ID=<bigtableTableId>
export DESTINATION_PATH=<destinationPath>
export FILENAME_PREFIX="part"

### Optional
export BIGTABLE_APP_PROFILE_ID=<bigtableAppProfileId>
export BIGTABLE_START_ROW=""
export BIGTABLE_STOP_ROW=""
export BIGTABLE_MAX_VERSIONS="2147483647"
export BIGTABLE_FILTER=""

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="cloud-bigtable-to-gcs-sequencefile-job" \
-DtemplateName="Cloud_Bigtable_to_GCS_SequenceFile" \
-Dparameters="bigtableProject=$BIGTABLE_PROJECT,bigtableInstanceId=$BIGTABLE_INSTANCE_ID,bigtableTableId=$BIGTABLE_TABLE_ID,bigtableAppProfileId=$BIGTABLE_APP_PROFILE_ID,bigtableStartRow=$BIGTABLE_START_ROW,bigtableStopRow=$BIGTABLE_STOP_ROW,bigtableMaxVersions=$BIGTABLE_MAX_VERSIONS,bigtableFilter=$BIGTABLE_FILTER,destinationPath=$DESTINATION_PATH,filenamePrefix=$FILENAME_PREFIX" \
-pl v1 \
-am
```
