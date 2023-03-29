Avro Files on Cloud Storage to Cloud Bigtable Template
---
A pipeline which reads data from Avro files in Cloud Storage and writes it to Cloud Bigtable table.

:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/avro-to-bigtable)
on how to use it without having to build from sources.

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Mandatory Parameters

* **bigtableProjectId** (Project ID): The ID of the Google Cloud project of the Cloud Bigtable instance that you want to write data to.
* **bigtableInstanceId** (Instance ID): The ID of the Cloud Bigtable instance that contains the table.
* **bigtableTableId** (Table ID): The ID of the Cloud Bigtable table to write.
* **inputFilePattern** (Input Cloud Storage File(s)): The Cloud Storage location of the files you'd like to process. (Example: gs://your-bucket/your-files/*.avro).

### Optional Parameters

* **splitLargeRows** (If true, large rows will be split into multiple MutateRows requests): The flag for enabling splitting of large rows into multiple MutateRows requests. Note that when a large row is split between multiple API calls, the updates to the row are not atomic. .

## Getting Started

### Requirements

* Java 11
* Maven
* Valid resources for mandatory parameters.
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
    * `gcloud auth login`
    * `gcloud auth application-default login`

The following instructions use the
[Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates#templates-plugin)
. Install the plugin with the following command to proceed:

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
-DtemplateName="GCS_Avro_to_Cloud_Bigtable" \
-pl v1 \
-am
```

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Classic Template was staged! gs://<bucket-name>/templates/GCS_Avro_to_Cloud_Bigtable
```

The specific path should be copied as it will be used in the following steps.

#### Running the Template

**Using the staged template**:

You can use the path above run the template (or share with others for execution).

To start a job with that template at any time using `gcloud`, you can use:

```shell
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>
export REGION=us-central1
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/GCS_Avro_to_Cloud_Bigtable"

### Mandatory
export BIGTABLE_PROJECT_ID=<bigtableProjectId>
export BIGTABLE_INSTANCE_ID=<bigtableInstanceId>
export BIGTABLE_TABLE_ID=<bigtableTableId>
export INPUT_FILE_PATTERN=<inputFilePattern>

### Optional
export SPLIT_LARGE_ROWS=<splitLargeRows>

gcloud dataflow jobs run "gcs-avro-to-cloud-bigtable-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "bigtableProjectId=$BIGTABLE_PROJECT_ID" \
  --parameters "bigtableInstanceId=$BIGTABLE_INSTANCE_ID" \
  --parameters "bigtableTableId=$BIGTABLE_TABLE_ID" \
  --parameters "inputFilePattern=$INPUT_FILE_PATTERN" \
  --parameters "splitLargeRows=$SPLIT_LARGE_ROWS"
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

### Mandatory
export BIGTABLE_PROJECT_ID=<bigtableProjectId>
export BIGTABLE_INSTANCE_ID=<bigtableInstanceId>
export BIGTABLE_TABLE_ID=<bigtableTableId>
export INPUT_FILE_PATTERN=<inputFilePattern>

### Optional
export SPLIT_LARGE_ROWS=<splitLargeRows>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="gcs-avro-to-cloud-bigtable-job" \
-DtemplateName="GCS_Avro_to_Cloud_Bigtable" \
-Dparameters="bigtableProjectId=$BIGTABLE_PROJECT_ID,bigtableInstanceId=$BIGTABLE_INSTANCE_ID,bigtableTableId=$BIGTABLE_TABLE_ID,inputFilePattern=$INPUT_FILE_PATTERN,splitLargeRows=$SPLIT_LARGE_ROWS" \
-pl v1 \
-am
```
