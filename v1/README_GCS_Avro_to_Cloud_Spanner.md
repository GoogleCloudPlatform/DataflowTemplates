
Avro Files on Cloud Storage to Cloud Spanner template
---
The Cloud Storage Avro files to Cloud Spanner template is a batch pipeline that
reads Avro files exported from Cloud Spanner stored in Cloud Storage and imports
them to a Cloud Spanner database.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/avro-to-cloud-spanner)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=GCS_Avro_to_Cloud_Spanner).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required Parameters

* **instanceId** (Cloud Spanner instance id): The instance id of the Cloud Spanner database that you want to import to.
* **databaseId** (Cloud Spanner database id): The database id of the Cloud Spanner database that you want to import into (must already exist).
* **inputDir** (Cloud storage input directory): The Cloud Storage path where the Avro files should be imported from.

### Optional Parameters

* **spannerHost** (Cloud Spanner Endpoint to call): The Cloud Spanner endpoint to call in the template. Only used for testing. (Example: https://batch-spanner.googleapis.com). Defaults to: https://batch-spanner.googleapis.com.
* **waitForIndexes** (Wait for Indexes): By default the import pipeline is not blocked on index creation, and it may complete with indexes still being created in the background. If true, the pipeline waits until indexes are created.
* **waitForForeignKeys** (Wait for Foreign Keys): By default the import pipeline is not blocked on foreign key creation, and it may complete with foreign keys still being created in the background. If true, the pipeline waits until foreign keys are created.
* **waitForChangeStreams** (Wait for Change Streams): By default the import pipeline is blocked on change stream creation. If false, it may complete with change streams still being created in the background.
* **waitForSequences** (Wait for Sequences): By default the import pipeline is blocked on sequence creation. If false, it may complete with sequences still being created in the background.
* **earlyIndexCreateFlag** (Create Indexes early): Flag to turn off early index creation if there are many indexes. Indexes and Foreign keys are created after dataload. If there are more than 40 DDL statements to be executed after dataload, it is preferable to create the indexes before datalod. This is the flag to turn the feature off. Defaults to: true.
* **spannerProjectId** (Cloud Spanner Project Id): The project id of the Cloud Spanner instance.
* **ddlCreationTimeoutInMinutes** (DDL Creation timeout in minutes): DDL Creation timeout in minutes. Defaults to: 30.
* **spannerPriority** (Priority for Spanner RPC invocations): The request priority for Cloud Spanner calls. The value must be one of: [HIGH,MEDIUM,LOW].



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!
[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v1/src/main/java/com/google/cloud/teleport/spanner/ImportPipeline.java)

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
-DtemplateName="GCS_Avro_to_Cloud_Spanner" \
-pl v1 \
-am
```

The `-DgcpTempLocation=<temp-bucket-name>` parameter can be specified to set the GCS bucket used by the DataflowRunner to write
temp files to during serialization. The path used will be `gs://<temp-bucket-name>/temp/`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Classic Template was staged! gs://<bucket-name>/templates/GCS_Avro_to_Cloud_Spanner
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/GCS_Avro_to_Cloud_Spanner"

### Required
export INSTANCE_ID=<instanceId>
export DATABASE_ID=<databaseId>
export INPUT_DIR=<inputDir>

### Optional
export SPANNER_HOST=https://batch-spanner.googleapis.com
export WAIT_FOR_INDEXES=false
export WAIT_FOR_FOREIGN_KEYS=false
export WAIT_FOR_CHANGE_STREAMS=true
export WAIT_FOR_SEQUENCES=true
export EARLY_INDEX_CREATE_FLAG=true
export SPANNER_PROJECT_ID=<spannerProjectId>
export DDL_CREATION_TIMEOUT_IN_MINUTES=30
export SPANNER_PRIORITY=<spannerPriority>

gcloud dataflow jobs run "gcs-avro-to-cloud-spanner-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "instanceId=$INSTANCE_ID" \
  --parameters "databaseId=$DATABASE_ID" \
  --parameters "inputDir=$INPUT_DIR" \
  --parameters "spannerHost=$SPANNER_HOST" \
  --parameters "waitForIndexes=$WAIT_FOR_INDEXES" \
  --parameters "waitForForeignKeys=$WAIT_FOR_FOREIGN_KEYS" \
  --parameters "waitForChangeStreams=$WAIT_FOR_CHANGE_STREAMS" \
  --parameters "waitForSequences=$WAIT_FOR_SEQUENCES" \
  --parameters "earlyIndexCreateFlag=$EARLY_INDEX_CREATE_FLAG" \
  --parameters "spannerProjectId=$SPANNER_PROJECT_ID" \
  --parameters "ddlCreationTimeoutInMinutes=$DDL_CREATION_TIMEOUT_IN_MINUTES" \
  --parameters "spannerPriority=$SPANNER_PRIORITY"
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
export INPUT_DIR=<inputDir>

### Optional
export SPANNER_HOST=https://batch-spanner.googleapis.com
export WAIT_FOR_INDEXES=false
export WAIT_FOR_FOREIGN_KEYS=false
export WAIT_FOR_CHANGE_STREAMS=true
export WAIT_FOR_SEQUENCES=true
export EARLY_INDEX_CREATE_FLAG=true
export SPANNER_PROJECT_ID=<spannerProjectId>
export DDL_CREATION_TIMEOUT_IN_MINUTES=30
export SPANNER_PRIORITY=<spannerPriority>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="gcs-avro-to-cloud-spanner-job" \
-DtemplateName="GCS_Avro_to_Cloud_Spanner" \
-Dparameters="instanceId=$INSTANCE_ID,databaseId=$DATABASE_ID,inputDir=$INPUT_DIR,spannerHost=$SPANNER_HOST,waitForIndexes=$WAIT_FOR_INDEXES,waitForForeignKeys=$WAIT_FOR_FOREIGN_KEYS,waitForChangeStreams=$WAIT_FOR_CHANGE_STREAMS,waitForSequences=$WAIT_FOR_SEQUENCES,earlyIndexCreateFlag=$EARLY_INDEX_CREATE_FLAG,spannerProjectId=$SPANNER_PROJECT_ID,ddlCreationTimeoutInMinutes=$DDL_CREATION_TIMEOUT_IN_MINUTES,spannerPriority=$SPANNER_PRIORITY" \
-pl v1 \
-am
```
