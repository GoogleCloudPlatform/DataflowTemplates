
Cloud Storage Text File to Pub/Sub (Batch) template
---
This template creates a batch pipeline that reads records from text files stored
in Cloud Storage and publishes them to a Pub/Sub topic. The template can be used
to publish records in a newline-delimited file containing JSON records or CSV
file to a Pub/Sub topic for real-time processing. You can use this template to
replay data to Pub/Sub.

This template does not set any timestamp on the individual records. The event
time is equal to the publishing time during execution. If your pipeline relies on
an accurate event time for processing, you must not use this pipeline.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/cloud-storage-to-pubsub)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=GCS_Text_to_Cloud_PubSub).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required Parameters

* **inputFilePattern** (Cloud Storage Input File(s)): Path of the file pattern glob to read from. (Example: gs://your-bucket/path/*.txt).
* **outputTopic** (Output Pub/Sub topic): The name of the topic to which data should published, in the format of 'projects/your-project-id/topics/your-topic-name' (Example: projects/your-project-id/topics/your-topic-name).

### Optional Parameters




## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!
[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v1/src/main/java/com/google/cloud/teleport/templates/TextToPubsub.java)

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
-DtemplateName="GCS_Text_to_Cloud_PubSub" \
-pl v1 \
-am
```

The `-DgcpTempLocation=<temp-bucket-name>` parameter can be specified to set the GCS bucket used by the DataflowRunner to write
temp files to during serialization. The path used will be `gs://<temp-bucket-name>/temp/`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Classic Template was staged! gs://<bucket-name>/templates/GCS_Text_to_Cloud_PubSub
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/GCS_Text_to_Cloud_PubSub"

### Required
export INPUT_FILE_PATTERN=<inputFilePattern>
export OUTPUT_TOPIC=<outputTopic>

### Optional

gcloud dataflow jobs run "gcs-text-to-cloud-pubsub-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "inputFilePattern=$INPUT_FILE_PATTERN" \
  --parameters "outputTopic=$OUTPUT_TOPIC"
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
export OUTPUT_TOPIC=<outputTopic>

### Optional

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="gcs-text-to-cloud-pubsub-job" \
-DtemplateName="GCS_Text_to_Cloud_PubSub" \
-Dparameters="inputFilePattern=$INPUT_FILE_PATTERN,outputTopic=$OUTPUT_TOPIC" \
-pl v1 \
-am
```
