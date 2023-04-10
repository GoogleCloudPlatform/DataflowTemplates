Kinesis To Pubsub Template
---
A pipeline which sends Kinesis Datastream records into a Pubsub topic.

:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided-templates)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Kinesis_To_Pubsub).


:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required Parameters

* **secretId1** (First Secret ID): First Secret ID containing aws key id.
* **secretId2** (Second Secret ID): Second Secret ID containing aws key id.
* **awsRegion** (AWS Region): AWS Region.
* **kinesisDataStream** (Name of the Kinesis Data stream to read from): Name of the Kinesis Data stream to read from. Enter the full name of the Kinesis Data stream.
* **outputPubsubTopic** (Output Pub/Sub topic): The name of the topic to which data should published, in the format of 'projects/your-project-id/topics/your-topic-name' (Example: projects/your-project-id/topics/your-topic-name).

### Optional Parameters

* **awsDataFormat** (Data format of input): Data format of input.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!
[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=/v2/kinesis-to-pubsub/src/main/java/com/google/cloud/teleport/v2/templates/KinesisToPubsub.java)

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
-DtemplateName="Kinesis_To_Pubsub" \
-pl v2/kinesis-to-pubsub \
-am
```

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Kinesis_To_Pubsub
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Kinesis_To_Pubsub"

### Required
export SECRET_ID1=<secretId1>
export SECRET_ID2=<secretId2>
export AWS_REGION=<awsRegion>
export KINESIS_DATA_STREAM=<kinesisDataStream>
export OUTPUT_PUBSUB_TOPIC=<outputPubsubTopic>

### Optional
export AWS_DATA_FORMAT=<awsDataFormat>

gcloud dataflow flex-template run "kinesis-to-pubsub-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "secretId1=$SECRET_ID1" \
  --parameters "secretId2=$SECRET_ID2" \
  --parameters "awsRegion=$AWS_REGION" \
  --parameters "awsDataFormat=$AWS_DATA_FORMAT" \
  --parameters "kinesisDataStream=$KINESIS_DATA_STREAM" \
  --parameters "outputPubsubTopic=$OUTPUT_PUBSUB_TOPIC"
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
export SECRET_ID1=<secretId1>
export SECRET_ID2=<secretId2>
export AWS_REGION=<awsRegion>
export KINESIS_DATA_STREAM=<kinesisDataStream>
export OUTPUT_PUBSUB_TOPIC=<outputPubsubTopic>

### Optional
export AWS_DATA_FORMAT=<awsDataFormat>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="kinesis-to-pubsub-job" \
-DtemplateName="Kinesis_To_Pubsub" \
-Dparameters="secretId1=$SECRET_ID1,secretId2=$SECRET_ID2,awsRegion=$AWS_REGION,awsDataFormat=$AWS_DATA_FORMAT,kinesisDataStream=$KINESIS_DATA_STREAM,outputPubsubTopic=$OUTPUT_PUBSUB_TOPIC" \
-pl v2/kinesis-to-pubsub \
-am
```
