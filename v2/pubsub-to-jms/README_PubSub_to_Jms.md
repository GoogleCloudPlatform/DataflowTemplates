Pubsub to Jms Template
---
A pipeline to extract data from Pubsub Subscription and write to JMS Server(Queue/Topic).

:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided-templates)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=PubSub_to_JMS).


:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required Parameters

* **inputSubscription** (Input Pub/Sub Subscription): The name of the Subscription from which data should be read, in the format of 'projects/your-project-id/subscriptions/your-subscription-name' (Example: projects/your-project-id/subscriptions/your-subscription-name).
* **jmsServer** jmsServer: JMS (ActiveMQ) Server IP. For example-tcp://<ActiveMQ-Host>:<PORT>
* **outputName** (JMS(ActiveMQ) queue/topic to write the input to): JMS Queue/Topic to write the input to. (Example: queue1/topic1).
* **outputType** (JMS(ActiveMQ)  Destination to write, can be queue or topic.

### Optional Parameters
* **username** : username for authentication with JMS server (Example: sampleusername).
* **password** : Password for username provided for authentication with JMS server (Example: samplepassword).



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
    * `gcloud auth login`
    * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!
[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=/v2/pubsub-to-jms/src/main/java/com/google/cloud/teleport/v2/templates/PubsubToJms.java)

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
-DtemplateName="PubSub_to_Jms" \
-pl v2/pubsub-to-jms \
-am
```

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/PubSub_to_Jms
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/PubSub_to_Jms"

### Required
export INPUT_SUBSCRIPTION=<inputSubscription>
export JMS_SERVER=<jmsServer>
export OUTPUT_NAME=<outputName>
export OUTPUT_TYPE=<outputType>



### Optional
export USERNAME=<username>
export PASSWORD=<password>

gcloud dataflow flex-template run "pubsub-to-jms-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "inputSubscription=$INPUT_SUBSCRIPTION" \
  --parameters "brokerServer=$BROKER_SERVER" \
  --parameters "outputName=$OUTPUT_NAME" \
  --parameters "outputType=$OUTPUT_TYPE" \
  --parameters "username=$USERNAME" \
  --parameters "password=$PASSWORD"
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
export INPUT_SUBSCRIPTION=<inputSubscription>
export JMS_SERVER=<jmsServer>
export OUTPUT_NAME=<outputName>
export OUTPUT_TYPE=<outputType>



### Optional
export USERNAME=<username>
export PASSWORD=<password>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="pubsub-to-jms-job" \
-DtemplateName="PubSub_to_Jms" \
-Dparameters="inputSubscription=$INPUT_SUBSCRIPTION,jmsServer=$JMS_SERVER,outputName=$OUTPUT_NAME,outputType=$OUTPUT_TYPE,username=$USERNAME,password=$PASSWORD" \
-pl v2/pubsub-to-jms \
-am
```
