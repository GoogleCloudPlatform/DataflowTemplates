Mqtt to Pubsub Template
---
A pipeline to extract from Mqtt Broker Server to Pubsub Topic.

:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided-templates)
on how to use it without having to build from sources.

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Mandatory Parameters

* **inputTopic** (MQTT topic(s) to read the input from): MQTT topic(s) to read the input from. (Example: topic).
* **outputTopic** (Output Pub/Sub topic): The name of the topic to which data should published, in the format of 'projects/your-project-id/topics/your-topic-name' (Example: projects/your-project-id/topics/your-topic-name).
* **username** (MQTT Username): MQTT username for authentication with MQTT server (Example: sampleusername).
* **password** (MQTT Password): Password for username provided for authentication with MQTT server (Example: samplepassword).

### Optional Parameters

* **brokerServer** (MQTT Broker IP): Server IP for MQTT broker (Example: tcp://host:1883).

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
-DtemplateName="Mqtt_to_PubSub" \
-pl v2/mqtt-to-pubsub \
-am
```

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Mqtt_to_PubSub
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Mqtt_to_PubSub"

### Mandatory
export INPUT_TOPIC=<inputTopic>
export OUTPUT_TOPIC=<outputTopic>
export USERNAME=<username>
export PASSWORD=<password>

### Optional
export BROKER_SERVER=<brokerServer>

gcloud dataflow flex-template run "mqtt-to-pubsub-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "brokerServer=$BROKER_SERVER" \
  --parameters "inputTopic=$INPUT_TOPIC" \
  --parameters "outputTopic=$OUTPUT_TOPIC" \
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

### Mandatory
export INPUT_TOPIC=<inputTopic>
export OUTPUT_TOPIC=<outputTopic>
export USERNAME=<username>
export PASSWORD=<password>

### Optional
export BROKER_SERVER=<brokerServer>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="mqtt-to-pubsub-job" \
-DtemplateName="Mqtt_to_PubSub" \
-Dparameters="brokerServer=$BROKER_SERVER,inputTopic=$INPUT_TOPIC,outputTopic=$OUTPUT_TOPIC,username=$USERNAME,password=$PASSWORD" \
-pl v2/mqtt-to-pubsub \
-am
```
