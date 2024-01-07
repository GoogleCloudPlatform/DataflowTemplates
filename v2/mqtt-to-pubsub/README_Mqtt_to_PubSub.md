
MQTT to Pubsub template
---
The MQTT to Pub/Sub template is a streaming pipeline that reads messages from an
MQTT topic and writes them to Pub/Sub. It includes the optional parameters
<code>username</code> and <code>password</code> in case authentication is
required by the MQTT server.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/mqtt-to-pubsub)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Mqtt_to_PubSub).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required Parameters

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
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/mqtt-to-pubsub/src/main/java/com/google/cloud/teleport/v2/templates/MqttToPubsub.java)

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates#templates-plugin).

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
-f v2/mqtt-to-pubsub
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

To start a job with the template at any time using `gcloud`, you are going to
need valid resources for the required parameters.

Provided that, the following command line can be used:

```shell
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>
export REGION=us-central1
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Mqtt_to_PubSub"

### Required
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

### Required
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
-f v2/mqtt-to-pubsub
```

## Terraform

Dataflow supports the utilization of Terraform to manage template jobs,
see [dataflow_flex_template_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_flex_template_job).

Here is an example of Terraform configuration:


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

resource "google_dataflow_flex_template_job" "mqtt_to_pubsub" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Mqtt_to_PubSub"
  name              = "mqtt-to-pubsub"
  region            = var.region
  parameters        = {
    inputTopic = "topic"
    outputTopic = "projects/your-project-id/topics/your-topic-name"
    username = "sampleusername"
    password = "samplepassword"
    # brokerServer = "tcp://host:1883"
  }
}
```
