
Pubsub to JMS template
---
A streaming pipeline which inserts data from a Pubsub Subscription and writes to
JMS Broker Server Topic or Queue.



:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required Parameters

* **inputSubscription** (Pub/Sub input subscription): Pub/Sub subscription to read the input from, in the format of 'projects/your-project-id/subscriptions/your-subscription-name' (Example: projects/your-project-id/subscriptions/your-subscription-name).
* **outputName** (JMS Queue/Topic Name to write the input to): JMS Queue/Topic Name to write the input to. (Example: queue).
* **outputType** (JMS Destination Type to Write the input to): JMS Destination Type to Write the input to. (Example: queue).
* **username** (JMS Username): JMS username for authentication with JMS server (Example: sampleusername).
* **password** (JMS Password): Password for username provided for authentication with JMS server (Example: samplepassword).

### Optional Parameters

* **jmsServer** (JMS Host IP): Server IP for JMS Host (Example: host:5672).



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/pubsub-to-jms/src/main/java/com/google/cloud/teleport/v2/templates/PubsubToJms.java)

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
-DtemplateName="Pubsub_to_Jms" \
-f v2/pubsub-to-jms
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Pubsub_to_Jms
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Pubsub_to_Jms"

### Required
export INPUT_SUBSCRIPTION=<inputSubscription>
export OUTPUT_NAME=<outputName>
export OUTPUT_TYPE=<outputType>
export USERNAME=<username>
export PASSWORD=<password>

### Optional
export JMS_SERVER=<jmsServer>

gcloud dataflow flex-template run "pubsub-to-jms-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "inputSubscription=$INPUT_SUBSCRIPTION" \
  --parameters "jmsServer=$JMS_SERVER" \
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
export OUTPUT_NAME=<outputName>
export OUTPUT_TYPE=<outputType>
export USERNAME=<username>
export PASSWORD=<password>

### Optional
export JMS_SERVER=<jmsServer>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="pubsub-to-jms-job" \
-DtemplateName="Pubsub_to_Jms" \
-Dparameters="inputSubscription=$INPUT_SUBSCRIPTION,jmsServer=$JMS_SERVER,outputName=$OUTPUT_NAME,outputType=$OUTPUT_TYPE,username=$USERNAME,password=$PASSWORD" \
-f v2/pubsub-to-jms
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

resource "google_dataflow_flex_template_job" "pubsub_to_jms" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Pubsub_to_Jms"
  name              = "pubsub-to-jms"
  region            = var.region
  parameters        = {
    inputSubscription = "projects/your-project-id/subscriptions/your-subscription-name"
    outputName = "queue"
    outputType = "queue"
    username = "sampleusername"
    password = "samplepassword"
    # jmsServer = "host:5672"
  }
}
```
