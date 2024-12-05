
Azure Eventhub to Pubsub template
---
A pipeline to extract from Azure Event hub Server to Cloud Pub/Sub Topic.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/azure-eventhub-to-pubsub)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Azure_Eventhub_to_PubSub).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **brokerServer**: Server IP or DNS for Azure Eventhub Endpoint For example, `mynamespace.servicebus.windows.net:9093`.
* **inputTopic**: Azure Eventhub topic(s) to read the input from For example, `topic`.
* **outputTopic**: The name of the topic to which data should published, in the format of 'projects/your-project-id/topics/your-topic-name' For example, `projects/your-project-id/topics/your-topic-name`.
* **secret**: Secret Version, it can be a number like 1,2 or 3 or can be 'latest' For example, `projects/{project}/secrets/{secret}/versions/{secret_version}`.

### Optional parameters




## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/azure-eventhub-to-pubsub/src/main/java/com/google/cloud/teleport/v2/templates/AzureEventhubToPubsub.java)

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
-DtemplateName="Azure_Eventhub_to_PubSub" \
-f v2/azure-eventhub-to-pubsub
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Azure_Eventhub_to_PubSub
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Azure_Eventhub_to_PubSub"

### Required
export BROKER_SERVER=<brokerServer>
export INPUT_TOPIC=<inputTopic>
export OUTPUT_TOPIC=<outputTopic>
export SECRET=<secret>

### Optional

gcloud dataflow flex-template run "azure-eventhub-to-pubsub-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "brokerServer=$BROKER_SERVER" \
  --parameters "inputTopic=$INPUT_TOPIC" \
  --parameters "outputTopic=$OUTPUT_TOPIC" \
  --parameters "secret=$SECRET"
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
export BROKER_SERVER=<brokerServer>
export INPUT_TOPIC=<inputTopic>
export OUTPUT_TOPIC=<outputTopic>
export SECRET=<secret>

### Optional

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="azure-eventhub-to-pubsub-job" \
-DtemplateName="Azure_Eventhub_to_PubSub" \
-Dparameters="brokerServer=$BROKER_SERVER,inputTopic=$INPUT_TOPIC,outputTopic=$OUTPUT_TOPIC,secret=$SECRET" \
-f v2/azure-eventhub-to-pubsub
```

## Terraform

Dataflow supports the utilization of Terraform to manage template jobs,
see [dataflow_flex_template_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_flex_template_job).

Terraform modules have been generated for most templates in this repository. This includes the relevant parameters
specific to the template. If available, they may be used instead of
[dataflow_flex_template_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_flex_template_job)
directly.

To use the autogenerated module, execute the standard
[terraform workflow](https://developer.hashicorp.com/terraform/intro/core-workflow):

```shell
cd v2/azure-eventhub-to-pubsub/terraform/Azure_Eventhub_to_PubSub
terraform init
terraform apply
```

To use
[dataflow_flex_template_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_flex_template_job)
directly:

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

resource "google_dataflow_flex_template_job" "azure_eventhub_to_pubsub" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Azure_Eventhub_to_PubSub"
  name              = "azure-eventhub-to-pubsub"
  region            = var.region
  parameters        = {
    brokerServer = "<brokerServer>"
    inputTopic = "<inputTopic>"
    outputTopic = "<outputTopic>"
    secret = "<secret>"
  }
}
```
