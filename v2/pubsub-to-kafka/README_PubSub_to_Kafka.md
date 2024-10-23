
Pub/Sub to Kafka template
---
A streaming pipeline which inserts data from a Pub/Sub Topic and transforms them
using a JavaScript user-defined function (UDF), and writes them to kafka topic.



:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **inputTopic** : The name of the topic from which data should published, in the format of 'projects/your-project-id/topics/your-topic-name' (Example: projects/your-project-id/topics/your-topic-name).
* **outputTopic** : Kafka topic to write the input from pubsub. (Example: topic).
* **outputDeadLetterTopic** : The Pub/Sub topic to publish deadletter records to. The name should be in the format of projects/your-project-id/topics/your-topic-name.

### Optional parameters

* **bootstrapServer** : Kafka Bootstrap Server  (Example: localhost:9092).
* **secretStoreUrl** : URL to credentials in Vault.
* **vaultToken** : Token to use for Vault.
* **javascriptTextTransformGcsPath** : The Cloud Storage URI of the .js file that defines the JavaScript user-defined function (UDF) to use. (Example: gs://my-bucket/my-udfs/my_file.js).
* **javascriptTextTransformFunctionName** : The name of the JavaScript user-defined function (UDF) to use. For example, if your JavaScript function code is `myTransform(inJson) { /*...do stuff...*/ }`, then the function name is `myTransform`. For sample JavaScript UDFs, see UDF Examples (https://github.com/GoogleCloudPlatform/DataflowTemplates#udf-examples).
* **javascriptTextTransformReloadIntervalMinutes** : Specifies how frequently to reload the UDF, in minutes. If the value is greater than 0, Dataflow periodically checks the UDF file in Cloud Storage, and reloads the UDF if the file is modified. This parameter allows you to update the UDF while the pipeline is running, without needing to restart the job. If the value is 0, UDF reloading is disabled. The default value is 0.


## User-Defined functions (UDFs)

The Pub/Sub to Kafka Template supports User-Defined functions (UDFs).
UDFs allow you to customize functionality by providing a JavaScript function
without having to maintain or build the entire template code.

Check [Create user-defined functions for Dataflow templates](https://cloud.google.com/dataflow/docs/guides/templates/create-template-udf)
and [Using UDFs](https://github.com/GoogleCloudPlatform/DataflowTemplates#using-udfs)
for more information about how to create and test those functions.


## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/pubsub-to-kafka/src/main/java/com/google/cloud/teleport/v2/templates/PubsubToKafka.java)

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#templates-plugin).

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
-DtemplateName="PubSub_to_Kafka" \
-f v2/pubsub-to-kafka
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/PubSub_to_Kafka
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/PubSub_to_Kafka"

### Required
export INPUT_TOPIC=<inputTopic>
export OUTPUT_TOPIC=<outputTopic>
export OUTPUT_DEAD_LETTER_TOPIC=<outputDeadLetterTopic>

### Optional
export BOOTSTRAP_SERVER=<bootstrapServer>
export SECRET_STORE_URL=<secretStoreUrl>
export VAULT_TOKEN=<vaultToken>
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>
export JAVASCRIPT_TEXT_TRANSFORM_RELOAD_INTERVAL_MINUTES=0

gcloud dataflow flex-template run "pubsub-to-kafka-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "inputTopic=$INPUT_TOPIC" \
  --parameters "bootstrapServer=$BOOTSTRAP_SERVER" \
  --parameters "outputTopic=$OUTPUT_TOPIC" \
  --parameters "outputDeadLetterTopic=$OUTPUT_DEAD_LETTER_TOPIC" \
  --parameters "secretStoreUrl=$SECRET_STORE_URL" \
  --parameters "vaultToken=$VAULT_TOKEN" \
  --parameters "javascriptTextTransformGcsPath=$JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH" \
  --parameters "javascriptTextTransformFunctionName=$JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME" \
  --parameters "javascriptTextTransformReloadIntervalMinutes=$JAVASCRIPT_TEXT_TRANSFORM_RELOAD_INTERVAL_MINUTES"
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
export OUTPUT_DEAD_LETTER_TOPIC=<outputDeadLetterTopic>

### Optional
export BOOTSTRAP_SERVER=<bootstrapServer>
export SECRET_STORE_URL=<secretStoreUrl>
export VAULT_TOKEN=<vaultToken>
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>
export JAVASCRIPT_TEXT_TRANSFORM_RELOAD_INTERVAL_MINUTES=0

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="pubsub-to-kafka-job" \
-DtemplateName="PubSub_to_Kafka" \
-Dparameters="inputTopic=$INPUT_TOPIC,bootstrapServer=$BOOTSTRAP_SERVER,outputTopic=$OUTPUT_TOPIC,outputDeadLetterTopic=$OUTPUT_DEAD_LETTER_TOPIC,secretStoreUrl=$SECRET_STORE_URL,vaultToken=$VAULT_TOKEN,javascriptTextTransformGcsPath=$JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH,javascriptTextTransformFunctionName=$JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME,javascriptTextTransformReloadIntervalMinutes=$JAVASCRIPT_TEXT_TRANSFORM_RELOAD_INTERVAL_MINUTES" \
-f v2/pubsub-to-kafka
```

#### Troubleshooting
If there are compilation errors related to template metadata or template plugin framework,
make sure the plugin dependencies are up-to-date by running:
```
mvn clean install -pl plugins/templates-maven-plugin,metadata -am
```
See [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#templates-plugin)
for more information.



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
cd v2/pubsub-to-kafka/terraform/PubSub_to_Kafka
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

resource "google_dataflow_flex_template_job" "pubsub_to_kafka" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/PubSub_to_Kafka"
  name              = "pubsub-to-kafka"
  region            = var.region
  parameters        = {
    inputTopic = "projects/your-project-id/topics/your-topic-name"
    outputTopic = "topic"
    outputDeadLetterTopic = "<outputDeadLetterTopic>"
    # bootstrapServer = "localhost:9092"
    # secretStoreUrl = "<secretStoreUrl>"
    # vaultToken = "<vaultToken>"
    # javascriptTextTransformGcsPath = "gs://my-bucket/my-udfs/my_file.js"
    # javascriptTextTransformFunctionName = "<javascriptTextTransformFunctionName>"
    # javascriptTextTransformReloadIntervalMinutes = "0"
  }
}
```
