
Pub/Sub subscription to Pub/Sub topic (YAML) template
---
The Pub/Sub subscription to Pub/Sub topic template is a streaming pipeline that
reads messages from a Pub/Sub subscription and writes them to another Pub/Sub
topic.



:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **subscription**: Pub/Sub subscription to read the input from. For example, `projects/your-project-id/subscriptions/your-subscription-name`.
* **outputTopic**: The Pub/Sub topic to write the output to. For example, `projects/your-project-id/topics/your-topic-name`.

### Optional parameters

* **format**: The message format. One of: AVRO, JSON, PROTO, RAW, or STRING. For example, `JSON`. Defaults to: JSON.
* **schema**: A schema is required if data format is JSON, AVRO or PROTO. For JSON, this is a JSON schema. For AVRO and PROTO, this is the full schema definition. For example, `{"type": "object", "properties": {"field1": {"type": "string"}}}`.
* **attributes**: List of attribute keys whose values will be flattened into the output message as additional fields. For example, if the format is `raw` and attributes is `[a, b]` then this read will produce elements of the form `Row(payload=..., a=..., b=...)`. For example, `["attr1", "attr2"]`.
* **attributesMap**: Name of a field in which to store the full set of attributes associated with this message. For example, if the format is `raw` and `attributes_map` is set to `attrs` then this read will produce elements of the form `Row(payload=..., attrs=...)` where `attrs` is a Map type of string to string. If both `attributes` and `attributes_map` are set, the overlapping attribute values will be present in both the flattened structure and the attribute map. For example, `attrs`.
* **idAttribute**: The attribute on incoming Pub/Sub messages to use as a unique record identifier. When specified, the value of this attribute (which can be any string that uniquely identifies the record) will be used for deduplication of messages. If not provided, we cannot guarantee that no duplicate data will be delivered on the Pub/Sub stream. In this case, deduplication of the stream will be strictly best effort. For example, `id`.
* **timestampAttribute**: Message value to use as element timestamp. If None, uses message publishing time as the timestamp. For example, `timestamp`.
* **publishTimeField**: Field to add to output messages with the Pub/Sub message publish time. If None, no such field is added. For example, `publish_time`.
* **outputFormat**: The output message format. One of: AVRO, JSON, PROTO, RAW, or STRING. For example, `JSON`. Defaults to: JSON.
* **outputSchema**: Schema specification for the output format, if applicable. For example, `{"type": "object", "properties": {"field1": {"type": "string"}}}`.
* **outputAttributes**: List of attribute keys whose values will be pulled out as Pub/Sub message attributes. For example, `["attr1", "attr2"]`.
* **outputAttributesMap**: Name of a string-to-string map field in which to pull a set of attributes associated with this message. For example, `attrs`.
* **outputIdAttribute**: If set, will set an attribute for each Cloud Pub/Sub message with the given name and a unique value. For example, `id`.
* **outputTimestampAttribute**: If set, will set an attribute for each Cloud Pub/Sub message with the given name and the message's publish time as the value. For example, `timestamp`.



## Getting Started

### Requirements

* Java 17
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=yaml/src/main/java/com/google/cloud/teleport/templates/yaml/PubSubSubscriptionToPubSubTopicYaml.java)

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#templates-plugin).

#### Validating the Template

This template has a validation command that is used to check code quality.

```shell
mvn clean install -PtemplatesValidate \
-DskipTests -am \
-pl yaml
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
export ARTIFACT_REGISTRY_REPO=<region>-docker.pkg.dev/$PROJECT/<repo>

mvn clean package -PtemplatesStage  \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-DartifactRegistry="$ARTIFACT_REGISTRY_REPO" \
-DstagePrefix="templates" \
-DtemplateName="PubSub_Subscription_to_PubSub_Topic_Yaml" \
-f yaml
```

The `-DartifactRegistry` parameter can be specified to set the artifact registry repository of the Flex Templates image.
If not provided, it defaults to `gcr.io/<project>`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/PubSub_Subscription_to_PubSub_Topic_Yaml
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/PubSub_Subscription_to_PubSub_Topic_Yaml"

### Required
export SUBSCRIPTION=<subscription>
export OUTPUT_TOPIC=<outputTopic>

### Optional
export FORMAT=JSON
export SCHEMA=<schema>
export ATTRIBUTES=<attributes>
export ATTRIBUTES_MAP=<attributesMap>
export ID_ATTRIBUTE=<idAttribute>
export TIMESTAMP_ATTRIBUTE=<timestampAttribute>
export PUBLISH_TIME_FIELD=<publishTimeField>
export OUTPUT_FORMAT=JSON
export OUTPUT_SCHEMA=<outputSchema>
export OUTPUT_ATTRIBUTES=<outputAttributes>
export OUTPUT_ATTRIBUTES_MAP=<outputAttributesMap>
export OUTPUT_ID_ATTRIBUTE=<outputIdAttribute>
export OUTPUT_TIMESTAMP_ATTRIBUTE=<outputTimestampAttribute>

gcloud dataflow flex-template run "pubsub-subscription-to-pubsub-topic-yaml-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "subscription=$SUBSCRIPTION" \
  --parameters "format=$FORMAT" \
  --parameters "schema=$SCHEMA" \
  --parameters "attributes=$ATTRIBUTES" \
  --parameters "attributesMap=$ATTRIBUTES_MAP" \
  --parameters "idAttribute=$ID_ATTRIBUTE" \
  --parameters "timestampAttribute=$TIMESTAMP_ATTRIBUTE" \
  --parameters "publishTimeField=$PUBLISH_TIME_FIELD" \
  --parameters "outputTopic=$OUTPUT_TOPIC" \
  --parameters "outputFormat=$OUTPUT_FORMAT" \
  --parameters "outputSchema=$OUTPUT_SCHEMA" \
  --parameters "outputAttributes=$OUTPUT_ATTRIBUTES" \
  --parameters "outputAttributesMap=$OUTPUT_ATTRIBUTES_MAP" \
  --parameters "outputIdAttribute=$OUTPUT_ID_ATTRIBUTE" \
  --parameters "outputTimestampAttribute=$OUTPUT_TIMESTAMP_ATTRIBUTE"
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
export SUBSCRIPTION=<subscription>
export OUTPUT_TOPIC=<outputTopic>

### Optional
export FORMAT=JSON
export SCHEMA=<schema>
export ATTRIBUTES=<attributes>
export ATTRIBUTES_MAP=<attributesMap>
export ID_ATTRIBUTE=<idAttribute>
export TIMESTAMP_ATTRIBUTE=<timestampAttribute>
export PUBLISH_TIME_FIELD=<publishTimeField>
export OUTPUT_FORMAT=JSON
export OUTPUT_SCHEMA=<outputSchema>
export OUTPUT_ATTRIBUTES=<outputAttributes>
export OUTPUT_ATTRIBUTES_MAP=<outputAttributesMap>
export OUTPUT_ID_ATTRIBUTE=<outputIdAttribute>
export OUTPUT_TIMESTAMP_ATTRIBUTE=<outputTimestampAttribute>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="pubsub-subscription-to-pubsub-topic-yaml-job" \
-DtemplateName="PubSub_Subscription_to_PubSub_Topic_Yaml" \
-Dparameters="subscription=$SUBSCRIPTION,format=$FORMAT,schema=$SCHEMA,attributes=$ATTRIBUTES,attributesMap=$ATTRIBUTES_MAP,idAttribute=$ID_ATTRIBUTE,timestampAttribute=$TIMESTAMP_ATTRIBUTE,publishTimeField=$PUBLISH_TIME_FIELD,outputTopic=$OUTPUT_TOPIC,outputFormat=$OUTPUT_FORMAT,outputSchema=$OUTPUT_SCHEMA,outputAttributes=$OUTPUT_ATTRIBUTES,outputAttributesMap=$OUTPUT_ATTRIBUTES_MAP,outputIdAttribute=$OUTPUT_ID_ATTRIBUTE,outputTimestampAttribute=$OUTPUT_TIMESTAMP_ATTRIBUTE" \
-f yaml
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
cd yaml/terraform/PubSub_Subscription_to_PubSub_Topic_Yaml
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

resource "google_dataflow_flex_template_job" "pubsub_subscription_to_pubsub_topic_yaml" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/PubSub_Subscription_to_PubSub_Topic_Yaml"
  name              = "pubsub-subscription-to-pubsub-topic-yaml"
  region            = var.region
  parameters        = {
    subscription = "<subscription>"
    outputTopic = "<outputTopic>"
    # format = "JSON"
    # schema = "<schema>"
    # attributes = "<attributes>"
    # attributesMap = "<attributesMap>"
    # idAttribute = "<idAttribute>"
    # timestampAttribute = "<timestampAttribute>"
    # publishTimeField = "<publishTimeField>"
    # outputFormat = "JSON"
    # outputSchema = "<outputSchema>"
    # outputAttributes = "<outputAttributes>"
    # outputAttributesMap = "<outputAttributesMap>"
    # outputIdAttribute = "<outputIdAttribute>"
    # outputTimestampAttribute = "<outputTimestampAttribute>"
  }
}
```
