
PubSub to BigTable (YAML) template
---
The PubSub to BigTable template is a streaming pipeline which ingests data from a
PubSub topic, executes a user-defined mapping, and writes the resulting records
to BigTable. Any errors which occur in the transformation of the data are written
to a separate Pub/Sub topic.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided-yaml/pubsub-to-bigtable)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=PubSub_To_BigTable_Yaml).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **topic**: Pub/Sub topic to read the input from. For example, `projects/your-project-id/topics/your-topic-name`.
* **schema**: A schema is required if data format is JSON, AVRO or PROTO. For JSON,  this is a JSON schema. For AVRO and PROTO, this is the full schema  definition.
* **language**: The language used to define (and execute) the expressions and/or  callables in fields. Defaults to generic.
* **fields**: The output fields to compute, each mapping to the expression or callable that creates them.
* **projectId**: The Google Cloud project ID of the BigTable instance.
* **instanceId**: The BigTable instance ID.
* **tableId**: BigTable table ID to write the output to.
* **outputDeadLetterPubSubTopic**: Pub/Sub error topic for failed transformation messages. For example, `projects/your-project-id/topics/your-error-topic-name`.

### Optional parameters

* **format**: The message format. One of: AVRO, JSON, PROTO, RAW, or STRING. Defaults to: JSON.
* **windowing**: Windowing options - see https://beam.apache.org/documentation/sdks/yaml/#windowing.



## Getting Started

### Requirements

* Java 17
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=yaml/src/main/java/com/google/cloud/teleport/templates/yaml/PubSubToBigTableYaml.java)

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
-DtemplateName="PubSub_To_BigTable_Yaml" \
-f yaml
```

The `-DartifactRegistry` parameter can be specified to set the artifact registry repository of the Flex Templates image.
If not provided, it defaults to `gcr.io/<project>`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/PubSub_To_BigTable_Yaml
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/PubSub_To_BigTable_Yaml"

### Required
export TOPIC=<topic>
export SCHEMA=<schema>
export LANGUAGE=<language>
export FIELDS=<fields>
export PROJECT_ID=<projectId>
export INSTANCE_ID=<instanceId>
export TABLE_ID=<tableId>
export OUTPUT_DEAD_LETTER_PUB_SUB_TOPIC=<outputDeadLetterPubSubTopic>

### Optional
export FORMAT=JSON
export WINDOWING=<windowing>

gcloud dataflow flex-template run "pubsub-to-bigtable-yaml-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "topic=$TOPIC" \
  --parameters "format=$FORMAT" \
  --parameters "schema=$SCHEMA" \
  --parameters "language=$LANGUAGE" \
  --parameters "fields=$FIELDS" \
  --parameters "projectId=$PROJECT_ID" \
  --parameters "instanceId=$INSTANCE_ID" \
  --parameters "tableId=$TABLE_ID" \
  --parameters "windowing=$WINDOWING" \
  --parameters "outputDeadLetterPubSubTopic=$OUTPUT_DEAD_LETTER_PUB_SUB_TOPIC"
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
export TOPIC=<topic>
export SCHEMA=<schema>
export LANGUAGE=<language>
export FIELDS=<fields>
export PROJECT_ID=<projectId>
export INSTANCE_ID=<instanceId>
export TABLE_ID=<tableId>
export OUTPUT_DEAD_LETTER_PUB_SUB_TOPIC=<outputDeadLetterPubSubTopic>

### Optional
export FORMAT=JSON
export WINDOWING=<windowing>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="pubsub-to-bigtable-yaml-job" \
-DtemplateName="PubSub_To_BigTable_Yaml" \
-Dparameters="topic=$TOPIC,format=$FORMAT,schema=$SCHEMA,language=$LANGUAGE,fields=$FIELDS,projectId=$PROJECT_ID,instanceId=$INSTANCE_ID,tableId=$TABLE_ID,windowing=$WINDOWING,outputDeadLetterPubSubTopic=$OUTPUT_DEAD_LETTER_PUB_SUB_TOPIC" \
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
cd v2/yaml/terraform/PubSub_To_BigTable_Yaml
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

resource "google_dataflow_flex_template_job" "pubsub_to_bigtable_yaml" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/PubSub_To_BigTable_Yaml"
  name              = "pubsub-to-bigtable-yaml"
  region            = var.region
  parameters        = {
    topic = "<topic>"
    schema = "<schema>"
    language = "<language>"
    fields = "<fields>"
    projectId = "<projectId>"
    instanceId = "<instanceId>"
    tableId = "<tableId>"
    outputDeadLetterPubSubTopic = "<outputDeadLetterPubSubTopic>"
    # format = "JSON"
    # windowing = "<windowing>"
  }
}
```
