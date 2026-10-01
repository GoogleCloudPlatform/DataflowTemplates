
Pub/Sub to BigQuery (YAML) template
---
The Pub/Sub to BigQuery template is a streaming pipeline that reads
JSON-formatted data from a Pub/Sub topic or subscription and writes the resulting
records to BigQuery.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided-yaml/pubsub-to-bigquery)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=PubSub_To_BigQuery_Yaml).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **topic**: Pub/Sub topic to read the input from. For example, `projects/your-project-id/topics/your-topic-name`.
* **format**: The message format. One of: AVRO, JSON, PROTO, RAW, or STRING.
* **schema**: A schema is required if data format is JSON, AVRO or PROTO. For JSON,  this is a JSON schema. For AVRO and PROTO, this is the full schema  definition.
* **table**: BigQuery table location to write the output to or read from. The name  should be in the format <project>:<dataset>.<table_name>. For write,  the table's schema must match input objects.

### Optional parameters

* **attributes**: List of attribute keys whose values will be flattened into the output message as additional fields.  For example, if the format is `raw` and attributes is `[a, b]` then this read will produce elements of the form `Row(payload=..., a=..., b=...)`.
* **attributesMap**: Name of a field in which to store the full set of attributes associated with this message.  For example, if the format is `raw` and `attribute_map` is set to `attrs` then this read will produce elements of the form `Row(payload=..., attrs=...)` where `attrs` is a Map type of string to string. If both `attributes` and `attribute_map` are set, the overlapping attribute values will be present in both the flattened structure and the attribute map.
* **idAttribute**: The attribute on incoming Pub/Sub messages to use as a unique record identifier. When specified, the value of this attribute (which can be any string that uniquely identifies the record) will be used for deduplication of messages. If not provided, we cannot guarantee that no duplicate data will be delivered on the Pub/Sub stream. In this case, deduplication of the stream will be strictly best effort.
* **timestampAttribute**: Message value to use as element timestamp. If None, uses message  publishing time as the timestamp. Timestamp values should be in one of two formats: 1). A numerical value representing the number of milliseconds since the Unix epoch. 2). A string in RFC 3339 format, UTC timezone. Example: ``2015-10-29T23:41:41.123Z``. The sub-second component of the timestamp is optional, and digits beyond the first three (i.e., time units smaller than milliseconds) may be ignored.
* **errorHandling**: This option specifies whether and where to output error rows.
* **subscription**: Pub/Sub subscription to read the input from. For example, `projects/your-project-id/subscriptions/your-subscription-name`.
* **createDisposition**: Specifies whether a table should be created if it does not exist.  Valid inputs are 'Never' and 'IfNeeded'. Defaults to: CREATE_IF_NEEDED.
* **writeDisposition**: How to specify if a write should append to an existing table, replace the table, or verify that the table is empty. Note that the my_dataset being written to must already exist. Unbounded collections can only be written using 'WRITE_EMPTY' or 'WRITE_APPEND'. Defaults to: WRITE_APPEND.
* **numStreams**: Number of streams defines the parallelism of the BigQueryIO’s Write  transform and roughly corresponds to the number of Storage Write API’s  streams which will be used by the pipeline. See https://cloud.google.com/blog/products/data-analytics/streaming-data-into-bigquery-using-storage-write-api for the recommended values. The default value is 1.



## Getting Started

### Requirements

* Java 17
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=yaml/src/main/java/com/google/cloud/teleport/templates/yaml/PubSubToBigQueryYaml.java)

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
-DtemplateName="PubSub_To_BigQuery_Yaml" \
-f yaml
```

The `-DartifactRegistry` parameter can be specified to set the artifact registry repository of the Flex Templates image.
If not provided, it defaults to `gcr.io/<project>`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/PubSub_To_BigQuery_Yaml
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/PubSub_To_BigQuery_Yaml"

### Required
export TOPIC=<topic>
export FORMAT=<format>
export SCHEMA=<schema>
export TABLE=<table>

### Optional
export ATTRIBUTES=<attributes>
export ATTRIBUTES_MAP=<attributesMap>
export ID_ATTRIBUTE=<idAttribute>
export TIMESTAMP_ATTRIBUTE=<timestampAttribute>
export ERROR_HANDLING=<errorHandling>
export SUBSCRIPTION=<subscription>
export CREATE_DISPOSITION=CREATE_IF_NEEDED
export WRITE_DISPOSITION=WRITE_APPEND
export NUM_STREAMS=1

gcloud dataflow flex-template run "pubsub-to-bigquery-yaml-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "topic=$TOPIC" \
  --parameters "format=$FORMAT" \
  --parameters "schema=$SCHEMA" \
  --parameters "attributes=$ATTRIBUTES" \
  --parameters "attributesMap=$ATTRIBUTES_MAP" \
  --parameters "idAttribute=$ID_ATTRIBUTE" \
  --parameters "timestampAttribute=$TIMESTAMP_ATTRIBUTE" \
  --parameters "errorHandling=$ERROR_HANDLING" \
  --parameters "subscription=$SUBSCRIPTION" \
  --parameters "table=$TABLE" \
  --parameters "createDisposition=$CREATE_DISPOSITION" \
  --parameters "writeDisposition=$WRITE_DISPOSITION" \
  --parameters "numStreams=$NUM_STREAMS"
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
export FORMAT=<format>
export SCHEMA=<schema>
export TABLE=<table>

### Optional
export ATTRIBUTES=<attributes>
export ATTRIBUTES_MAP=<attributesMap>
export ID_ATTRIBUTE=<idAttribute>
export TIMESTAMP_ATTRIBUTE=<timestampAttribute>
export ERROR_HANDLING=<errorHandling>
export SUBSCRIPTION=<subscription>
export CREATE_DISPOSITION=CREATE_IF_NEEDED
export WRITE_DISPOSITION=WRITE_APPEND
export NUM_STREAMS=1

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="pubsub-to-bigquery-yaml-job" \
-DtemplateName="PubSub_To_BigQuery_Yaml" \
-Dparameters="topic=$TOPIC,format=$FORMAT,schema=$SCHEMA,attributes=$ATTRIBUTES,attributesMap=$ATTRIBUTES_MAP,idAttribute=$ID_ATTRIBUTE,timestampAttribute=$TIMESTAMP_ATTRIBUTE,errorHandling=$ERROR_HANDLING,subscription=$SUBSCRIPTION,table=$TABLE,createDisposition=$CREATE_DISPOSITION,writeDisposition=$WRITE_DISPOSITION,numStreams=$NUM_STREAMS" \
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
cd yaml/terraform/PubSub_To_BigQuery_Yaml
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

resource "google_dataflow_flex_template_job" "pubsub_to_bigquery_yaml" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/PubSub_To_BigQuery_Yaml"
  name              = "pubsub-to-bigquery-yaml"
  region            = var.region
  parameters        = {
    topic = "<topic>"
    format = "<format>"
    schema = "<schema>"
    table = "<table>"
    # attributes = "<attributes>"
    # attributesMap = "<attributesMap>"
    # idAttribute = "<idAttribute>"
    # timestampAttribute = "<timestampAttribute>"
    # errorHandling = "<errorHandling>"
    # subscription = "<subscription>"
    # createDisposition = "CREATE_IF_NEEDED"
    # writeDisposition = "WRITE_APPEND"
    # numStreams = "1"
  }
}
```
