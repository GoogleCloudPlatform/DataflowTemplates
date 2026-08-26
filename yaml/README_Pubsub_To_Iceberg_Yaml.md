
Pub/Sub to Iceberg (YAML) template
---
The Pub/Sub to Iceberg template is a streaming pipeline that ingests data from a
Pub/Sub topic or subscription and writes the records to an Apache Iceberg table.



:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **topic**: Pub/Sub topic to read the input from. For example, `projects/your-project-id/topics/your-topic-name`.
* **format**: The message format. One of: AVRO, JSON, PROTO, RAW, or STRING.
* **schema**: A schema is required if data format is JSON, AVRO or PROTO. For JSON,  this is a JSON schema. For AVRO and PROTO, this is the full schema  definition.
* **table**: A fully-qualified table identifier, e.g., my_dataset.my_table. For example, `my_dataset.my_table`.
* **catalogName**: The name of the Iceberg catalog that contains the table. For example, `my_hadoop_catalog`.
* **catalogProperties**: A map of properties for setting up the Iceberg catalog. For example, `{"type": "hadoop", "warehouse": "gs://your-bucket/warehouse"}`.
* **triggeringFrequencySeconds**: The frequency in seconds for producing snapshots in a streaming pipeline. For example, `60`.

### Optional parameters

* **attributes**: List of attribute keys whose values will be flattened into the output message as additional fields.  For example, if the format is `raw` and attributes is `[a, b]` then this read will produce elements of the form `Row(payload=..., a=..., b=...)`.
* **attributesMap**: Name of a field in which to store the full set of attributes associated with this message.  For example, if the format is `raw` and `attribute_map` is set to `attrs` then this read will produce elements of the form `Row(payload=..., attrs=...)` where `attrs` is a Map type of string to string. If both `attributes` and `attribute_map` are set, the overlapping attribute values will be present in both the flattened structure and the attribute map.
* **idAttribute**: The attribute on incoming Pub/Sub messages to use as a unique record identifier. When specified, the value of this attribute (which can be any string that uniquely identifies the record) will be used for deduplication of messages. If not provided, we cannot guarantee that no duplicate data will be delivered on the Pub/Sub stream. In this case, deduplication of the stream will be strictly best effort.
* **timestampAttribute**: Message value to use as element timestamp. If None, uses message  publishing time as the timestamp. Timestamp values should be in one of two formats: 1). A numerical value representing the number of milliseconds since the Unix epoch. 2). A string in RFC 3339 format, UTC timezone. Example: ``2015-10-29T23:41:41.123Z``. The sub-second component of the timestamp is optional, and digits beyond the first three (i.e., time units smaller than milliseconds) may be ignored.
* **errorHandling**: This option specifies whether and where to output error rows.
* **subscription**: Pub/Sub subscription to read the input from. For example, `projects/your-project-id/subscriptions/your-subscription-name`.
* **configProperties**: A map of properties to pass to the Hadoop Configuration. For example, `{"fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"}`.
* **drop**: A list of field names to drop. Mutually exclusive with 'keep' and 'only'. For example, `["field_to_drop_1", "field_to_drop_2"]`.
* **filter**: A filter expression to apply to records from the Iceberg table. For example, `age > 18`.
* **keep**: A list of field names to keep. Mutually exclusive with 'drop' and 'only'. For example, `["field_to_keep_1", "field_to_keep_2"]`.
* **only**: The name of a single field to write. Mutually exclusive with 'keep' and 'drop'. For example, `my_record_field`.
* **partitionFields**: A list of fields and transforms for partitioning, e.g., ['day(ts)', 'category']. For example, `["day(ts)", "bucket(id, 4)"]`.
* **tableProperties**: A map of Iceberg table properties to set when the table is created. For example, `{"commit.retry.num-retries": "2"}`.
* **sdfCheckpointAfterDuration**: Duration after which to checkpoint stateful DoFns. For example: 30s. Documentation: https://docs.cloud.google.com/dataflow/docs/reference/service-options For example, `30s`. Defaults to: 30s.
* **sdfCheckpointAfterOutputBytes**: Output bytes after which to checkpoint stateful DoFns. For example: 536870912. Documentation: https://docs.cloud.google.com/dataflow/docs/reference/service-options For example, `536870912`. Defaults to: 536870912.



## Getting Started

### Requirements

* Java 17
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=yaml/src/main/java/com/google/cloud/teleport/templates/yaml/PubsubToIcebergYaml.java)

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
-DtemplateName="Pubsub_To_Iceberg_Yaml" \
-f yaml
```

The `-DartifactRegistry` parameter can be specified to set the artifact registry repository of the Flex Templates image.
If not provided, it defaults to `gcr.io/<project>`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Pubsub_To_Iceberg_Yaml
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Pubsub_To_Iceberg_Yaml"

### Required
export TOPIC=<topic>
export FORMAT=<format>
export SCHEMA=<schema>
export TABLE=<table>
export CATALOG_NAME=<catalogName>
export CATALOG_PROPERTIES=<catalogProperties>
export TRIGGERING_FREQUENCY_SECONDS=<triggeringFrequencySeconds>

### Optional
export ATTRIBUTES=<attributes>
export ATTRIBUTES_MAP=<attributesMap>
export ID_ATTRIBUTE=<idAttribute>
export TIMESTAMP_ATTRIBUTE=<timestampAttribute>
export ERROR_HANDLING=<errorHandling>
export SUBSCRIPTION=<subscription>
export CONFIG_PROPERTIES=<configProperties>
export DROP=<drop>
export FILTER=<filter>
export KEEP=<keep>
export ONLY=<only>
export PARTITION_FIELDS=<partitionFields>
export TABLE_PROPERTIES=<tableProperties>
export SDF_CHECKPOINT_AFTER_DURATION=30s
export SDF_CHECKPOINT_AFTER_OUTPUT_BYTES=536870912

gcloud dataflow flex-template run "pubsub-to-iceberg-yaml-job" \
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
  --parameters "catalogName=$CATALOG_NAME" \
  --parameters "catalogProperties=$CATALOG_PROPERTIES" \
  --parameters "configProperties=$CONFIG_PROPERTIES" \
  --parameters "drop=$DROP" \
  --parameters "filter=$FILTER" \
  --parameters "keep=$KEEP" \
  --parameters "only=$ONLY" \
  --parameters "partitionFields=$PARTITION_FIELDS" \
  --parameters "tableProperties=$TABLE_PROPERTIES" \
  --parameters "triggeringFrequencySeconds=$TRIGGERING_FREQUENCY_SECONDS" \
  --parameters "sdfCheckpointAfterDuration=$SDF_CHECKPOINT_AFTER_DURATION" \
  --parameters "sdfCheckpointAfterOutputBytes=$SDF_CHECKPOINT_AFTER_OUTPUT_BYTES"
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
export CATALOG_NAME=<catalogName>
export CATALOG_PROPERTIES=<catalogProperties>
export TRIGGERING_FREQUENCY_SECONDS=<triggeringFrequencySeconds>

### Optional
export ATTRIBUTES=<attributes>
export ATTRIBUTES_MAP=<attributesMap>
export ID_ATTRIBUTE=<idAttribute>
export TIMESTAMP_ATTRIBUTE=<timestampAttribute>
export ERROR_HANDLING=<errorHandling>
export SUBSCRIPTION=<subscription>
export CONFIG_PROPERTIES=<configProperties>
export DROP=<drop>
export FILTER=<filter>
export KEEP=<keep>
export ONLY=<only>
export PARTITION_FIELDS=<partitionFields>
export TABLE_PROPERTIES=<tableProperties>
export SDF_CHECKPOINT_AFTER_DURATION=30s
export SDF_CHECKPOINT_AFTER_OUTPUT_BYTES=536870912

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="pubsub-to-iceberg-yaml-job" \
-DtemplateName="Pubsub_To_Iceberg_Yaml" \
-Dparameters="topic=$TOPIC,format=$FORMAT,schema=$SCHEMA,attributes=$ATTRIBUTES,attributesMap=$ATTRIBUTES_MAP,idAttribute=$ID_ATTRIBUTE,timestampAttribute=$TIMESTAMP_ATTRIBUTE,errorHandling=$ERROR_HANDLING,subscription=$SUBSCRIPTION,table=$TABLE,catalogName=$CATALOG_NAME,catalogProperties=$CATALOG_PROPERTIES,configProperties=$CONFIG_PROPERTIES,drop=$DROP,filter=$FILTER,keep=$KEEP,only=$ONLY,partitionFields=$PARTITION_FIELDS,tableProperties=$TABLE_PROPERTIES,triggeringFrequencySeconds=$TRIGGERING_FREQUENCY_SECONDS,sdfCheckpointAfterDuration=$SDF_CHECKPOINT_AFTER_DURATION,sdfCheckpointAfterOutputBytes=$SDF_CHECKPOINT_AFTER_OUTPUT_BYTES" \
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
cd yaml/terraform/Pubsub_To_Iceberg_Yaml
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

resource "google_dataflow_flex_template_job" "pubsub_to_iceberg_yaml" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Pubsub_To_Iceberg_Yaml"
  name              = "pubsub-to-iceberg-yaml"
  region            = var.region
  parameters        = {
    topic = "<topic>"
    format = "<format>"
    schema = "<schema>"
    table = "<table>"
    catalogName = "<catalogName>"
    catalogProperties = "<catalogProperties>"
    triggeringFrequencySeconds = "<triggeringFrequencySeconds>"
    # attributes = "<attributes>"
    # attributesMap = "<attributesMap>"
    # idAttribute = "<idAttribute>"
    # timestampAttribute = "<timestampAttribute>"
    # errorHandling = "<errorHandling>"
    # subscription = "<subscription>"
    # configProperties = "<configProperties>"
    # drop = "<drop>"
    # filter = "<filter>"
    # keep = "<keep>"
    # only = "<only>"
    # partitionFields = "<partitionFields>"
    # tableProperties = "<tableProperties>"
    # sdfCheckpointAfterDuration = "30s"
    # sdfCheckpointAfterOutputBytes = "536870912"
  }
}
```
