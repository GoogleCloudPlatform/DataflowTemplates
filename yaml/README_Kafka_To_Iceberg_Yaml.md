
Kafka to Iceberg (YAML) template
---
The Kafka to Iceberg template is a streaming pipeline that reads data from Kafka
and writes to an Iceberg table.



:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **bootstrapServers**: A list of host/port pairs to use for establishing the initial connection to the Kafka cluster. For example: host1:port1,host2:port2 For example, `host1:port1,host2:port2`.
* **topic**: Kafka topic to read from. For example: my_topic For example, `my_topic`.
* **table**: A fully-qualified table identifier, e.g., my_dataset.my_table. For example, `my_dataset.my_table`.
* **catalogName**: The name of the Iceberg catalog that contains the table. For example, `my_hadoop_catalog`.
* **catalogProperties**: A map of properties for setting up the Iceberg catalog. For example, `{"type": "hadoop", "warehouse": "gs://your-bucket/warehouse"}`.
* **triggeringFrequencySeconds**: The frequency in seconds for producing snapshots in a streaming pipeline. For example, `60`.

### Optional parameters

* **allowDuplicates**: If the Kafka read allows duplicates. For example: true For example, `true`.
* **confluentSchemaRegistrySubject**: The subject name for the Confluent Schema Registry. For example: my_subject For example, `my_subject`.
* **confluentSchemaRegistryUrl**: The URL for the Confluent Schema Registry. For example: http://schema-registry:8081 For example, `http://schema-registry:8081`.
* **consumerConfigUpdates**: A list of key-value pairs that act as configuration parameters for Kafka consumers. For example: {'group.id': 'my_group'} For example, `{"group.id": "my_group"}`.
* **fileDescriptorPath**: The path to the Protocol Buffer File Descriptor Set file. For example: gs://bucket/path/to/descriptor.pb For example, `gs://bucket/path/to/descriptor.pb`.
* **format**: The encoding format for the data stored in Kafka. Valid options are: RAW,STRING,AVRO,JSON,PROTO. For example: JSON For example, `JSON`. Defaults to: JSON.
* **messageName**: The name of the Protocol Buffer message to be used for schema extraction and data conversion. For example: MyMessage For example, `MyMessage`.
* **offsetDeduplication**: If the redistribute is using offset deduplication mode. For example: true For example, `true`.
* **redistributeByRecordKey**: If the redistribute keys by the Kafka record key. For example: true For example, `true`.
* **redistributeNumKeys**: The number of keys for redistributing Kafka inputs. For example: 10 For example, `10`.
* **redistributed**: If the Kafka read should be redistributed. For example: true For example, `true`.
* **schema**: The schema in which the data is encoded in the Kafka topic. For example: {'type': 'record', 'name': 'User', 'fields': [{'name': 'name', 'type': 'string'}]} For example, `{"type": "record", "name": "User", "fields": [{"name": "name", "type": "string"}]}`.
* **configProperties**: A map of properties to pass to the Hadoop Configuration. For example, `{"fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"}`.
* **drop**: A list of field names to drop. Mutually exclusive with 'keep' and 'only'. For example, `["field_to_drop_1", "field_to_drop_2"]`.
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

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=yaml/src/main/java/com/google/cloud/teleport/templates/yaml/KafkaToIcebergYaml.java)

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
-DtemplateName="Kafka_To_Iceberg_Yaml" \
-f yaml
```

The `-DartifactRegistry` parameter can be specified to set the artifact registry repository of the Flex Templates image.
If not provided, it defaults to `gcr.io/<project>`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Kafka_To_Iceberg_Yaml
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Kafka_To_Iceberg_Yaml"

### Required
export BOOTSTRAP_SERVERS=<bootstrapServers>
export TOPIC=<topic>
export TABLE=<table>
export CATALOG_NAME=<catalogName>
export CATALOG_PROPERTIES=<catalogProperties>
export TRIGGERING_FREQUENCY_SECONDS=<triggeringFrequencySeconds>

### Optional
export ALLOW_DUPLICATES=<allowDuplicates>
export CONFLUENT_SCHEMA_REGISTRY_SUBJECT=<confluentSchemaRegistrySubject>
export CONFLUENT_SCHEMA_REGISTRY_URL=<confluentSchemaRegistryUrl>
export CONSUMER_CONFIG_UPDATES=<consumerConfigUpdates>
export FILE_DESCRIPTOR_PATH=<fileDescriptorPath>
export FORMAT=JSON
export MESSAGE_NAME=<messageName>
export OFFSET_DEDUPLICATION=<offsetDeduplication>
export REDISTRIBUTE_BY_RECORD_KEY=<redistributeByRecordKey>
export REDISTRIBUTE_NUM_KEYS=<redistributeNumKeys>
export REDISTRIBUTED=<redistributed>
export SCHEMA=<schema>
export CONFIG_PROPERTIES=<configProperties>
export DROP=<drop>
export KEEP=<keep>
export ONLY=<only>
export PARTITION_FIELDS=<partitionFields>
export TABLE_PROPERTIES=<tableProperties>
export SDF_CHECKPOINT_AFTER_DURATION=30s
export SDF_CHECKPOINT_AFTER_OUTPUT_BYTES=536870912

gcloud dataflow flex-template run "kafka-to-iceberg-yaml-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "bootstrapServers=$BOOTSTRAP_SERVERS" \
  --parameters "topic=$TOPIC" \
  --parameters "allowDuplicates=$ALLOW_DUPLICATES" \
  --parameters "confluentSchemaRegistrySubject=$CONFLUENT_SCHEMA_REGISTRY_SUBJECT" \
  --parameters "confluentSchemaRegistryUrl=$CONFLUENT_SCHEMA_REGISTRY_URL" \
  --parameters "consumerConfigUpdates=$CONSUMER_CONFIG_UPDATES" \
  --parameters "fileDescriptorPath=$FILE_DESCRIPTOR_PATH" \
  --parameters "format=$FORMAT" \
  --parameters "messageName=$MESSAGE_NAME" \
  --parameters "offsetDeduplication=$OFFSET_DEDUPLICATION" \
  --parameters "redistributeByRecordKey=$REDISTRIBUTE_BY_RECORD_KEY" \
  --parameters "redistributeNumKeys=$REDISTRIBUTE_NUM_KEYS" \
  --parameters "redistributed=$REDISTRIBUTED" \
  --parameters "schema=$SCHEMA" \
  --parameters "table=$TABLE" \
  --parameters "catalogName=$CATALOG_NAME" \
  --parameters "catalogProperties=$CATALOG_PROPERTIES" \
  --parameters "configProperties=$CONFIG_PROPERTIES" \
  --parameters "drop=$DROP" \
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
export BOOTSTRAP_SERVERS=<bootstrapServers>
export TOPIC=<topic>
export TABLE=<table>
export CATALOG_NAME=<catalogName>
export CATALOG_PROPERTIES=<catalogProperties>
export TRIGGERING_FREQUENCY_SECONDS=<triggeringFrequencySeconds>

### Optional
export ALLOW_DUPLICATES=<allowDuplicates>
export CONFLUENT_SCHEMA_REGISTRY_SUBJECT=<confluentSchemaRegistrySubject>
export CONFLUENT_SCHEMA_REGISTRY_URL=<confluentSchemaRegistryUrl>
export CONSUMER_CONFIG_UPDATES=<consumerConfigUpdates>
export FILE_DESCRIPTOR_PATH=<fileDescriptorPath>
export FORMAT=JSON
export MESSAGE_NAME=<messageName>
export OFFSET_DEDUPLICATION=<offsetDeduplication>
export REDISTRIBUTE_BY_RECORD_KEY=<redistributeByRecordKey>
export REDISTRIBUTE_NUM_KEYS=<redistributeNumKeys>
export REDISTRIBUTED=<redistributed>
export SCHEMA=<schema>
export CONFIG_PROPERTIES=<configProperties>
export DROP=<drop>
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
-DjobName="kafka-to-iceberg-yaml-job" \
-DtemplateName="Kafka_To_Iceberg_Yaml" \
-Dparameters="bootstrapServers=$BOOTSTRAP_SERVERS,topic=$TOPIC,allowDuplicates=$ALLOW_DUPLICATES,confluentSchemaRegistrySubject=$CONFLUENT_SCHEMA_REGISTRY_SUBJECT,confluentSchemaRegistryUrl=$CONFLUENT_SCHEMA_REGISTRY_URL,consumerConfigUpdates=$CONSUMER_CONFIG_UPDATES,fileDescriptorPath=$FILE_DESCRIPTOR_PATH,format=$FORMAT,messageName=$MESSAGE_NAME,offsetDeduplication=$OFFSET_DEDUPLICATION,redistributeByRecordKey=$REDISTRIBUTE_BY_RECORD_KEY,redistributeNumKeys=$REDISTRIBUTE_NUM_KEYS,redistributed=$REDISTRIBUTED,schema=$SCHEMA,table=$TABLE,catalogName=$CATALOG_NAME,catalogProperties=$CATALOG_PROPERTIES,configProperties=$CONFIG_PROPERTIES,drop=$DROP,keep=$KEEP,only=$ONLY,partitionFields=$PARTITION_FIELDS,tableProperties=$TABLE_PROPERTIES,triggeringFrequencySeconds=$TRIGGERING_FREQUENCY_SECONDS,sdfCheckpointAfterDuration=$SDF_CHECKPOINT_AFTER_DURATION,sdfCheckpointAfterOutputBytes=$SDF_CHECKPOINT_AFTER_OUTPUT_BYTES" \
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
cd v2/yaml/terraform/Kafka_To_Iceberg_Yaml
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

resource "google_dataflow_flex_template_job" "kafka_to_iceberg_yaml" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Kafka_To_Iceberg_Yaml"
  name              = "kafka-to-iceberg-yaml"
  region            = var.region
  parameters        = {
    bootstrapServers = "<bootstrapServers>"
    topic = "<topic>"
    table = "<table>"
    catalogName = "<catalogName>"
    catalogProperties = "<catalogProperties>"
    triggeringFrequencySeconds = "<triggeringFrequencySeconds>"
    # allowDuplicates = "<allowDuplicates>"
    # confluentSchemaRegistrySubject = "<confluentSchemaRegistrySubject>"
    # confluentSchemaRegistryUrl = "<confluentSchemaRegistryUrl>"
    # consumerConfigUpdates = "<consumerConfigUpdates>"
    # fileDescriptorPath = "<fileDescriptorPath>"
    # format = "JSON"
    # messageName = "<messageName>"
    # offsetDeduplication = "<offsetDeduplication>"
    # redistributeByRecordKey = "<redistributeByRecordKey>"
    # redistributeNumKeys = "<redistributeNumKeys>"
    # redistributed = "<redistributed>"
    # schema = "<schema>"
    # configProperties = "<configProperties>"
    # drop = "<drop>"
    # keep = "<keep>"
    # only = "<only>"
    # partitionFields = "<partitionFields>"
    # tableProperties = "<tableProperties>"
    # sdfCheckpointAfterDuration = "30s"
    # sdfCheckpointAfterOutputBytes = "536870912"
  }
}
```
