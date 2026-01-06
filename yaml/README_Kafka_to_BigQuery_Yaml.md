
Kafka to BigQuery (YAML) template
---
The Apache Kafka to BigQuery template is a streaming pipeline which ingests text
data from Apache Kafka, executes a user-defined function (UDF), and outputs the
resulting records to BigQuery. Any errors which occur in the transformation of
the data, execution of the UDF, or inserting into the output table are inserted
into a separate errors table in BigQuery. If the errors table does not exist
prior to execution, then it is created.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/kafka-to-bigquery)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Kafka_to_BigQuery_Yaml).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **bootstrapServers**: A list of host/port pairs to use for establishing the initial connection to the Kafka cluster. For example: host1:port1,host2:port2 For example, `host1:port1,host2:port2,localhost:9092,127.0.0.1:9093`.
* **topic**: Kafka topic to read from. For example: my_topic For example, `my_topic`.
* **table**: BigQuery table location to write the output to or read from. The name  should be in the format <project>:<dataset>.<table_name>`. For write,  the table's schema must match input objects.
* **outputDeadletterTable**: BigQuery table for failed messages. Messages failed to reach the output  table for different reasons (e.g., mismatched schema, malformed json)  are written to this table. If it doesn't exist, it will be created  during pipeline execution. If not specified,  'outputTableSpec_error_records' is used instead. The dead-letter table name to output failed messages to BigQuery. For example, `your-project-id:your-dataset.your-table-name`.

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
* **schema**: The schema in which the data is encoded in the Kafka topic.  For example: {'type': 'record', 'name': 'User', 'fields': [{'name': 'name', 'type': 'string'}]}. A schema is required if data format is JSON, AVRO or PROTO. For example, `{"type": "record", "name": "User", "fields": [{"name": "name", "type": "string"}]}`.
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

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=yaml/src/main/java/com/google/cloud/teleport/templates/yaml/KafkaToBigQueryYaml.java)

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
-DtemplateName="Kafka_to_BigQuery_Yaml" \
-f yaml
```

The `-DartifactRegistry` parameter can be specified to set the artifact registry repository of the Flex Templates image.
If not provided, it defaults to `gcr.io/<project>`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Kafka_to_BigQuery_Yaml
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Kafka_to_BigQuery_Yaml"

### Required
export BOOTSTRAP_SERVERS=<bootstrapServers>
export TOPIC=<topic>
export TABLE=<table>
export OUTPUT_DEADLETTER_TABLE=<outputDeadletterTable>

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
export CREATE_DISPOSITION=CREATE_IF_NEEDED
export WRITE_DISPOSITION=WRITE_APPEND
export NUM_STREAMS=1

gcloud dataflow flex-template run "kafka-to-bigquery-yaml-job" \
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
  --parameters "createDisposition=$CREATE_DISPOSITION" \
  --parameters "writeDisposition=$WRITE_DISPOSITION" \
  --parameters "numStreams=$NUM_STREAMS" \
  --parameters "outputDeadletterTable=$OUTPUT_DEADLETTER_TABLE"
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
export OUTPUT_DEADLETTER_TABLE=<outputDeadletterTable>

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
export CREATE_DISPOSITION=CREATE_IF_NEEDED
export WRITE_DISPOSITION=WRITE_APPEND
export NUM_STREAMS=1

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="kafka-to-bigquery-yaml-job" \
-DtemplateName="Kafka_to_BigQuery_Yaml" \
-Dparameters="bootstrapServers=$BOOTSTRAP_SERVERS,topic=$TOPIC,allowDuplicates=$ALLOW_DUPLICATES,confluentSchemaRegistrySubject=$CONFLUENT_SCHEMA_REGISTRY_SUBJECT,confluentSchemaRegistryUrl=$CONFLUENT_SCHEMA_REGISTRY_URL,consumerConfigUpdates=$CONSUMER_CONFIG_UPDATES,fileDescriptorPath=$FILE_DESCRIPTOR_PATH,format=$FORMAT,messageName=$MESSAGE_NAME,offsetDeduplication=$OFFSET_DEDUPLICATION,redistributeByRecordKey=$REDISTRIBUTE_BY_RECORD_KEY,redistributeNumKeys=$REDISTRIBUTE_NUM_KEYS,redistributed=$REDISTRIBUTED,schema=$SCHEMA,table=$TABLE,createDisposition=$CREATE_DISPOSITION,writeDisposition=$WRITE_DISPOSITION,numStreams=$NUM_STREAMS,outputDeadletterTable=$OUTPUT_DEADLETTER_TABLE" \
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
cd v2/yaml/terraform/Kafka_to_BigQuery_Yaml
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

resource "google_dataflow_flex_template_job" "kafka_to_bigquery_yaml" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Kafka_to_BigQuery_Yaml"
  name              = "kafka-to-bigquery-yaml"
  region            = var.region
  parameters        = {
    bootstrapServers = "<bootstrapServers>"
    topic = "<topic>"
    table = "<table>"
    outputDeadletterTable = "<outputDeadletterTable>"
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
    # createDisposition = "CREATE_IF_NEEDED"
    # writeDisposition = "WRITE_APPEND"
    # numStreams = "1"
  }
}
```
