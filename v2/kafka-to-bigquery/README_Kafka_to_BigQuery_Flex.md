
Kafka to BigQuery template
---
The Apache Kafka to BigQuery template is a streaming pipeline which ingests text
data from Apache Kafka, executes a user-defined function (UDF), and outputs the
resulting records to BigQuery. Any errors which occur in the transformation of
the data, execution of the UDF, or inserting into the output table are inserted
into a separate errors table in BigQuery. If the errors table does not exist
prior to execution, then it is created.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/kafka-to-bigquery)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Kafka_to_BigQuery_Flex).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters


### Optional parameters

* **outputTableSpec** : BigQuery table location to write the output to. The name should be in the format `<project>:<dataset>.<table_name>`. The table's schema must match input objects.
* **writeDisposition** : BigQuery WriteDisposition. For example, WRITE_APPEND, WRITE_EMPTY or WRITE_TRUNCATE. Defaults to: WRITE_APPEND.
* **createDisposition** : BigQuery CreateDisposition. For example, CREATE_IF_NEEDED, CREATE_NEVER. Defaults to: CREATE_IF_NEEDED.
* **outputDeadletterTable** : BigQuery table for failed messages. Messages failed to reach the output table for different reasons (e.g., mismatched schema, malformed json) are written to this table. If it doesn't exist, it will be created during pipeline execution. If not specified, "outputTableSpec_error_records" is used instead. (Example: your-project-id:your-dataset.your-table-name).
* **messageFormat** : The Kafka message format. Can be AVRO or JSON. Defaults to: AVRO.
* **avroFormat** : This parameter is used to indicate what format to use for the avro messages. Default is CONFLUENT_WIRE_FORMAT.
* **avroSchemaPath** : Cloud Storage path to Avro schema file. For example, gs://MyBucket/file.avsc.
* **schemaRegistryConnectionUrl** : Schema Registry Connection URL for a registry which supports Confluent wire format.
* **outputDataset** : BigQuery output dataset to write the output to. Tables will be created dynamically in the dataset. If the tables are created beforehand, the table names should follow the specified naming convention. The name should be `bqTableNamePrefix + Avro Schema FullName` {@link org.apache.avro.Schema.getFullName}, each word will be seperated by a hyphen '-'.
* **persistKafkaKey** : If true, the pipeline will persist the Kafka message key in the BigQuery table, in a `_key` field of type `BYTES`. Default is false (Key is ignored).
* **bqTableNamePrefix** : Naming prefix to be used while creating BigQuery output tables. Only applicable when using schema registry. Defaults to empty.
* **useStorageWriteApiAtLeastOnce** : This parameter takes effect only if "Use BigQuery Storage Write API" is enabled. If enabled the at-least-once semantics will be used for Storage Write API, otherwise exactly-once semantics will be used. Defaults to: false.
* **readBootstrapServers** : Kafka Bootstrap Server list, separated by commas. (Example: localhost:9092,127.0.0.1:9093).
* **kafkaReadTopics** : Kafka topic(s) to read input from. (Example: topic1,topic2).
* **kafkaReadOffset** : The Kafka Offset to read from. Defaults to: latest.
* **kafkaReadUsernameSecretId** : Secret Manager secret ID for the SASL_PLAIN username. Should be in the format projects/{project}/secrets/{secret}/versions/{secret_version}. (Example: projects/your-project-id/secrets/your-secret/versions/your-secret-version).
* **kafkaReadPasswordSecretId** : Secret Manager secret ID for the SASL_PLAIN password. Should be in the format projects/{project}/secrets/{secret}/versions/{secret_version} (Example: projects/your-project-id/secrets/your-secret/versions/your-secret-version).
* **useStorageWriteApi** : If true, the pipeline uses the BigQuery Storage Write API (https://cloud.google.com/bigquery/docs/write-api). The default value is `false`. For more information, see Using the Storage Write API (https://beam.apache.org/documentation/io/built-in/google-bigquery/#storage-write-api).
* **numStorageWriteApiStreams** : When using the Storage Write API, specifies the number of write streams. If `useStorageWriteApi` is `true` and `useStorageWriteApiAtLeastOnce` is `false`, then you must set this parameter. Defaults to: 0.
* **storageWriteApiTriggeringFrequencySec** : When using the Storage Write API, specifies the triggering frequency, in seconds. If `useStorageWriteApi` is `true` and `useStorageWriteApiAtLeastOnce` is `false`, then you must set this parameter.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/kafka-to-bigquery/src/main/java/com/google/cloud/teleport/v2/templates/KafkaToBigQueryFlex.java)

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
-DtemplateName="Kafka_to_BigQuery_Flex" \
-f v2/kafka-to-bigquery
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Kafka_to_BigQuery_Flex
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Kafka_to_BigQuery_Flex"

### Required

### Optional
export OUTPUT_TABLE_SPEC=<outputTableSpec>
export WRITE_DISPOSITION=WRITE_APPEND
export CREATE_DISPOSITION=CREATE_IF_NEEDED
export OUTPUT_DEADLETTER_TABLE=<outputDeadletterTable>
export MESSAGE_FORMAT=AVRO
export AVRO_FORMAT=CONFLUENT_WIRE_FORMAT
export AVRO_SCHEMA_PATH=<avroSchemaPath>
export SCHEMA_REGISTRY_CONNECTION_URL=<schemaRegistryConnectionUrl>
export OUTPUT_DATASET=<outputDataset>
export PERSIST_KAFKA_KEY=false
export BQ_TABLE_NAME_PREFIX=""
export USE_STORAGE_WRITE_API_AT_LEAST_ONCE=false
export READ_BOOTSTRAP_SERVERS=<readBootstrapServers>
export KAFKA_READ_TOPICS=<kafkaReadTopics>
export KAFKA_READ_OFFSET=latest
export KAFKA_READ_USERNAME_SECRET_ID=<kafkaReadUsernameSecretId>
export KAFKA_READ_PASSWORD_SECRET_ID=<kafkaReadPasswordSecretId>
export USE_STORAGE_WRITE_API=false
export NUM_STORAGE_WRITE_API_STREAMS=0
export STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC=<storageWriteApiTriggeringFrequencySec>

gcloud dataflow flex-template run "kafka-to-bigquery-flex-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "outputTableSpec=$OUTPUT_TABLE_SPEC" \
  --parameters "writeDisposition=$WRITE_DISPOSITION" \
  --parameters "createDisposition=$CREATE_DISPOSITION" \
  --parameters "outputDeadletterTable=$OUTPUT_DEADLETTER_TABLE" \
  --parameters "messageFormat=$MESSAGE_FORMAT" \
  --parameters "avroFormat=$AVRO_FORMAT" \
  --parameters "avroSchemaPath=$AVRO_SCHEMA_PATH" \
  --parameters "schemaRegistryConnectionUrl=$SCHEMA_REGISTRY_CONNECTION_URL" \
  --parameters "outputDataset=$OUTPUT_DATASET" \
  --parameters "persistKafkaKey=$PERSIST_KAFKA_KEY" \
  --parameters "bqTableNamePrefix=$BQ_TABLE_NAME_PREFIX" \
  --parameters "useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE" \
  --parameters "readBootstrapServers=$READ_BOOTSTRAP_SERVERS" \
  --parameters "kafkaReadTopics=$KAFKA_READ_TOPICS" \
  --parameters "kafkaReadOffset=$KAFKA_READ_OFFSET" \
  --parameters "kafkaReadUsernameSecretId=$KAFKA_READ_USERNAME_SECRET_ID" \
  --parameters "kafkaReadPasswordSecretId=$KAFKA_READ_PASSWORD_SECRET_ID" \
  --parameters "useStorageWriteApi=$USE_STORAGE_WRITE_API" \
  --parameters "numStorageWriteApiStreams=$NUM_STORAGE_WRITE_API_STREAMS" \
  --parameters "storageWriteApiTriggeringFrequencySec=$STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC"
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

### Optional
export OUTPUT_TABLE_SPEC=<outputTableSpec>
export WRITE_DISPOSITION=WRITE_APPEND
export CREATE_DISPOSITION=CREATE_IF_NEEDED
export OUTPUT_DEADLETTER_TABLE=<outputDeadletterTable>
export MESSAGE_FORMAT=AVRO
export AVRO_FORMAT=CONFLUENT_WIRE_FORMAT
export AVRO_SCHEMA_PATH=<avroSchemaPath>
export SCHEMA_REGISTRY_CONNECTION_URL=<schemaRegistryConnectionUrl>
export OUTPUT_DATASET=<outputDataset>
export PERSIST_KAFKA_KEY=false
export BQ_TABLE_NAME_PREFIX=""
export USE_STORAGE_WRITE_API_AT_LEAST_ONCE=false
export READ_BOOTSTRAP_SERVERS=<readBootstrapServers>
export KAFKA_READ_TOPICS=<kafkaReadTopics>
export KAFKA_READ_OFFSET=latest
export KAFKA_READ_USERNAME_SECRET_ID=<kafkaReadUsernameSecretId>
export KAFKA_READ_PASSWORD_SECRET_ID=<kafkaReadPasswordSecretId>
export USE_STORAGE_WRITE_API=false
export NUM_STORAGE_WRITE_API_STREAMS=0
export STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC=<storageWriteApiTriggeringFrequencySec>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="kafka-to-bigquery-flex-job" \
-DtemplateName="Kafka_to_BigQuery_Flex" \
-Dparameters="outputTableSpec=$OUTPUT_TABLE_SPEC,writeDisposition=$WRITE_DISPOSITION,createDisposition=$CREATE_DISPOSITION,outputDeadletterTable=$OUTPUT_DEADLETTER_TABLE,messageFormat=$MESSAGE_FORMAT,avroFormat=$AVRO_FORMAT,avroSchemaPath=$AVRO_SCHEMA_PATH,schemaRegistryConnectionUrl=$SCHEMA_REGISTRY_CONNECTION_URL,outputDataset=$OUTPUT_DATASET,persistKafkaKey=$PERSIST_KAFKA_KEY,bqTableNamePrefix=$BQ_TABLE_NAME_PREFIX,useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE,readBootstrapServers=$READ_BOOTSTRAP_SERVERS,kafkaReadTopics=$KAFKA_READ_TOPICS,kafkaReadOffset=$KAFKA_READ_OFFSET,kafkaReadUsernameSecretId=$KAFKA_READ_USERNAME_SECRET_ID,kafkaReadPasswordSecretId=$KAFKA_READ_PASSWORD_SECRET_ID,useStorageWriteApi=$USE_STORAGE_WRITE_API,numStorageWriteApiStreams=$NUM_STORAGE_WRITE_API_STREAMS,storageWriteApiTriggeringFrequencySec=$STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC" \
-f v2/kafka-to-bigquery
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
cd v2/kafka-to-bigquery/terraform/Kafka_to_BigQuery_Flex
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

resource "google_dataflow_flex_template_job" "kafka_to_bigquery_flex" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Kafka_to_BigQuery_Flex"
  name              = "kafka-to-bigquery-flex"
  region            = var.region
  parameters        = {
    # outputTableSpec = "<outputTableSpec>"
    # writeDisposition = "WRITE_APPEND"
    # createDisposition = "CREATE_IF_NEEDED"
    # outputDeadletterTable = "your-project-id:your-dataset.your-table-name"
    # messageFormat = "AVRO"
    # avroFormat = "CONFLUENT_WIRE_FORMAT"
    # avroSchemaPath = "<avroSchemaPath>"
    # schemaRegistryConnectionUrl = "<schemaRegistryConnectionUrl>"
    # outputDataset = "<outputDataset>"
    # persistKafkaKey = "false"
    # bqTableNamePrefix = ""
    # useStorageWriteApiAtLeastOnce = "false"
    # readBootstrapServers = "localhost:9092,127.0.0.1:9093"
    # kafkaReadTopics = "topic1,topic2"
    # kafkaReadOffset = "latest"
    # kafkaReadUsernameSecretId = "projects/your-project-id/secrets/your-secret/versions/your-secret-version"
    # kafkaReadPasswordSecretId = "projects/your-project-id/secrets/your-secret/versions/your-secret-version"
    # useStorageWriteApi = "false"
    # numStorageWriteApiStreams = "0"
    # storageWriteApiTriggeringFrequencySec = "<storageWriteApiTriggeringFrequencySec>"
  }
}
```
