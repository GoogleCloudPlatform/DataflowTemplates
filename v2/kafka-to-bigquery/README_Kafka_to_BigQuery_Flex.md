
Kafka to BigQuery template
---
The Apache Kafka to BigQuery template is a streaming pipeline which ingests text
data from Apache Kafka, and outputs the resulting records to BigQuery. Any errors
which occur in the transformation of the data, or inserting into the output table
are inserted into a separate errors table in BigQuery. For any errors which occur
in the transformation of the data, the original records can be inserted into a
separate Kafka topic. The template supports reading a Kafka topic which contains
single/multiple schema(s). It can write to a single or multiple BigQuery tables,
depending on the schema of records.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/kafka-to-bigquery)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Kafka_to_BigQuery_Flex).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **readBootstrapServerAndTopic** : Kafka Topic to read the input from.
* **writeMode** : Write records to one table or multiple tables (based on schema). The `DYNAMIC_TABLE_NAMES` mode is supported only for `AVRO_CONFLUENT_WIRE_FORMAT` Source Message Format and `SCHEMA_REGISTRY` Schema Source. The target table name is auto-generated based on the Avro schema name of each message, it could either be a single schema (creating a single table) or multiple schemas (creating multiple tables). The `SINGLE_TABLE_NAME` mode writes to a single table (single schema) specified by the user. Defaults to `SINGLE_TABLE_NAME`.
* **kafkaReadAuthenticationMode** : The mode of authentication to use with the Kafka cluster. Use `NONE` for no authentication, `SASL_PLAIN` for SASL/PLAIN username and password, and `TLS` for certificate-based authentication. Apache Kafka for BigQuery only supports the `SASL_PLAIN` authentication mode. Defaults to: SASL_PLAIN.
* **messageFormat** : The format of the Kafka messages to read. The supported values are `AVRO_CONFLUENT_WIRE_FORMAT` (Confluent Schema Registry encoded Avro), `AVRO_BINARY_ENCODING` (Plain binary Avro), and `JSON`. Defaults to: AVRO_CONFLUENT_WIRE_FORMAT.
* **useBigQueryDLQ** : If true, failed messages will be written to BigQuery with extra error information. Defaults to: false.

### Optional parameters

* **outputTableSpec** : BigQuery table location to write the output to. The name should be in the format `<project>:<dataset>.<table_name>`. The table's schema must match input objects.
* **persistKafkaKey** : If true, the pipeline will persist the Kafka message key in the BigQuery table, in a `_key` field of type `BYTES`. Default is `false` (Key is ignored).
* **outputProject** : BigQuery output project in wehich the dataset resides. Tables will be created dynamically in the dataset. Defaults to empty.
* **outputDataset** : BigQuery output dataset to write the output to. Tables will be created dynamically in the dataset. If the tables are created beforehand, the table names should follow the specified naming convention. The name should be `bqTableNamePrefix + Avro Schema FullName` , each word will be separated by a hyphen `-`. Defaults to empty.
* **bqTableNamePrefix** : Naming prefix to be used while creating BigQuery output tables. Only applicable when using schema registry. Defaults to empty.
* **createDisposition** : BigQuery CreateDisposition. For example: `CREATE_IF_NEEDED`, `CREATE_NEVER`. Defaults to: CREATE_IF_NEEDED.
* **writeDisposition** : BigQuery WriteDisposition. For example: `WRITE_APPEND`, `WRITE_EMPTY` or `WRITE_TRUNCATE`. Defaults to: WRITE_APPEND.
* **useAutoSharding** : If true, the pipeline uses auto-sharding when writng to BigQueryThe default value is `true`.
* **numStorageWriteApiStreams** : Specifies the number of write streams, this parameter must be set. Default is `0`.
* **storageWriteApiTriggeringFrequencySec** : Specifies the triggering frequency in seconds, this parameter must be set. Default is 5 seconds.
* **useStorageWriteApiAtLeastOnce** : This parameter takes effect only if "Use BigQuery Storage Write API" is enabled. If enabled the at-least-once semantics will be used for Storage Write API, otherwise exactly-once semantics will be used. Defaults to: false.
* **enableCommitOffsets** : Commit offsets of processed messages to Kafka. If enabled, this will minimize the gaps or duplicate processing of messages when restarting the pipeline. Requires specifying the Consumer Group ID. Defaults to: false.
* **consumerGroupId** : The unique identifier for the consumer group that this pipeline belongs to. Required if Commit Offsets to Kafka is enabled. Defaults to empty.
* **kafkaReadOffset** : The starting point for reading messages when no committed offsets exist. The earliest starts from the beginning, the latest from the newest message. Defaults to: latest.
* **kafkaReadUsernameSecretId** : The Google Cloud Secret Manager secret ID that contains the Kafka username to use with `SASL_PLAIN` authentication. (Example: projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>). Defaults to empty.
* **kafkaReadPasswordSecretId** : The Google Cloud Secret Manager secret ID that contains the Kafka password to use with `SASL_PLAIN` authentication. (Example: projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>). Defaults to empty.
* **kafkaReadKeystoreLocation** : The Google Cloud Storage path to the Java KeyStore (JKS) file that contains the TLS certificate and private key to use when authenticating with the Kafka cluster. (Example: gs://your-bucket/keystore.jks).
* **kafkaReadTruststoreLocation** : The Google Cloud Storage path to the Java TrustStore (JKS) file that contains the trusted certificates to use to verify the identity of the Kafka broker.
* **kafkaReadTruststorePasswordSecretId** : The Google Cloud Secret Manager secret ID that contains the password to use to access the Java TrustStore (JKS) file for Kafka TLS authentication (Example: projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>).
* **kafkaReadKeystorePasswordSecretId** : The Google Cloud Secret Manager secret ID that contains the password to use to access the Java KeyStore (JKS) file for Kafka TLS authentication. (Example: projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>).
* **kafkaReadKeyPasswordSecretId** : The Google Cloud Secret Manager secret ID that contains the password to use to access the private key within the Java KeyStore (JKS) file for Kafka TLS authentication. (Example: projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>).
* **schemaFormat** : The Kafka schema format. Can be provided as `SINGLE_SCHEMA_FILE` or `SCHEMA_REGISTRY`. If `SINGLE_SCHEMA_FILE` is specified, use the schema mentioned in the avro schema file for all messages. If `SCHEMA_REGISTRY` is specified, the messages can have either a single schema or multiple schemas. Defaults to: SINGLE_SCHEMA_FILE.
* **confluentAvroSchemaPath** : The Google Cloud Storage path to the single Avro schema file used to decode all of the messages in a topic. Defaults to empty.
* **schemaRegistryConnectionUrl** : The URL for the Confluent Schema Registry instance used to manage Avro schemas for message decoding. Defaults to empty.
* **binaryAvroSchemaPath** : The Google Cloud Storage path to the Avro schema file used to decode binary-encoded Avro messages. Defaults to empty.
* **schemaRegistryAuthenticationMode** : Schema Registry authentication mode. Can be NONE or TLS. Defaults to: NONE.
* **schemaRegistryTruststoreLocation** : Location of the SSL certificate where the trust store for authentication to Schema Registry are stored. (Example: /your-bucket/truststore.jks).
* **schemaRegistryTruststorePasswordSecretId** : SecretId in secret manager where the password to access secret in truststore is stored. (Example: projects/your-project-number/secrets/your-secret-name/versions/your-secret-version).
* **schemaRegistryKeystoreLocation** : Keystore location that contains the SSL certificate and private key. (Example: /your-bucket/keystore.jks).
* **schemaRegistryKeystorePasswordSecretId** : SecretId in secret manager where the password to access the keystore file (Example: projects/your-project-number/secrets/your-secret-name/versions/your-secret-version).
* **schemaRegistryKeyPasswordSecretId** : SecretId of password required to access the client's private key stored within the keystore (Example: projects/your-project-number/secrets/your-secret-name/versions/your-secret-version).
* **outputDeadletterTable** : Fully Qualified BigQuery table name for failed messages. Messages failed to reach the output table for different reasons (e.g., mismatched schema, malformed json) are written to this table.The table will be created by the template. (Example: your-project-id:your-dataset.your-table-name).



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
export READ_BOOTSTRAP_SERVER_AND_TOPIC=<readBootstrapServerAndTopic>
export WRITE_MODE=SINGLE_TABLE_NAME
export KAFKA_READ_AUTHENTICATION_MODE=SASL_PLAIN
export MESSAGE_FORMAT=AVRO_CONFLUENT_WIRE_FORMAT
export USE_BIG_QUERY_DLQ=false

### Optional
export OUTPUT_TABLE_SPEC=<outputTableSpec>
export PERSIST_KAFKA_KEY=false
export OUTPUT_PROJECT=""
export OUTPUT_DATASET=""
export BQ_TABLE_NAME_PREFIX=""
export CREATE_DISPOSITION=CREATE_IF_NEEDED
export WRITE_DISPOSITION=WRITE_APPEND
export USE_AUTO_SHARDING=true
export NUM_STORAGE_WRITE_API_STREAMS=0
export STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC=<storageWriteApiTriggeringFrequencySec>
export USE_STORAGE_WRITE_API_AT_LEAST_ONCE=false
export ENABLE_COMMIT_OFFSETS=false
export CONSUMER_GROUP_ID=""
export KAFKA_READ_OFFSET=latest
export KAFKA_READ_USERNAME_SECRET_ID=""
export KAFKA_READ_PASSWORD_SECRET_ID=""
export KAFKA_READ_KEYSTORE_LOCATION=<kafkaReadKeystoreLocation>
export KAFKA_READ_TRUSTSTORE_LOCATION=<kafkaReadTruststoreLocation>
export KAFKA_READ_TRUSTSTORE_PASSWORD_SECRET_ID=<kafkaReadTruststorePasswordSecretId>
export KAFKA_READ_KEYSTORE_PASSWORD_SECRET_ID=<kafkaReadKeystorePasswordSecretId>
export KAFKA_READ_KEY_PASSWORD_SECRET_ID=<kafkaReadKeyPasswordSecretId>
export SCHEMA_FORMAT=SINGLE_SCHEMA_FILE
export CONFLUENT_AVRO_SCHEMA_PATH=""
export SCHEMA_REGISTRY_CONNECTION_URL=""
export BINARY_AVRO_SCHEMA_PATH=""
export SCHEMA_REGISTRY_AUTHENTICATION_MODE=NONE
export SCHEMA_REGISTRY_TRUSTSTORE_LOCATION=<schemaRegistryTruststoreLocation>
export SCHEMA_REGISTRY_TRUSTSTORE_PASSWORD_SECRET_ID=<schemaRegistryTruststorePasswordSecretId>
export SCHEMA_REGISTRY_KEYSTORE_LOCATION=<schemaRegistryKeystoreLocation>
export SCHEMA_REGISTRY_KEYSTORE_PASSWORD_SECRET_ID=<schemaRegistryKeystorePasswordSecretId>
export SCHEMA_REGISTRY_KEY_PASSWORD_SECRET_ID=<schemaRegistryKeyPasswordSecretId>
export OUTPUT_DEADLETTER_TABLE=<outputDeadletterTable>

gcloud dataflow flex-template run "kafka-to-bigquery-flex-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "readBootstrapServerAndTopic=$READ_BOOTSTRAP_SERVER_AND_TOPIC" \
  --parameters "outputTableSpec=$OUTPUT_TABLE_SPEC" \
  --parameters "persistKafkaKey=$PERSIST_KAFKA_KEY" \
  --parameters "writeMode=$WRITE_MODE" \
  --parameters "outputProject=$OUTPUT_PROJECT" \
  --parameters "outputDataset=$OUTPUT_DATASET" \
  --parameters "bqTableNamePrefix=$BQ_TABLE_NAME_PREFIX" \
  --parameters "createDisposition=$CREATE_DISPOSITION" \
  --parameters "writeDisposition=$WRITE_DISPOSITION" \
  --parameters "useAutoSharding=$USE_AUTO_SHARDING" \
  --parameters "numStorageWriteApiStreams=$NUM_STORAGE_WRITE_API_STREAMS" \
  --parameters "storageWriteApiTriggeringFrequencySec=$STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC" \
  --parameters "useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE" \
  --parameters "enableCommitOffsets=$ENABLE_COMMIT_OFFSETS" \
  --parameters "consumerGroupId=$CONSUMER_GROUP_ID" \
  --parameters "kafkaReadOffset=$KAFKA_READ_OFFSET" \
  --parameters "kafkaReadAuthenticationMode=$KAFKA_READ_AUTHENTICATION_MODE" \
  --parameters "kafkaReadUsernameSecretId=$KAFKA_READ_USERNAME_SECRET_ID" \
  --parameters "kafkaReadPasswordSecretId=$KAFKA_READ_PASSWORD_SECRET_ID" \
  --parameters "kafkaReadKeystoreLocation=$KAFKA_READ_KEYSTORE_LOCATION" \
  --parameters "kafkaReadTruststoreLocation=$KAFKA_READ_TRUSTSTORE_LOCATION" \
  --parameters "kafkaReadTruststorePasswordSecretId=$KAFKA_READ_TRUSTSTORE_PASSWORD_SECRET_ID" \
  --parameters "kafkaReadKeystorePasswordSecretId=$KAFKA_READ_KEYSTORE_PASSWORD_SECRET_ID" \
  --parameters "kafkaReadKeyPasswordSecretId=$KAFKA_READ_KEY_PASSWORD_SECRET_ID" \
  --parameters "messageFormat=$MESSAGE_FORMAT" \
  --parameters "schemaFormat=$SCHEMA_FORMAT" \
  --parameters "confluentAvroSchemaPath=$CONFLUENT_AVRO_SCHEMA_PATH" \
  --parameters "schemaRegistryConnectionUrl=$SCHEMA_REGISTRY_CONNECTION_URL" \
  --parameters "binaryAvroSchemaPath=$BINARY_AVRO_SCHEMA_PATH" \
  --parameters "schemaRegistryAuthenticationMode=$SCHEMA_REGISTRY_AUTHENTICATION_MODE" \
  --parameters "schemaRegistryTruststoreLocation=$SCHEMA_REGISTRY_TRUSTSTORE_LOCATION" \
  --parameters "schemaRegistryTruststorePasswordSecretId=$SCHEMA_REGISTRY_TRUSTSTORE_PASSWORD_SECRET_ID" \
  --parameters "schemaRegistryKeystoreLocation=$SCHEMA_REGISTRY_KEYSTORE_LOCATION" \
  --parameters "schemaRegistryKeystorePasswordSecretId=$SCHEMA_REGISTRY_KEYSTORE_PASSWORD_SECRET_ID" \
  --parameters "schemaRegistryKeyPasswordSecretId=$SCHEMA_REGISTRY_KEY_PASSWORD_SECRET_ID" \
  --parameters "outputDeadletterTable=$OUTPUT_DEADLETTER_TABLE" \
  --parameters "useBigQueryDLQ=$USE_BIG_QUERY_DLQ"
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
export READ_BOOTSTRAP_SERVER_AND_TOPIC=<readBootstrapServerAndTopic>
export WRITE_MODE=SINGLE_TABLE_NAME
export KAFKA_READ_AUTHENTICATION_MODE=SASL_PLAIN
export MESSAGE_FORMAT=AVRO_CONFLUENT_WIRE_FORMAT
export USE_BIG_QUERY_DLQ=false

### Optional
export OUTPUT_TABLE_SPEC=<outputTableSpec>
export PERSIST_KAFKA_KEY=false
export OUTPUT_PROJECT=""
export OUTPUT_DATASET=""
export BQ_TABLE_NAME_PREFIX=""
export CREATE_DISPOSITION=CREATE_IF_NEEDED
export WRITE_DISPOSITION=WRITE_APPEND
export USE_AUTO_SHARDING=true
export NUM_STORAGE_WRITE_API_STREAMS=0
export STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC=<storageWriteApiTriggeringFrequencySec>
export USE_STORAGE_WRITE_API_AT_LEAST_ONCE=false
export ENABLE_COMMIT_OFFSETS=false
export CONSUMER_GROUP_ID=""
export KAFKA_READ_OFFSET=latest
export KAFKA_READ_USERNAME_SECRET_ID=""
export KAFKA_READ_PASSWORD_SECRET_ID=""
export KAFKA_READ_KEYSTORE_LOCATION=<kafkaReadKeystoreLocation>
export KAFKA_READ_TRUSTSTORE_LOCATION=<kafkaReadTruststoreLocation>
export KAFKA_READ_TRUSTSTORE_PASSWORD_SECRET_ID=<kafkaReadTruststorePasswordSecretId>
export KAFKA_READ_KEYSTORE_PASSWORD_SECRET_ID=<kafkaReadKeystorePasswordSecretId>
export KAFKA_READ_KEY_PASSWORD_SECRET_ID=<kafkaReadKeyPasswordSecretId>
export SCHEMA_FORMAT=SINGLE_SCHEMA_FILE
export CONFLUENT_AVRO_SCHEMA_PATH=""
export SCHEMA_REGISTRY_CONNECTION_URL=""
export BINARY_AVRO_SCHEMA_PATH=""
export SCHEMA_REGISTRY_AUTHENTICATION_MODE=NONE
export SCHEMA_REGISTRY_TRUSTSTORE_LOCATION=<schemaRegistryTruststoreLocation>
export SCHEMA_REGISTRY_TRUSTSTORE_PASSWORD_SECRET_ID=<schemaRegistryTruststorePasswordSecretId>
export SCHEMA_REGISTRY_KEYSTORE_LOCATION=<schemaRegistryKeystoreLocation>
export SCHEMA_REGISTRY_KEYSTORE_PASSWORD_SECRET_ID=<schemaRegistryKeystorePasswordSecretId>
export SCHEMA_REGISTRY_KEY_PASSWORD_SECRET_ID=<schemaRegistryKeyPasswordSecretId>
export OUTPUT_DEADLETTER_TABLE=<outputDeadletterTable>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="kafka-to-bigquery-flex-job" \
-DtemplateName="Kafka_to_BigQuery_Flex" \
-Dparameters="readBootstrapServerAndTopic=$READ_BOOTSTRAP_SERVER_AND_TOPIC,outputTableSpec=$OUTPUT_TABLE_SPEC,persistKafkaKey=$PERSIST_KAFKA_KEY,writeMode=$WRITE_MODE,outputProject=$OUTPUT_PROJECT,outputDataset=$OUTPUT_DATASET,bqTableNamePrefix=$BQ_TABLE_NAME_PREFIX,createDisposition=$CREATE_DISPOSITION,writeDisposition=$WRITE_DISPOSITION,useAutoSharding=$USE_AUTO_SHARDING,numStorageWriteApiStreams=$NUM_STORAGE_WRITE_API_STREAMS,storageWriteApiTriggeringFrequencySec=$STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC,useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE,enableCommitOffsets=$ENABLE_COMMIT_OFFSETS,consumerGroupId=$CONSUMER_GROUP_ID,kafkaReadOffset=$KAFKA_READ_OFFSET,kafkaReadAuthenticationMode=$KAFKA_READ_AUTHENTICATION_MODE,kafkaReadUsernameSecretId=$KAFKA_READ_USERNAME_SECRET_ID,kafkaReadPasswordSecretId=$KAFKA_READ_PASSWORD_SECRET_ID,kafkaReadKeystoreLocation=$KAFKA_READ_KEYSTORE_LOCATION,kafkaReadTruststoreLocation=$KAFKA_READ_TRUSTSTORE_LOCATION,kafkaReadTruststorePasswordSecretId=$KAFKA_READ_TRUSTSTORE_PASSWORD_SECRET_ID,kafkaReadKeystorePasswordSecretId=$KAFKA_READ_KEYSTORE_PASSWORD_SECRET_ID,kafkaReadKeyPasswordSecretId=$KAFKA_READ_KEY_PASSWORD_SECRET_ID,messageFormat=$MESSAGE_FORMAT,schemaFormat=$SCHEMA_FORMAT,confluentAvroSchemaPath=$CONFLUENT_AVRO_SCHEMA_PATH,schemaRegistryConnectionUrl=$SCHEMA_REGISTRY_CONNECTION_URL,binaryAvroSchemaPath=$BINARY_AVRO_SCHEMA_PATH,schemaRegistryAuthenticationMode=$SCHEMA_REGISTRY_AUTHENTICATION_MODE,schemaRegistryTruststoreLocation=$SCHEMA_REGISTRY_TRUSTSTORE_LOCATION,schemaRegistryTruststorePasswordSecretId=$SCHEMA_REGISTRY_TRUSTSTORE_PASSWORD_SECRET_ID,schemaRegistryKeystoreLocation=$SCHEMA_REGISTRY_KEYSTORE_LOCATION,schemaRegistryKeystorePasswordSecretId=$SCHEMA_REGISTRY_KEYSTORE_PASSWORD_SECRET_ID,schemaRegistryKeyPasswordSecretId=$SCHEMA_REGISTRY_KEY_PASSWORD_SECRET_ID,outputDeadletterTable=$OUTPUT_DEADLETTER_TABLE,useBigQueryDLQ=$USE_BIG_QUERY_DLQ" \
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
    readBootstrapServerAndTopic = "<readBootstrapServerAndTopic>"
    writeMode = "SINGLE_TABLE_NAME"
    kafkaReadAuthenticationMode = "SASL_PLAIN"
    messageFormat = "AVRO_CONFLUENT_WIRE_FORMAT"
    useBigQueryDLQ = "false"
    # outputTableSpec = "<outputTableSpec>"
    # persistKafkaKey = "false"
    # outputProject = ""
    # outputDataset = ""
    # bqTableNamePrefix = ""
    # createDisposition = "CREATE_IF_NEEDED"
    # writeDisposition = "WRITE_APPEND"
    # useAutoSharding = "true"
    # numStorageWriteApiStreams = "0"
    # storageWriteApiTriggeringFrequencySec = "<storageWriteApiTriggeringFrequencySec>"
    # useStorageWriteApiAtLeastOnce = "false"
    # enableCommitOffsets = "false"
    # consumerGroupId = ""
    # kafkaReadOffset = "latest"
    # kafkaReadUsernameSecretId = "projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>"
    # kafkaReadPasswordSecretId = "projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>"
    # kafkaReadKeystoreLocation = "gs://your-bucket/keystore.jks"
    # kafkaReadTruststoreLocation = "<kafkaReadTruststoreLocation>"
    # kafkaReadTruststorePasswordSecretId = "projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>"
    # kafkaReadKeystorePasswordSecretId = "projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>"
    # kafkaReadKeyPasswordSecretId = "projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>"
    # schemaFormat = "SINGLE_SCHEMA_FILE"
    # confluentAvroSchemaPath = ""
    # schemaRegistryConnectionUrl = ""
    # binaryAvroSchemaPath = ""
    # schemaRegistryAuthenticationMode = "NONE"
    # schemaRegistryTruststoreLocation = "/your-bucket/truststore.jks"
    # schemaRegistryTruststorePasswordSecretId = "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version"
    # schemaRegistryKeystoreLocation = "/your-bucket/keystore.jks"
    # schemaRegistryKeystorePasswordSecretId = "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version"
    # schemaRegistryKeyPasswordSecretId = "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version"
    # outputDeadletterTable = "your-project-id:your-dataset.your-table-name"
  }
}
```
