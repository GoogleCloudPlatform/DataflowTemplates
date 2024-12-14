
Kafka to Cloud Storage template
---
A streaming pipeline which ingests data from Kafka and writes to a pre-existing
Cloud Storage bucket with a variety of file types.



:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **readBootstrapServerAndTopic**: Kafka Topic to read the input from.
* **outputDirectory**: The path and filename prefix for writing output files. Must end with a slash. For example, `gs://your-bucket/your-path/`.
* **kafkaReadAuthenticationMode**: The mode of authentication to use with the Kafka cluster. Use `KafkaAuthenticationMethod.NONE` for no authentication, `KafkaAuthenticationMethod.SASL_PLAIN` for SASL/PLAIN username and password, and `KafkaAuthenticationMethod.TLS` for certificate-based authentication. `KafkaAuthenticationMethod.APPLICATION_DEFAULT_CREDENTIALS` should be used only for Google Cloud Apache Kafka for BigQuery cluster, it allows to authenticate using application default credentials.
* **messageFormat**: The format of the Kafka messages to read. The supported values are `AVRO_CONFLUENT_WIRE_FORMAT` (Confluent Schema Registry encoded Avro), `AVRO_BINARY_ENCODING` (Plain binary Avro), and `JSON`. Defaults to: AVRO_CONFLUENT_WIRE_FORMAT.
* **useBigQueryDLQ**: If true, failed messages will be written to BigQuery with extra error information. Defaults to: false.

### Optional parameters

* **windowDuration**: The window duration/size in which data will be written to Cloud Storage. Allowed formats are: Ns (for seconds, example: 5s), Nm (for minutes, example: 12m), Nh (for hours, example: 2h). For example, `5m`. Defaults to: 5m.
* **outputFilenamePrefix**: The prefix to place on each windowed file. For example, `output-`. Defaults to: output.
* **numShards**: The maximum number of output shards produced when writing. A higher number of shards means higher throughput for writing to Cloud Storage, but potentially higher data aggregation cost across shards when processing output Cloud Storage files. Default value is decided by Dataflow.
* **enableCommitOffsets**: Commit offsets of processed messages to Kafka. If enabled, this will minimize the gaps or duplicate processing of messages when restarting the pipeline. Requires specifying the Consumer Group ID. Defaults to: false.
* **consumerGroupId**: The unique identifier for the consumer group that this pipeline belongs to. Required if Commit Offsets to Kafka is enabled. Defaults to empty.
* **kafkaReadOffset**: The starting point for reading messages when no committed offsets exist. The earliest starts from the beginning, the latest from the newest message. Defaults to: latest.
* **kafkaReadUsernameSecretId**: The Google Cloud Secret Manager secret ID that contains the Kafka username to use with `SASL_PLAIN` authentication. For example, `projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>`. Defaults to empty.
* **kafkaReadPasswordSecretId**: The Google Cloud Secret Manager secret ID that contains the Kafka password to use with `SASL_PLAIN` authentication. For example, `projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>`. Defaults to empty.
* **kafkaReadKeystoreLocation**: The Google Cloud Storage path to the Java KeyStore (JKS) file that contains the TLS certificate and private key to use when authenticating with the Kafka cluster. For example, `gs://your-bucket/keystore.jks`.
* **kafkaReadTruststoreLocation**: The Google Cloud Storage path to the Java TrustStore (JKS) file that contains the trusted certificates to use to verify the identity of the Kafka broker.
* **kafkaReadTruststorePasswordSecretId**: The Google Cloud Secret Manager secret ID that contains the password to use to access the Java TrustStore (JKS) file for Kafka TLS authentication For example, `projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>`.
* **kafkaReadKeystorePasswordSecretId**: The Google Cloud Secret Manager secret ID that contains the password to use to access the Java KeyStore (JKS) file for Kafka TLS authentication. For example, `projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>`.
* **kafkaReadKeyPasswordSecretId**: The Google Cloud Secret Manager secret ID that contains the password to use to access the private key within the Java KeyStore (JKS) file for Kafka TLS authentication. For example, `projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>`.
* **schemaFormat**: The Kafka schema format. Can be provided as `SINGLE_SCHEMA_FILE` or `SCHEMA_REGISTRY`. If `SINGLE_SCHEMA_FILE` is specified, use the schema mentioned in the avro schema file for all messages. If `SCHEMA_REGISTRY` is specified, the messages can have either a single schema or multiple schemas. Defaults to: SINGLE_SCHEMA_FILE.
* **confluentAvroSchemaPath**: The Google Cloud Storage path to the single Avro schema file used to decode all of the messages in a topic. Defaults to empty.
* **schemaRegistryConnectionUrl**: The URL for the Confluent Schema Registry instance used to manage Avro schemas for message decoding. Defaults to empty.
* **binaryAvroSchemaPath**: The Google Cloud Storage path to the Avro schema file used to decode binary-encoded Avro messages. Defaults to empty.
* **schemaRegistryAuthenticationMode**: Schema Registry authentication mode. Can be NONE, TLS or OAUTH. Defaults to: NONE.
* **schemaRegistryTruststoreLocation**: Location of the SSL certificate where the trust store for authentication to Schema Registry are stored. For example, `/your-bucket/truststore.jks`.
* **schemaRegistryTruststorePasswordSecretId**: SecretId in secret manager where the password to access secret in truststore is stored. For example, `projects/your-project-number/secrets/your-secret-name/versions/your-secret-version`.
* **schemaRegistryKeystoreLocation**: Keystore location that contains the SSL certificate and private key. For example, `/your-bucket/keystore.jks`.
* **schemaRegistryKeystorePasswordSecretId**: SecretId in secret manager where the password to access the keystore file For example, `projects/your-project-number/secrets/your-secret-name/versions/your-secret-version`.
* **schemaRegistryKeyPasswordSecretId**: SecretId of password required to access the client's private key stored within the keystore For example, `projects/your-project-number/secrets/your-secret-name/versions/your-secret-version`.
* **schemaRegistryOauthClientId**: Client ID used to authenticate the Schema Registry client in OAUTH mode. Required for AVRO_CONFLUENT_WIRE_FORMAT message format.
* **schemaRegistryOauthClientSecretId**: The Google Cloud Secret Manager secret ID that contains the Client Secret to use to authenticate the Schema Registry client in OAUTH mode. Required for AVRO_CONFLUENT_WIRE_FORMAT message format. For example, `projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>`.
* **schemaRegistryOauthScope**: The access token scope used to authenticate the Schema Registry client in OAUTH mode. This field is optional, as the request can be made without a scope parameter passed. For example, `openid`.
* **schemaRegistryOauthTokenEndpointUrl**: The HTTP(S)-based URL for the OAuth/OIDC identity provider used to authenticate the Schema Registry client in OAUTH mode. Required for AVRO_CONFLUENT_WIRE_FORMAT message format.
* **outputDeadletterTable**: Fully Qualified BigQuery table name for failed messages. Messages failed to reach the output table for different reasons (e.g., mismatched schema, malformed json) are written to this table.The table will be created by the template. For example, `your-project-id:your-dataset.your-table-name`.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/kafka-to-gcs/src/main/java/com/google/cloud/teleport/v2/templates/KafkaToGcsFlex.java)

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
-DtemplateName="Kafka_to_Gcs_Flex" \
-f v2/kafka-to-gcs
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Kafka_to_Gcs_Flex
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Kafka_to_Gcs_Flex"

### Required
export READ_BOOTSTRAP_SERVER_AND_TOPIC=<readBootstrapServerAndTopic>
export OUTPUT_DIRECTORY=<outputDirectory>
export KAFKA_READ_AUTHENTICATION_MODE=SASL_PLAIN
export MESSAGE_FORMAT=AVRO_CONFLUENT_WIRE_FORMAT
export USE_BIG_QUERY_DLQ=false

### Optional
export WINDOW_DURATION=5m
export OUTPUT_FILENAME_PREFIX=output
export NUM_SHARDS=0
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
export SCHEMA_REGISTRY_OAUTH_CLIENT_ID=<schemaRegistryOauthClientId>
export SCHEMA_REGISTRY_OAUTH_CLIENT_SECRET_ID=<schemaRegistryOauthClientSecretId>
export SCHEMA_REGISTRY_OAUTH_SCOPE=<schemaRegistryOauthScope>
export SCHEMA_REGISTRY_OAUTH_TOKEN_ENDPOINT_URL=<schemaRegistryOauthTokenEndpointUrl>
export OUTPUT_DEADLETTER_TABLE=<outputDeadletterTable>

gcloud dataflow flex-template run "kafka-to-gcs-flex-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "readBootstrapServerAndTopic=$READ_BOOTSTRAP_SERVER_AND_TOPIC" \
  --parameters "windowDuration=$WINDOW_DURATION" \
  --parameters "outputDirectory=$OUTPUT_DIRECTORY" \
  --parameters "outputFilenamePrefix=$OUTPUT_FILENAME_PREFIX" \
  --parameters "numShards=$NUM_SHARDS" \
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
  --parameters "schemaRegistryOauthClientId=$SCHEMA_REGISTRY_OAUTH_CLIENT_ID" \
  --parameters "schemaRegistryOauthClientSecretId=$SCHEMA_REGISTRY_OAUTH_CLIENT_SECRET_ID" \
  --parameters "schemaRegistryOauthScope=$SCHEMA_REGISTRY_OAUTH_SCOPE" \
  --parameters "schemaRegistryOauthTokenEndpointUrl=$SCHEMA_REGISTRY_OAUTH_TOKEN_ENDPOINT_URL" \
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
export OUTPUT_DIRECTORY=<outputDirectory>
export KAFKA_READ_AUTHENTICATION_MODE=SASL_PLAIN
export MESSAGE_FORMAT=AVRO_CONFLUENT_WIRE_FORMAT
export USE_BIG_QUERY_DLQ=false

### Optional
export WINDOW_DURATION=5m
export OUTPUT_FILENAME_PREFIX=output
export NUM_SHARDS=0
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
export SCHEMA_REGISTRY_OAUTH_CLIENT_ID=<schemaRegistryOauthClientId>
export SCHEMA_REGISTRY_OAUTH_CLIENT_SECRET_ID=<schemaRegistryOauthClientSecretId>
export SCHEMA_REGISTRY_OAUTH_SCOPE=<schemaRegistryOauthScope>
export SCHEMA_REGISTRY_OAUTH_TOKEN_ENDPOINT_URL=<schemaRegistryOauthTokenEndpointUrl>
export OUTPUT_DEADLETTER_TABLE=<outputDeadletterTable>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="kafka-to-gcs-flex-job" \
-DtemplateName="Kafka_to_Gcs_Flex" \
-Dparameters="readBootstrapServerAndTopic=$READ_BOOTSTRAP_SERVER_AND_TOPIC,windowDuration=$WINDOW_DURATION,outputDirectory=$OUTPUT_DIRECTORY,outputFilenamePrefix=$OUTPUT_FILENAME_PREFIX,numShards=$NUM_SHARDS,enableCommitOffsets=$ENABLE_COMMIT_OFFSETS,consumerGroupId=$CONSUMER_GROUP_ID,kafkaReadOffset=$KAFKA_READ_OFFSET,kafkaReadAuthenticationMode=$KAFKA_READ_AUTHENTICATION_MODE,kafkaReadUsernameSecretId=$KAFKA_READ_USERNAME_SECRET_ID,kafkaReadPasswordSecretId=$KAFKA_READ_PASSWORD_SECRET_ID,kafkaReadKeystoreLocation=$KAFKA_READ_KEYSTORE_LOCATION,kafkaReadTruststoreLocation=$KAFKA_READ_TRUSTSTORE_LOCATION,kafkaReadTruststorePasswordSecretId=$KAFKA_READ_TRUSTSTORE_PASSWORD_SECRET_ID,kafkaReadKeystorePasswordSecretId=$KAFKA_READ_KEYSTORE_PASSWORD_SECRET_ID,kafkaReadKeyPasswordSecretId=$KAFKA_READ_KEY_PASSWORD_SECRET_ID,messageFormat=$MESSAGE_FORMAT,schemaFormat=$SCHEMA_FORMAT,confluentAvroSchemaPath=$CONFLUENT_AVRO_SCHEMA_PATH,schemaRegistryConnectionUrl=$SCHEMA_REGISTRY_CONNECTION_URL,binaryAvroSchemaPath=$BINARY_AVRO_SCHEMA_PATH,schemaRegistryAuthenticationMode=$SCHEMA_REGISTRY_AUTHENTICATION_MODE,schemaRegistryTruststoreLocation=$SCHEMA_REGISTRY_TRUSTSTORE_LOCATION,schemaRegistryTruststorePasswordSecretId=$SCHEMA_REGISTRY_TRUSTSTORE_PASSWORD_SECRET_ID,schemaRegistryKeystoreLocation=$SCHEMA_REGISTRY_KEYSTORE_LOCATION,schemaRegistryKeystorePasswordSecretId=$SCHEMA_REGISTRY_KEYSTORE_PASSWORD_SECRET_ID,schemaRegistryKeyPasswordSecretId=$SCHEMA_REGISTRY_KEY_PASSWORD_SECRET_ID,schemaRegistryOauthClientId=$SCHEMA_REGISTRY_OAUTH_CLIENT_ID,schemaRegistryOauthClientSecretId=$SCHEMA_REGISTRY_OAUTH_CLIENT_SECRET_ID,schemaRegistryOauthScope=$SCHEMA_REGISTRY_OAUTH_SCOPE,schemaRegistryOauthTokenEndpointUrl=$SCHEMA_REGISTRY_OAUTH_TOKEN_ENDPOINT_URL,outputDeadletterTable=$OUTPUT_DEADLETTER_TABLE,useBigQueryDLQ=$USE_BIG_QUERY_DLQ" \
-f v2/kafka-to-gcs
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
cd v2/kafka-to-gcs/terraform/Kafka_to_Gcs_Flex
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

resource "google_dataflow_flex_template_job" "kafka_to_gcs_flex" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Kafka_to_Gcs_Flex"
  name              = "kafka-to-gcs-flex"
  region            = var.region
  parameters        = {
    readBootstrapServerAndTopic = "<readBootstrapServerAndTopic>"
    outputDirectory = "<outputDirectory>"
    kafkaReadAuthenticationMode = "SASL_PLAIN"
    messageFormat = "AVRO_CONFLUENT_WIRE_FORMAT"
    useBigQueryDLQ = "false"
    # windowDuration = "5m"
    # outputFilenamePrefix = "output"
    # numShards = "0"
    # enableCommitOffsets = "false"
    # consumerGroupId = ""
    # kafkaReadOffset = "latest"
    # kafkaReadUsernameSecretId = ""
    # kafkaReadPasswordSecretId = ""
    # kafkaReadKeystoreLocation = "<kafkaReadKeystoreLocation>"
    # kafkaReadTruststoreLocation = "<kafkaReadTruststoreLocation>"
    # kafkaReadTruststorePasswordSecretId = "<kafkaReadTruststorePasswordSecretId>"
    # kafkaReadKeystorePasswordSecretId = "<kafkaReadKeystorePasswordSecretId>"
    # kafkaReadKeyPasswordSecretId = "<kafkaReadKeyPasswordSecretId>"
    # schemaFormat = "SINGLE_SCHEMA_FILE"
    # confluentAvroSchemaPath = ""
    # schemaRegistryConnectionUrl = ""
    # binaryAvroSchemaPath = ""
    # schemaRegistryAuthenticationMode = "NONE"
    # schemaRegistryTruststoreLocation = "<schemaRegistryTruststoreLocation>"
    # schemaRegistryTruststorePasswordSecretId = "<schemaRegistryTruststorePasswordSecretId>"
    # schemaRegistryKeystoreLocation = "<schemaRegistryKeystoreLocation>"
    # schemaRegistryKeystorePasswordSecretId = "<schemaRegistryKeystorePasswordSecretId>"
    # schemaRegistryKeyPasswordSecretId = "<schemaRegistryKeyPasswordSecretId>"
    # schemaRegistryOauthClientId = "<schemaRegistryOauthClientId>"
    # schemaRegistryOauthClientSecretId = "<schemaRegistryOauthClientSecretId>"
    # schemaRegistryOauthScope = "<schemaRegistryOauthScope>"
    # schemaRegistryOauthTokenEndpointUrl = "<schemaRegistryOauthTokenEndpointUrl>"
    # outputDeadletterTable = "<outputDeadletterTable>"
  }
}
```
