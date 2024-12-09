
Cloud Bigtable Change Streams to Apache Kafka template
---
Streaming pipeline. Streams Bigtable data change records and writes them into
Kafka using Dataflow runner V2.



:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **bigtableChangeStreamAppProfile**: The Bigtable application profile ID. The application profile must use single-cluster routing and allow single-row transactions.
* **bigtableReadInstanceId**: The source Bigtable instance ID.
* **bigtableReadTableId**: The source Bigtable table ID.
* **writeBootstrapServerAndTopic**: Kafka topic to write the output to.
* **kafkaWriteAuthenticationMethod**: The mode of authentication to use with the Kafka cluster. Use NONE for no authentication, SASL_PLAIN for SASL/PLAIN username and password,  SASL_SCRAM_512 for SASL_SCRAM_512 based authentication and TLS for certificate-based authentication. Defaults to: APPLICATION_DEFAULT_CREDENTIALS.
* **messageFormat**: The format of the Kafka messages to read. The supported values are `AVRO_CONFLUENT_WIRE_FORMAT` (Confluent Schema Registry encoded Avro), `AVRO_BINARY_ENCODING` (Plain binary Avro), and `JSON`. Defaults to: AVRO_CONFLUENT_WIRE_FORMAT.

### Optional parameters

* **dlqDirectory**: The directory for the dead-letter queue. Records that fail to be processed are stored in this directory. Defaults to a directory under the Dataflow job temp location. In most cases, you can use the default path.
* **dlqRetryMinutes**: The number of minutes between dead-letter queue retries. Defaults to `10`.
* **dlqMaxRetries**: The dead letter maximum retries. Defaults to `5`.
* **disableDlqRetries**: Whether or not to disable retries for the DLQ. Defaults to: false.
* **bigtableChangeStreamMetadataInstanceId**: The Bigtable change streams metadata instance ID. Defaults to empty.
* **bigtableChangeStreamMetadataTableTableId**: The ID of the Bigtable change streams connector metadata table. If not provided, a Bigtable change streams connector metadata table is automatically created during pipeline execution. Defaults to empty.
* **bigtableChangeStreamCharset**: The Bigtable change streams charset name. Defaults to: UTF-8.
* **bigtableChangeStreamStartTimestamp**: The starting timestamp (https://tools.ietf.org/html/rfc3339), inclusive, to use for reading change streams. For example, `2022-05-05T07:59:59Z`. Defaults to the timestamp of the pipeline start time.
* **bigtableChangeStreamIgnoreColumnFamilies**: A comma-separated list of column family name changes to ignore. Defaults to empty.
* **bigtableChangeStreamIgnoreColumns**: A comma-separated list of column name changes to ignore. Example: "cf1:col1,cf2:col2". Defaults to empty.
* **bigtableChangeStreamName**: A unique name for the client pipeline. Lets you resume processing from the point at which a previously running pipeline stopped. Defaults to an automatically generated name. See the Dataflow job logs for the value used.
* **bigtableChangeStreamResume**: When set to `true`, a new pipeline resumes processing from the point at which a previously running pipeline with the same `bigtableChangeStreamName` value stopped. If the pipeline with the given `bigtableChangeStreamName` value has never run, a new pipeline doesn't start. When set to `false`, a new pipeline starts. If a pipeline with the same `bigtableChangeStreamName` value has already run for the given source, a new pipeline doesn't start. Defaults to `false`.
* **bigtableReadProjectId**: The Bigtable project ID. The default is the project for the Dataflow job.
* **kafkaWriteUsernameSecretId**: The Google Cloud Secret Manager secret ID that contains the Kafka username  for SASL_PLAIN authentication with the destination Kafka cluster. For example, `projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>`. Defaults to empty.
* **kafkaWritePasswordSecretId**: The Google Cloud Secret Manager secret ID that contains the Kafka password to use for SASL_PLAIN authentication with the destination Kafka cluster. For example, `projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>`. Defaults to empty.
* **kafkaWriteKeystoreLocation**: The Google Cloud Storage path to the Java KeyStore (JKS) file that contains the TLS certificate and private key for authenticating with the destination Kafka cluster. For example, `gs://<BUCKET>/<KEYSTORE>.jks`.
* **kafkaWriteTruststoreLocation**: The Google Cloud Storage path to the Java TrustStore (JKS) file that contains the trusted certificates to use to verify the identity of the destination Kafka broker.
* **kafkaWriteTruststorePasswordSecretId**: The Google Cloud Secret Manager secret ID that contains the password to use to access the Java TrustStore (JKS) file for TLS authentication with the destination Kafka cluster. For example, `projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>`.
* **kafkaWriteKeystorePasswordSecretId**: The Google Cloud Secret Manager secret ID that contains the password to access the Java KeyStore (JKS) file to use for TLS authentication with the destination Kafka cluster. For example, `projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>`.
* **kafkaWriteKeyPasswordSecretId**: The Google Cloud Secret Manager secret ID that contains the password to use to access the private key within the Java KeyStore (JKS) file for TLS authentication with the destination Kafka cluster. For example, `projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>`.
* **kafkaReadSaslScramUsernameSecretId**: The Google Cloud Secret Manager secret ID that contains the Kafka username to use with `SASL_SCRAM` authentication. For example, `projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>`.
* **kafkaReadSaslScramPasswordSecretId**: The Google Cloud Secret Manager secret ID that contains the Kafka password to use with `SASL_SCRAM` authentication. For example, `projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>`.
* **kafkaReadSaslScramTruststoreLocation**: The Google Cloud Storage path to the Java TrustStore (JKS) file that contains the trusted certificates to use to verify the identity of the Kafka broker.
* **kafkaReadSaslScramTruststorePasswordSecretId**: The Google Cloud Secret Manager secret ID that contains the password to use to access the Java TrustStore (JKS) file for Kafka SASL_SCRAM authentication For example, `projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>`.
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



## Getting Started

### Requirements

* Java 17
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/bigtable-changestreams-to-kafka/src/main/java/com/google/cloud/teleport/v2/templates/BigtableChangeStreamsToKafka.java)

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
-DtemplateName="Bigtable_Change_Streams_to_Kafka" \
-f v2/bigtable-changestreams-to-kafka
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Bigtable_Change_Streams_to_Kafka
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Bigtable_Change_Streams_to_Kafka"

### Required
export BIGTABLE_CHANGE_STREAM_APP_PROFILE=<bigtableChangeStreamAppProfile>
export BIGTABLE_READ_INSTANCE_ID=<bigtableReadInstanceId>
export BIGTABLE_READ_TABLE_ID=<bigtableReadTableId>
export WRITE_BOOTSTRAP_SERVER_AND_TOPIC=<writeBootstrapServerAndTopic>
export KAFKA_WRITE_AUTHENTICATION_METHOD=APPLICATION_DEFAULT_CREDENTIALS
export MESSAGE_FORMAT=AVRO_CONFLUENT_WIRE_FORMAT

### Optional
export DLQ_DIRECTORY=""
export DLQ_RETRY_MINUTES=10
export DLQ_MAX_RETRIES=5
export DISABLE_DLQ_RETRIES=false
export BIGTABLE_CHANGE_STREAM_METADATA_INSTANCE_ID=""
export BIGTABLE_CHANGE_STREAM_METADATA_TABLE_TABLE_ID=""
export BIGTABLE_CHANGE_STREAM_CHARSET=UTF-8
export BIGTABLE_CHANGE_STREAM_START_TIMESTAMP=""
export BIGTABLE_CHANGE_STREAM_IGNORE_COLUMN_FAMILIES=""
export BIGTABLE_CHANGE_STREAM_IGNORE_COLUMNS=""
export BIGTABLE_CHANGE_STREAM_NAME=<bigtableChangeStreamName>
export BIGTABLE_CHANGE_STREAM_RESUME=false
export BIGTABLE_READ_PROJECT_ID=""
export KAFKA_WRITE_USERNAME_SECRET_ID=""
export KAFKA_WRITE_PASSWORD_SECRET_ID=""
export KAFKA_WRITE_KEYSTORE_LOCATION=<kafkaWriteKeystoreLocation>
export KAFKA_WRITE_TRUSTSTORE_LOCATION=<kafkaWriteTruststoreLocation>
export KAFKA_WRITE_TRUSTSTORE_PASSWORD_SECRET_ID=<kafkaWriteTruststorePasswordSecretId>
export KAFKA_WRITE_KEYSTORE_PASSWORD_SECRET_ID=<kafkaWriteKeystorePasswordSecretId>
export KAFKA_WRITE_KEY_PASSWORD_SECRET_ID=<kafkaWriteKeyPasswordSecretId>
export KAFKA_READ_SASL_SCRAM_USERNAME_SECRET_ID=<kafkaReadSaslScramUsernameSecretId>
export KAFKA_READ_SASL_SCRAM_PASSWORD_SECRET_ID=<kafkaReadSaslScramPasswordSecretId>
export KAFKA_READ_SASL_SCRAM_TRUSTSTORE_LOCATION=<kafkaReadSaslScramTruststoreLocation>
export KAFKA_READ_SASL_SCRAM_TRUSTSTORE_PASSWORD_SECRET_ID=<kafkaReadSaslScramTruststorePasswordSecretId>
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

gcloud dataflow flex-template run "bigtable-change-streams-to-kafka-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "dlqDirectory=$DLQ_DIRECTORY" \
  --parameters "dlqRetryMinutes=$DLQ_RETRY_MINUTES" \
  --parameters "dlqMaxRetries=$DLQ_MAX_RETRIES" \
  --parameters "disableDlqRetries=$DISABLE_DLQ_RETRIES" \
  --parameters "bigtableChangeStreamMetadataInstanceId=$BIGTABLE_CHANGE_STREAM_METADATA_INSTANCE_ID" \
  --parameters "bigtableChangeStreamMetadataTableTableId=$BIGTABLE_CHANGE_STREAM_METADATA_TABLE_TABLE_ID" \
  --parameters "bigtableChangeStreamAppProfile=$BIGTABLE_CHANGE_STREAM_APP_PROFILE" \
  --parameters "bigtableChangeStreamCharset=$BIGTABLE_CHANGE_STREAM_CHARSET" \
  --parameters "bigtableChangeStreamStartTimestamp=$BIGTABLE_CHANGE_STREAM_START_TIMESTAMP" \
  --parameters "bigtableChangeStreamIgnoreColumnFamilies=$BIGTABLE_CHANGE_STREAM_IGNORE_COLUMN_FAMILIES" \
  --parameters "bigtableChangeStreamIgnoreColumns=$BIGTABLE_CHANGE_STREAM_IGNORE_COLUMNS" \
  --parameters "bigtableChangeStreamName=$BIGTABLE_CHANGE_STREAM_NAME" \
  --parameters "bigtableChangeStreamResume=$BIGTABLE_CHANGE_STREAM_RESUME" \
  --parameters "bigtableReadInstanceId=$BIGTABLE_READ_INSTANCE_ID" \
  --parameters "bigtableReadTableId=$BIGTABLE_READ_TABLE_ID" \
  --parameters "bigtableReadProjectId=$BIGTABLE_READ_PROJECT_ID" \
  --parameters "writeBootstrapServerAndTopic=$WRITE_BOOTSTRAP_SERVER_AND_TOPIC" \
  --parameters "kafkaWriteAuthenticationMethod=$KAFKA_WRITE_AUTHENTICATION_METHOD" \
  --parameters "kafkaWriteUsernameSecretId=$KAFKA_WRITE_USERNAME_SECRET_ID" \
  --parameters "kafkaWritePasswordSecretId=$KAFKA_WRITE_PASSWORD_SECRET_ID" \
  --parameters "kafkaWriteKeystoreLocation=$KAFKA_WRITE_KEYSTORE_LOCATION" \
  --parameters "kafkaWriteTruststoreLocation=$KAFKA_WRITE_TRUSTSTORE_LOCATION" \
  --parameters "kafkaWriteTruststorePasswordSecretId=$KAFKA_WRITE_TRUSTSTORE_PASSWORD_SECRET_ID" \
  --parameters "kafkaWriteKeystorePasswordSecretId=$KAFKA_WRITE_KEYSTORE_PASSWORD_SECRET_ID" \
  --parameters "kafkaWriteKeyPasswordSecretId=$KAFKA_WRITE_KEY_PASSWORD_SECRET_ID" \
  --parameters "kafkaReadSaslScramUsernameSecretId=$KAFKA_READ_SASL_SCRAM_USERNAME_SECRET_ID" \
  --parameters "kafkaReadSaslScramPasswordSecretId=$KAFKA_READ_SASL_SCRAM_PASSWORD_SECRET_ID" \
  --parameters "kafkaReadSaslScramTruststoreLocation=$KAFKA_READ_SASL_SCRAM_TRUSTSTORE_LOCATION" \
  --parameters "kafkaReadSaslScramTruststorePasswordSecretId=$KAFKA_READ_SASL_SCRAM_TRUSTSTORE_PASSWORD_SECRET_ID" \
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
  --parameters "schemaRegistryOauthTokenEndpointUrl=$SCHEMA_REGISTRY_OAUTH_TOKEN_ENDPOINT_URL"
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
export BIGTABLE_CHANGE_STREAM_APP_PROFILE=<bigtableChangeStreamAppProfile>
export BIGTABLE_READ_INSTANCE_ID=<bigtableReadInstanceId>
export BIGTABLE_READ_TABLE_ID=<bigtableReadTableId>
export WRITE_BOOTSTRAP_SERVER_AND_TOPIC=<writeBootstrapServerAndTopic>
export KAFKA_WRITE_AUTHENTICATION_METHOD=APPLICATION_DEFAULT_CREDENTIALS
export MESSAGE_FORMAT=AVRO_CONFLUENT_WIRE_FORMAT

### Optional
export DLQ_DIRECTORY=""
export DLQ_RETRY_MINUTES=10
export DLQ_MAX_RETRIES=5
export DISABLE_DLQ_RETRIES=false
export BIGTABLE_CHANGE_STREAM_METADATA_INSTANCE_ID=""
export BIGTABLE_CHANGE_STREAM_METADATA_TABLE_TABLE_ID=""
export BIGTABLE_CHANGE_STREAM_CHARSET=UTF-8
export BIGTABLE_CHANGE_STREAM_START_TIMESTAMP=""
export BIGTABLE_CHANGE_STREAM_IGNORE_COLUMN_FAMILIES=""
export BIGTABLE_CHANGE_STREAM_IGNORE_COLUMNS=""
export BIGTABLE_CHANGE_STREAM_NAME=<bigtableChangeStreamName>
export BIGTABLE_CHANGE_STREAM_RESUME=false
export BIGTABLE_READ_PROJECT_ID=""
export KAFKA_WRITE_USERNAME_SECRET_ID=""
export KAFKA_WRITE_PASSWORD_SECRET_ID=""
export KAFKA_WRITE_KEYSTORE_LOCATION=<kafkaWriteKeystoreLocation>
export KAFKA_WRITE_TRUSTSTORE_LOCATION=<kafkaWriteTruststoreLocation>
export KAFKA_WRITE_TRUSTSTORE_PASSWORD_SECRET_ID=<kafkaWriteTruststorePasswordSecretId>
export KAFKA_WRITE_KEYSTORE_PASSWORD_SECRET_ID=<kafkaWriteKeystorePasswordSecretId>
export KAFKA_WRITE_KEY_PASSWORD_SECRET_ID=<kafkaWriteKeyPasswordSecretId>
export KAFKA_READ_SASL_SCRAM_USERNAME_SECRET_ID=<kafkaReadSaslScramUsernameSecretId>
export KAFKA_READ_SASL_SCRAM_PASSWORD_SECRET_ID=<kafkaReadSaslScramPasswordSecretId>
export KAFKA_READ_SASL_SCRAM_TRUSTSTORE_LOCATION=<kafkaReadSaslScramTruststoreLocation>
export KAFKA_READ_SASL_SCRAM_TRUSTSTORE_PASSWORD_SECRET_ID=<kafkaReadSaslScramTruststorePasswordSecretId>
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

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="bigtable-change-streams-to-kafka-job" \
-DtemplateName="Bigtable_Change_Streams_to_Kafka" \
-Dparameters="dlqDirectory=$DLQ_DIRECTORY,dlqRetryMinutes=$DLQ_RETRY_MINUTES,dlqMaxRetries=$DLQ_MAX_RETRIES,disableDlqRetries=$DISABLE_DLQ_RETRIES,bigtableChangeStreamMetadataInstanceId=$BIGTABLE_CHANGE_STREAM_METADATA_INSTANCE_ID,bigtableChangeStreamMetadataTableTableId=$BIGTABLE_CHANGE_STREAM_METADATA_TABLE_TABLE_ID,bigtableChangeStreamAppProfile=$BIGTABLE_CHANGE_STREAM_APP_PROFILE,bigtableChangeStreamCharset=$BIGTABLE_CHANGE_STREAM_CHARSET,bigtableChangeStreamStartTimestamp=$BIGTABLE_CHANGE_STREAM_START_TIMESTAMP,bigtableChangeStreamIgnoreColumnFamilies=$BIGTABLE_CHANGE_STREAM_IGNORE_COLUMN_FAMILIES,bigtableChangeStreamIgnoreColumns=$BIGTABLE_CHANGE_STREAM_IGNORE_COLUMNS,bigtableChangeStreamName=$BIGTABLE_CHANGE_STREAM_NAME,bigtableChangeStreamResume=$BIGTABLE_CHANGE_STREAM_RESUME,bigtableReadInstanceId=$BIGTABLE_READ_INSTANCE_ID,bigtableReadTableId=$BIGTABLE_READ_TABLE_ID,bigtableReadProjectId=$BIGTABLE_READ_PROJECT_ID,writeBootstrapServerAndTopic=$WRITE_BOOTSTRAP_SERVER_AND_TOPIC,kafkaWriteAuthenticationMethod=$KAFKA_WRITE_AUTHENTICATION_METHOD,kafkaWriteUsernameSecretId=$KAFKA_WRITE_USERNAME_SECRET_ID,kafkaWritePasswordSecretId=$KAFKA_WRITE_PASSWORD_SECRET_ID,kafkaWriteKeystoreLocation=$KAFKA_WRITE_KEYSTORE_LOCATION,kafkaWriteTruststoreLocation=$KAFKA_WRITE_TRUSTSTORE_LOCATION,kafkaWriteTruststorePasswordSecretId=$KAFKA_WRITE_TRUSTSTORE_PASSWORD_SECRET_ID,kafkaWriteKeystorePasswordSecretId=$KAFKA_WRITE_KEYSTORE_PASSWORD_SECRET_ID,kafkaWriteKeyPasswordSecretId=$KAFKA_WRITE_KEY_PASSWORD_SECRET_ID,kafkaReadSaslScramUsernameSecretId=$KAFKA_READ_SASL_SCRAM_USERNAME_SECRET_ID,kafkaReadSaslScramPasswordSecretId=$KAFKA_READ_SASL_SCRAM_PASSWORD_SECRET_ID,kafkaReadSaslScramTruststoreLocation=$KAFKA_READ_SASL_SCRAM_TRUSTSTORE_LOCATION,kafkaReadSaslScramTruststorePasswordSecretId=$KAFKA_READ_SASL_SCRAM_TRUSTSTORE_PASSWORD_SECRET_ID,messageFormat=$MESSAGE_FORMAT,schemaFormat=$SCHEMA_FORMAT,confluentAvroSchemaPath=$CONFLUENT_AVRO_SCHEMA_PATH,schemaRegistryConnectionUrl=$SCHEMA_REGISTRY_CONNECTION_URL,binaryAvroSchemaPath=$BINARY_AVRO_SCHEMA_PATH,schemaRegistryAuthenticationMode=$SCHEMA_REGISTRY_AUTHENTICATION_MODE,schemaRegistryTruststoreLocation=$SCHEMA_REGISTRY_TRUSTSTORE_LOCATION,schemaRegistryTruststorePasswordSecretId=$SCHEMA_REGISTRY_TRUSTSTORE_PASSWORD_SECRET_ID,schemaRegistryKeystoreLocation=$SCHEMA_REGISTRY_KEYSTORE_LOCATION,schemaRegistryKeystorePasswordSecretId=$SCHEMA_REGISTRY_KEYSTORE_PASSWORD_SECRET_ID,schemaRegistryKeyPasswordSecretId=$SCHEMA_REGISTRY_KEY_PASSWORD_SECRET_ID,schemaRegistryOauthClientId=$SCHEMA_REGISTRY_OAUTH_CLIENT_ID,schemaRegistryOauthClientSecretId=$SCHEMA_REGISTRY_OAUTH_CLIENT_SECRET_ID,schemaRegistryOauthScope=$SCHEMA_REGISTRY_OAUTH_SCOPE,schemaRegistryOauthTokenEndpointUrl=$SCHEMA_REGISTRY_OAUTH_TOKEN_ENDPOINT_URL" \
-f v2/bigtable-changestreams-to-kafka
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
cd v2/bigtable-changestreams-to-kafka/terraform/Bigtable_Change_Streams_to_Kafka
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

resource "google_dataflow_flex_template_job" "bigtable_change_streams_to_kafka" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Bigtable_Change_Streams_to_Kafka"
  name              = "bigtable-change-streams-to-kafka"
  region            = var.region
  parameters        = {
    bigtableChangeStreamAppProfile = "<bigtableChangeStreamAppProfile>"
    bigtableReadInstanceId = "<bigtableReadInstanceId>"
    bigtableReadTableId = "<bigtableReadTableId>"
    writeBootstrapServerAndTopic = "<writeBootstrapServerAndTopic>"
    kafkaWriteAuthenticationMethod = "APPLICATION_DEFAULT_CREDENTIALS"
    messageFormat = "AVRO_CONFLUENT_WIRE_FORMAT"
    # dlqDirectory = ""
    # dlqRetryMinutes = "10"
    # dlqMaxRetries = "5"
    # disableDlqRetries = "false"
    # bigtableChangeStreamMetadataInstanceId = ""
    # bigtableChangeStreamMetadataTableTableId = ""
    # bigtableChangeStreamCharset = "UTF-8"
    # bigtableChangeStreamStartTimestamp = ""
    # bigtableChangeStreamIgnoreColumnFamilies = ""
    # bigtableChangeStreamIgnoreColumns = ""
    # bigtableChangeStreamName = "<bigtableChangeStreamName>"
    # bigtableChangeStreamResume = "false"
    # bigtableReadProjectId = ""
    # kafkaWriteUsernameSecretId = ""
    # kafkaWritePasswordSecretId = ""
    # kafkaWriteKeystoreLocation = "<kafkaWriteKeystoreLocation>"
    # kafkaWriteTruststoreLocation = "<kafkaWriteTruststoreLocation>"
    # kafkaWriteTruststorePasswordSecretId = "<kafkaWriteTruststorePasswordSecretId>"
    # kafkaWriteKeystorePasswordSecretId = "<kafkaWriteKeystorePasswordSecretId>"
    # kafkaWriteKeyPasswordSecretId = "<kafkaWriteKeyPasswordSecretId>"
    # kafkaReadSaslScramUsernameSecretId = "<kafkaReadSaslScramUsernameSecretId>"
    # kafkaReadSaslScramPasswordSecretId = "<kafkaReadSaslScramPasswordSecretId>"
    # kafkaReadSaslScramTruststoreLocation = "<kafkaReadSaslScramTruststoreLocation>"
    # kafkaReadSaslScramTruststorePasswordSecretId = "<kafkaReadSaslScramTruststorePasswordSecretId>"
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
  }
}
```
