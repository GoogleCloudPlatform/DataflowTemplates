
Kafka to Kafka template
---
A pipeline that writes data to a kafka destination from another kafka source.



:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **readBootstrapServerAndTopic** : Kafka Bootstrap server and topic to read the input from. (Example: localhost:9092;topic1,topic2).
* **kafkaReadAuthenticationMode** : The mode of authentication to use with the Kafka cluster. Use NONE for no authentication, SASL_PLAIN for SASL/PLAIN username and password, TLSfor certificate-based authentication. APPLICATION_DEFAULT_CREDENTIALS should be used only for Google Cloud Apache Kafka for BigQuery cluster since This allow you to authenticate with Google Cloud Apache Kafka for BigQuery using application default credentials.
* **writeBootstrapServerAndTopic** : Kafka topic to write the output to.
* **kafkaWriteAuthenticationMethod** : The mode of authentication to use with the Kafka cluster. Use NONE for no authentication, SASL_PLAIN for SASL/PLAIN username and password, and TLS for certificate-based authentication. Defaults to: APPLICATION_DEFAULT_CREDENTIALS.

### Optional parameters

* **enableCommitOffsets** : Commit offsets of processed messages to Kafka. If enabled, this will minimize the gaps or duplicate processing of messages when restarting the pipeline. Requires specifying the Consumer Group ID. Defaults to: false.
* **consumerGroupId** : The unique identifier for the consumer group that this pipeline belongs to. Required if Commit Offsets to Kafka is enabled. Defaults to empty.
* **kafkaReadOffset** : The starting point for reading messages when no committed offsets exist. The earliest starts from the beginning, the latest from the newest message. Defaults to: latest.
* **kafkaReadUsernameSecretId** : The Google Cloud Secret Manager secret ID that contains the Kafka username to use with SASL_PLAIN authentication. (Example: projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>). Defaults to empty.
* **kafkaReadPasswordSecretId** : The Google Cloud Secret Manager secret ID that contains the Kafka password to use with SASL_PLAIN authentication. (Example: projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>). Defaults to empty.
* **kafkaReadKeystoreLocation** : The Google Cloud Storage path to the Java KeyStore (JKS) file that contains the TLS certificate and private key to use when authenticating with the Kafka cluster. (Example: gs://your-bucket/keystore.jks).
* **kafkaReadTruststoreLocation** : The Google Cloud Storage path to the Java TrustStore (JKS) file that contains the trusted certificates to use to verify the identity of the Kafka broker.
* **kafkaReadTruststorePasswordSecretId** : The Google Cloud Secret Manager secret ID that contains the password to use to access the Java TrustStore (JKS) file for Kafka TLS authentication (Example: projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>).
* **kafkaReadKeystorePasswordSecretId** : The Google Cloud Secret Manager secret ID that contains the password to use to access the Java KeyStore (JKS) file for Kafka TLS authentication. (Example: projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>).
* **kafkaReadKeyPasswordSecretId** : The Google Cloud Secret Manager secret ID that contains the password to use to access the private key within the Java KeyStore (JKS) file for Kafka TLS authentication. (Example: projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>).
* **kafkaWriteUsernameSecretId** : The Google Cloud Secret Manager secret ID that contains the Kafka username  for SASL_PLAIN authentication with the destination Kafka cluster. (Example: projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>). Defaults to empty.
* **kafkaWritePasswordSecretId** : The Google Cloud Secret Manager secret ID that contains the Kafka password to use for SASL_PLAIN authentication with the destination Kafka cluster. (Example: projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>). Defaults to empty.
* **kafkaWriteKeystoreLocation** : The Google Cloud Storage path to the Java KeyStore (JKS) file that contains the TLS certificate and private key for authenticating with the destination Kafka cluster. (Example: gs://<BUCKET>/<KEYSTORE>.jks).
* **kafkaWriteTruststoreLocation** : The Google Cloud Storage path to the Java TrustStore (JKS) file that contains the trusted certificates to use to verify the identity of the destination Kafka broker.
* **kafkaWriteTruststorePasswordSecretId** : The Google Cloud Secret Manager secret ID that contains the password to use to access the Java TrustStore (JKS) file for TLS authentication with the destination Kafka cluster. (Example: projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>).
* **kafkaWriteKeystorePasswordSecretId** : The Google Cloud Secret Manager secret ID that contains the password to access the Java KeyStore (JKS) file to use for TLS authentication with the destination Kafka cluster. (Example: projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>).
* **kafkaWriteKeyPasswordSecretId** : The Google Cloud Secret Manager secret ID that contains the password to use to access the private key within the Java KeyStore (JKS) file for TLS authentication with the destination Kafka cluster. (Example: projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>).



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/kafka-to-kafka/src/main/java/com/google/cloud/teleport/v2/templates/KafkaToKafka.java)

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#templates-plugin).

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
-DtemplateName="Kafka_to_Kafka" \
-f v2/kafka-to-kafka
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Kafka_to_Kafka
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Kafka_to_Kafka"

### Required
export READ_BOOTSTRAP_SERVER_AND_TOPIC=<readBootstrapServerAndTopic>
export KAFKA_READ_AUTHENTICATION_MODE=APPLICATION_DEFAULT_CREDENTIALS
export WRITE_BOOTSTRAP_SERVER_AND_TOPIC=<writeBootstrapServerAndTopic>
export KAFKA_WRITE_AUTHENTICATION_METHOD=APPLICATION_DEFAULT_CREDENTIALS

### Optional
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
export KAFKA_WRITE_USERNAME_SECRET_ID=""
export KAFKA_WRITE_PASSWORD_SECRET_ID=""
export KAFKA_WRITE_KEYSTORE_LOCATION=<kafkaWriteKeystoreLocation>
export KAFKA_WRITE_TRUSTSTORE_LOCATION=<kafkaWriteTruststoreLocation>
export KAFKA_WRITE_TRUSTSTORE_PASSWORD_SECRET_ID=<kafkaWriteTruststorePasswordSecretId>
export KAFKA_WRITE_KEYSTORE_PASSWORD_SECRET_ID=<kafkaWriteKeystorePasswordSecretId>
export KAFKA_WRITE_KEY_PASSWORD_SECRET_ID=<kafkaWriteKeyPasswordSecretId>

gcloud dataflow flex-template run "kafka-to-kafka-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "readBootstrapServerAndTopic=$READ_BOOTSTRAP_SERVER_AND_TOPIC" \
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
  --parameters "writeBootstrapServerAndTopic=$WRITE_BOOTSTRAP_SERVER_AND_TOPIC" \
  --parameters "kafkaWriteAuthenticationMethod=$KAFKA_WRITE_AUTHENTICATION_METHOD" \
  --parameters "kafkaWriteUsernameSecretId=$KAFKA_WRITE_USERNAME_SECRET_ID" \
  --parameters "kafkaWritePasswordSecretId=$KAFKA_WRITE_PASSWORD_SECRET_ID" \
  --parameters "kafkaWriteKeystoreLocation=$KAFKA_WRITE_KEYSTORE_LOCATION" \
  --parameters "kafkaWriteTruststoreLocation=$KAFKA_WRITE_TRUSTSTORE_LOCATION" \
  --parameters "kafkaWriteTruststorePasswordSecretId=$KAFKA_WRITE_TRUSTSTORE_PASSWORD_SECRET_ID" \
  --parameters "kafkaWriteKeystorePasswordSecretId=$KAFKA_WRITE_KEYSTORE_PASSWORD_SECRET_ID" \
  --parameters "kafkaWriteKeyPasswordSecretId=$KAFKA_WRITE_KEY_PASSWORD_SECRET_ID"
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
export KAFKA_READ_AUTHENTICATION_MODE=APPLICATION_DEFAULT_CREDENTIALS
export WRITE_BOOTSTRAP_SERVER_AND_TOPIC=<writeBootstrapServerAndTopic>
export KAFKA_WRITE_AUTHENTICATION_METHOD=APPLICATION_DEFAULT_CREDENTIALS

### Optional
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
export KAFKA_WRITE_USERNAME_SECRET_ID=""
export KAFKA_WRITE_PASSWORD_SECRET_ID=""
export KAFKA_WRITE_KEYSTORE_LOCATION=<kafkaWriteKeystoreLocation>
export KAFKA_WRITE_TRUSTSTORE_LOCATION=<kafkaWriteTruststoreLocation>
export KAFKA_WRITE_TRUSTSTORE_PASSWORD_SECRET_ID=<kafkaWriteTruststorePasswordSecretId>
export KAFKA_WRITE_KEYSTORE_PASSWORD_SECRET_ID=<kafkaWriteKeystorePasswordSecretId>
export KAFKA_WRITE_KEY_PASSWORD_SECRET_ID=<kafkaWriteKeyPasswordSecretId>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="kafka-to-kafka-job" \
-DtemplateName="Kafka_to_Kafka" \
-Dparameters="readBootstrapServerAndTopic=$READ_BOOTSTRAP_SERVER_AND_TOPIC,enableCommitOffsets=$ENABLE_COMMIT_OFFSETS,consumerGroupId=$CONSUMER_GROUP_ID,kafkaReadOffset=$KAFKA_READ_OFFSET,kafkaReadAuthenticationMode=$KAFKA_READ_AUTHENTICATION_MODE,kafkaReadUsernameSecretId=$KAFKA_READ_USERNAME_SECRET_ID,kafkaReadPasswordSecretId=$KAFKA_READ_PASSWORD_SECRET_ID,kafkaReadKeystoreLocation=$KAFKA_READ_KEYSTORE_LOCATION,kafkaReadTruststoreLocation=$KAFKA_READ_TRUSTSTORE_LOCATION,kafkaReadTruststorePasswordSecretId=$KAFKA_READ_TRUSTSTORE_PASSWORD_SECRET_ID,kafkaReadKeystorePasswordSecretId=$KAFKA_READ_KEYSTORE_PASSWORD_SECRET_ID,kafkaReadKeyPasswordSecretId=$KAFKA_READ_KEY_PASSWORD_SECRET_ID,writeBootstrapServerAndTopic=$WRITE_BOOTSTRAP_SERVER_AND_TOPIC,kafkaWriteAuthenticationMethod=$KAFKA_WRITE_AUTHENTICATION_METHOD,kafkaWriteUsernameSecretId=$KAFKA_WRITE_USERNAME_SECRET_ID,kafkaWritePasswordSecretId=$KAFKA_WRITE_PASSWORD_SECRET_ID,kafkaWriteKeystoreLocation=$KAFKA_WRITE_KEYSTORE_LOCATION,kafkaWriteTruststoreLocation=$KAFKA_WRITE_TRUSTSTORE_LOCATION,kafkaWriteTruststorePasswordSecretId=$KAFKA_WRITE_TRUSTSTORE_PASSWORD_SECRET_ID,kafkaWriteKeystorePasswordSecretId=$KAFKA_WRITE_KEYSTORE_PASSWORD_SECRET_ID,kafkaWriteKeyPasswordSecretId=$KAFKA_WRITE_KEY_PASSWORD_SECRET_ID" \
-f v2/kafka-to-kafka
```

#### Troubleshooting
If there are compilation errors related to template metadata or template plugin framework,
make sure the plugin dependencies are up-to-date by running:
```
mvn clean install -pl plugins/templates-maven-plugin,metadata -am
```
See [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#templates-plugin)
for more information.



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
cd v2/kafka-to-kafka/terraform/Kafka_to_Kafka
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

resource "google_dataflow_flex_template_job" "kafka_to_kafka" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Kafka_to_Kafka"
  name              = "kafka-to-kafka"
  region            = var.region
  parameters        = {
    readBootstrapServerAndTopic = "localhost:9092;topic1,topic2"
    kafkaReadAuthenticationMode = "APPLICATION_DEFAULT_CREDENTIALS"
    writeBootstrapServerAndTopic = "<writeBootstrapServerAndTopic>"
    kafkaWriteAuthenticationMethod = "APPLICATION_DEFAULT_CREDENTIALS"
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
    # kafkaWriteUsernameSecretId = "projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>"
    # kafkaWritePasswordSecretId = "projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>"
    # kafkaWriteKeystoreLocation = "gs://<BUCKET>/<KEYSTORE>.jks"
    # kafkaWriteTruststoreLocation = "<kafkaWriteTruststoreLocation>"
    # kafkaWriteTruststorePasswordSecretId = "projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>"
    # kafkaWriteKeystorePasswordSecretId = "projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>"
    # kafkaWriteKeyPasswordSecretId = "projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>"
  }
}
```
