
Kafka to Kafka template
---
A pipeline that writes data to a kafka destination from another kafka source.



:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **sourceBootstrapServers** : Kafka Bootstrap Server List, separated by commas to read messages from the given input topic. (Example: localhost:9092, 127.0.0.1:9093).
* **inputTopic** : Kafka topic(s) to read the input from the given source bootstrap server. (Example: topic1,topic2).
* **outputTopic** : Topics to write to in the destination Kafka for the data read from the source Kafka. (Example: topic1,topic2).
* **destinationBootstrapServer** : Destination kafka Bootstrap Server to write data to. (Example: localhost:9092).

### Optional parameters

* **migrationType** : Migration type for the data movement from a source to a destination kafka.
* **authenticationMethod** : Type of authentication mechanism to authenticate to Kafka.
* **sourceUsernameSecretId** : Secret version id from the secret manager to get Kafka SASL_PLAIN username for source Kafka. (Example: projects/your-project-number/secrets/your-secret-name/versions/your-secret-version).
* **sourcePasswordSecretId** : Secret version id from the secret manager to get Kafka SASL_PLAIN password for the source Kafka. (Example: projects/your-project-number/secrets/your-secret-name/versions/your-secret-version).
* **destinationUsernameSecretId** : Secret version id from the secret manager to get Kafka SASL_PLAIN username for the destination Kafka. (Example: projects/your-project-number/secrets/your-secret-name/versions/your-secret-version).
* **destinationPasswordSecretId** :  Secret version id from the secret manager to get Kafka SASL_PLAIN password for the destination Kafka. (Example: projects/your-project-number/secrets/your-secret-name/versions/your-secret-version).
* **secretStoreUrl** : URL to credentials in Vault.
* **vaultToken** : Token to use for Vault.



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
export SOURCE_BOOTSTRAP_SERVERS=<sourceBootstrapServers>
export INPUT_TOPIC=<inputTopic>
export OUTPUT_TOPIC=<outputTopic>
export DESTINATION_BOOTSTRAP_SERVER=<destinationBootstrapServer>

### Optional
export MIGRATION_TYPE=<migrationType>
export AUTHENTICATION_METHOD=<authenticationMethod>
export SOURCE_USERNAME_SECRET_ID=<sourceUsernameSecretId>
export SOURCE_PASSWORD_SECRET_ID=<sourcePasswordSecretId>
export DESTINATION_USERNAME_SECRET_ID=<destinationUsernameSecretId>
export DESTINATION_PASSWORD_SECRET_ID=<destinationPasswordSecretId>
export SECRET_STORE_URL=<secretStoreUrl>
export VAULT_TOKEN=<vaultToken>

gcloud dataflow flex-template run "kafka-to-kafka-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "sourceBootstrapServers=$SOURCE_BOOTSTRAP_SERVERS" \
  --parameters "inputTopic=$INPUT_TOPIC" \
  --parameters "outputTopic=$OUTPUT_TOPIC" \
  --parameters "destinationBootstrapServer=$DESTINATION_BOOTSTRAP_SERVER" \
  --parameters "migrationType=$MIGRATION_TYPE" \
  --parameters "authenticationMethod=$AUTHENTICATION_METHOD" \
  --parameters "sourceUsernameSecretId=$SOURCE_USERNAME_SECRET_ID" \
  --parameters "sourcePasswordSecretId=$SOURCE_PASSWORD_SECRET_ID" \
  --parameters "destinationUsernameSecretId=$DESTINATION_USERNAME_SECRET_ID" \
  --parameters "destinationPasswordSecretId=$DESTINATION_PASSWORD_SECRET_ID" \
  --parameters "secretStoreUrl=$SECRET_STORE_URL" \
  --parameters "vaultToken=$VAULT_TOKEN"
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
export SOURCE_BOOTSTRAP_SERVERS=<sourceBootstrapServers>
export INPUT_TOPIC=<inputTopic>
export OUTPUT_TOPIC=<outputTopic>
export DESTINATION_BOOTSTRAP_SERVER=<destinationBootstrapServer>

### Optional
export MIGRATION_TYPE=<migrationType>
export AUTHENTICATION_METHOD=<authenticationMethod>
export SOURCE_USERNAME_SECRET_ID=<sourceUsernameSecretId>
export SOURCE_PASSWORD_SECRET_ID=<sourcePasswordSecretId>
export DESTINATION_USERNAME_SECRET_ID=<destinationUsernameSecretId>
export DESTINATION_PASSWORD_SECRET_ID=<destinationPasswordSecretId>
export SECRET_STORE_URL=<secretStoreUrl>
export VAULT_TOKEN=<vaultToken>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="kafka-to-kafka-job" \
-DtemplateName="Kafka_to_Kafka" \
-Dparameters="sourceBootstrapServers=$SOURCE_BOOTSTRAP_SERVERS,inputTopic=$INPUT_TOPIC,outputTopic=$OUTPUT_TOPIC,destinationBootstrapServer=$DESTINATION_BOOTSTRAP_SERVER,migrationType=$MIGRATION_TYPE,authenticationMethod=$AUTHENTICATION_METHOD,sourceUsernameSecretId=$SOURCE_USERNAME_SECRET_ID,sourcePasswordSecretId=$SOURCE_PASSWORD_SECRET_ID,destinationUsernameSecretId=$DESTINATION_USERNAME_SECRET_ID,destinationPasswordSecretId=$DESTINATION_PASSWORD_SECRET_ID,secretStoreUrl=$SECRET_STORE_URL,vaultToken=$VAULT_TOKEN" \
-f v2/kafka-to-kafka
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
    sourceBootstrapServers = "localhost:9092, 127.0.0.1:9093"
    inputTopic = "topic1,topic2"
    outputTopic = "topic1,topic2"
    destinationBootstrapServer = "localhost:9092"
    # migrationType = "<migrationType>"
    # authenticationMethod = "<authenticationMethod>"
    # sourceUsernameSecretId = "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version"
    # sourcePasswordSecretId = "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version"
    # destinationUsernameSecretId = "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version"
    # destinationPasswordSecretId = "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version"
    # secretStoreUrl = "<secretStoreUrl>"
    # vaultToken = "<vaultToken>"
  }
}
```
