
Kafka to Cloud Storage template
---
A streaming pipeline which ingests data from Kafka and writes to a pre-existing
Cloud Storage bucket with a variety of file types.



:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **bootstrapServers** : Kafka Bootstrap Server list, separated by commas. (Example: localhost:9092,127.0.0.1:9093).
* **inputTopics** : Kafka topic(s) to read the input from. (Example: topic1,topic2).
* **outputFileFormat** : The file format of the desired output files. Can be TEXT, AVRO or PARQUET. Defaults to TEXT.
* **outputDirectory** : The path and filename prefix for writing output files. Must end with a slash. (Example: gs://your-bucket/your-path).
* **numShards** : The maximum number of output shards produced when writing. Default number is runner-dependent.

### Optional parameters

* **windowDuration** : The window duration/size in which data will be written to Cloud Storage. Allowed formats are: Ns (for seconds, example: 5s), Nm (for minutes, example: 12m), Nh (for hours, example: 2h). (Example: 5m). Defaults to: 5m.
* **schemaRegistryURL** : Provide the full URL of your Schema Registry (e.g., http://your-registry:8081) if your Kafka messages are encoded in Confluent Wire Format. Leave blank for other formats.
* **schemaPath** : Specify the Google Cloud Storage path (or other accessible path) to the Avro schema (.avsc) file that defines the structure of your Kafka messages. (Example: gs://<bucket_name>/schema1.avsc).
* **messageFormat** : Choose the encoding used for your Kafka messages:
 - CONFLUENT_WIRE_FORMAT: Confluent format, requires a Schema Registry URL.
 - AVRO_BINARY_ENCODING: Avro's compact binary format.
 - AVRO_SINGLE_OBJECT_ENCODING: Avro, but each message is a single Avro object. Defaults to: CONFLUENT_WIRE_FORMAT.
* **userNameSecretID** : Secret Manager secret ID for the SASL_PLAIN username. Should be in the format projects/{project}/secrets/{secret}/versions/{secret_version} (Example: projects/your-project-id/secrets/your-secret/versions/your-secret-version). Defaults to empty.
* **passwordSecretID** : Secret Manager secret ID for the SASL_PLAIN password. Should be in the format projects/{project}/secrets/{secret}/versions/{secret_version} (Example: projects/your-project-id/secrets/your-secret/versions/your-secret-version). Defaults to empty.
* **offset** : Set the Kafka offset to earliest or latest(default).
* **outputFilenamePrefix** : The prefix to place on each windowed file. (Example: output-). Defaults to: output.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/kafka-to-gcs/src/main/java/com/google/cloud/teleport/v2/templates/KafkaToGcs2.java)

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
-DtemplateName="Kafka_to_GCS_2" \
-f v2/kafka-to-gcs
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Kafka_to_GCS_2
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Kafka_to_GCS_2"

### Required
export BOOTSTRAP_SERVERS=<bootstrapServers>
export INPUT_TOPICS=<inputTopics>
export OUTPUT_FILE_FORMAT=TEXT
export OUTPUT_DIRECTORY=<outputDirectory>
export NUM_SHARDS=0

### Optional
export WINDOW_DURATION=5m
export SCHEMA_REGISTRY_URL=<schemaRegistryURL>
export SCHEMA_PATH=<schemaPath>
export MESSAGE_FORMAT=CONFLUENT_WIRE_FORMAT
export USER_NAME_SECRET_ID=""
export PASSWORD_SECRET_ID=""
export OFFSET=latest
export OUTPUT_FILENAME_PREFIX=output

gcloud dataflow flex-template run "kafka-to-gcs-2-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "bootstrapServers=$BOOTSTRAP_SERVERS" \
  --parameters "inputTopics=$INPUT_TOPICS" \
  --parameters "outputFileFormat=$OUTPUT_FILE_FORMAT" \
  --parameters "windowDuration=$WINDOW_DURATION" \
  --parameters "schemaRegistryURL=$SCHEMA_REGISTRY_URL" \
  --parameters "schemaPath=$SCHEMA_PATH" \
  --parameters "messageFormat=$MESSAGE_FORMAT" \
  --parameters "userNameSecretID=$USER_NAME_SECRET_ID" \
  --parameters "passwordSecretID=$PASSWORD_SECRET_ID" \
  --parameters "offset=$OFFSET" \
  --parameters "outputDirectory=$OUTPUT_DIRECTORY" \
  --parameters "outputFilenamePrefix=$OUTPUT_FILENAME_PREFIX" \
  --parameters "numShards=$NUM_SHARDS"
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
export INPUT_TOPICS=<inputTopics>
export OUTPUT_FILE_FORMAT=TEXT
export OUTPUT_DIRECTORY=<outputDirectory>
export NUM_SHARDS=0

### Optional
export WINDOW_DURATION=5m
export SCHEMA_REGISTRY_URL=<schemaRegistryURL>
export SCHEMA_PATH=<schemaPath>
export MESSAGE_FORMAT=CONFLUENT_WIRE_FORMAT
export USER_NAME_SECRET_ID=""
export PASSWORD_SECRET_ID=""
export OFFSET=latest
export OUTPUT_FILENAME_PREFIX=output

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="kafka-to-gcs-2-job" \
-DtemplateName="Kafka_to_GCS_2" \
-Dparameters="bootstrapServers=$BOOTSTRAP_SERVERS,inputTopics=$INPUT_TOPICS,outputFileFormat=$OUTPUT_FILE_FORMAT,windowDuration=$WINDOW_DURATION,schemaRegistryURL=$SCHEMA_REGISTRY_URL,schemaPath=$SCHEMA_PATH,messageFormat=$MESSAGE_FORMAT,userNameSecretID=$USER_NAME_SECRET_ID,passwordSecretID=$PASSWORD_SECRET_ID,offset=$OFFSET,outputDirectory=$OUTPUT_DIRECTORY,outputFilenamePrefix=$OUTPUT_FILENAME_PREFIX,numShards=$NUM_SHARDS" \
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
cd v2/kafka-to-gcs/terraform/Kafka_to_GCS_2
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

resource "google_dataflow_flex_template_job" "kafka_to_gcs_2" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Kafka_to_GCS_2"
  name              = "kafka-to-gcs-2"
  region            = var.region
  parameters        = {
    bootstrapServers = "localhost:9092,127.0.0.1:9093"
    inputTopics = "topic1,topic2"
    outputFileFormat = "TEXT"
    outputDirectory = "gs://your-bucket/your-path"
    numShards = "0"
    # windowDuration = "5m"
    # schemaRegistryURL = "<schemaRegistryURL>"
    # schemaPath = "gs://<bucket_name>/schema1.avsc"
    # messageFormat = "CONFLUENT_WIRE_FORMAT"
    # userNameSecretID = "projects/your-project-id/secrets/your-secret/versions/your-secret-version"
    # passwordSecretID = "projects/your-project-id/secrets/your-secret/versions/your-secret-version"
    # offset = "latest"
    # outputFilenamePrefix = "output-"
  }
}
```
