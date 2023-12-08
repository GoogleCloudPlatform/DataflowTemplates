
Kafka to Cloud Storage template
---
A streaming pipeline which ingests data from Kafka and writes to a pre-existing
Cloud Storage bucket with a variety of file types.



:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required Parameters

* **bootstrapServers** (Kafka Bootstrap Server list): Kafka Bootstrap Server list, separated by commas. (Example: localhost:9092,127.0.0.1:9093).
* **inputTopics** (Kafka topic(s) to read the input from): Kafka topic(s) to read the input from. (Example: topic1,topic2).
* **outputFileFormat** (File format of the desired output files. (TEXT, AVRO or PARQUET)): The file format of the desired output files. Can be TEXT, AVRO or PARQUET. Defaults to TEXT.
* **outputDirectory** (Output file directory in Cloud Storage): The path and filename prefix for writing output files. Must end with a slash. (Example: gs://your-bucket/your-path).
* **numShards** (Number of file shards): The maximum number of output shards produced when writing. Default number is runner-dependent.

### Optional Parameters

* **windowDuration** (Window duration): The window duration/size in which data will be written to Cloud Storage. Allowed formats are: Ns (for seconds, example: 5s), Nm (for minutes, example: 12m), Nh (for hours, example: 2h). (Example: 5m). Defaults to: 5m.
* **outputFilenamePrefix** (Output filename prefix of the files to write): The prefix to place on each windowed file. (Example: output-). Defaults to: output.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!
[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/kafka-to-gcs/src/main/java/com/google/cloud/teleport/v2/templates/KafkaToGCS.java)

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates#templates-plugin)
. Install the plugin with the following command before proceeding:

```shell
mvn clean install -pl plugins/templates-maven-plugin -am
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

mvn clean package -PtemplatesStage  \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-DstagePrefix="templates" \
-DtemplateName="Kafka_to_GCS" \
-pl v2/kafka-to-gcs \
-am
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Kafka_to_GCS
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Kafka_to_GCS"

### Required
export BOOTSTRAP_SERVERS=<bootstrapServers>
export INPUT_TOPICS=<inputTopics>
export OUTPUT_FILE_FORMAT=TEXT
export OUTPUT_DIRECTORY=<outputDirectory>
export NUM_SHARDS=0

### Optional
export WINDOW_DURATION=5m
export OUTPUT_FILENAME_PREFIX=output

gcloud dataflow flex-template run "kafka-to-gcs-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "bootstrapServers=$BOOTSTRAP_SERVERS" \
  --parameters "inputTopics=$INPUT_TOPICS" \
  --parameters "outputFileFormat=$OUTPUT_FILE_FORMAT" \
  --parameters "windowDuration=$WINDOW_DURATION" \
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
export OUTPUT_FILENAME_PREFIX=output

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="kafka-to-gcs-job" \
-DtemplateName="Kafka_to_GCS" \
-Dparameters="bootstrapServers=$BOOTSTRAP_SERVERS,inputTopics=$INPUT_TOPICS,outputFileFormat=$OUTPUT_FILE_FORMAT,windowDuration=$WINDOW_DURATION,outputDirectory=$OUTPUT_DIRECTORY,outputFilenamePrefix=$OUTPUT_FILENAME_PREFIX,numShards=$NUM_SHARDS" \
-pl v2/kafka-to-gcs \
-am
```

## Terraform

Dataflow supports the utilization of Terraform to manage template jobs,
see [dataflow_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_job).

Here is an example of Terraform command:


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

resource "google_dataflow_flex_template_job" "kafka_to_gcs" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Kafka_to_GCS"
  name              = "kafka-to-gcs"
  region            = var.region
  parameters        = {
    bootstrapServers = "localhost:9092,127.0.0.1:9093"
    inputTopics = "topic1,topic2"
    outputFileFormat = "TEXT"
    outputDirectory = "gs://your-bucket/your-path"
    numShards = "0"
  }
}
```
