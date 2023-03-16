Kafka to Cloud Storage Template
---
A streaming pipeline which ingests data from Kafka and writes to a pre-existing Cloud Storage bucket with a variety of file types.


:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Mandatory Parameters

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
* Valid resources for mandatory parameters.
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following command:
    * `gcloud auth login`

This README uses
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates#templates-plugin)
. Install the plugin with the following command to proceed:

```shell
mvn clean install -pl plugins/templates-maven-plugin -am
```

### Building Template

This template is a Flex Template, meaning that the pipeline code will be
containerized and the container will be executed on Dataflow. Please
check [Use Flex Templates](https://cloud.google.com/dataflow/docs/guides/templates/using-flex-templates)
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
-pl v2/kafka-to-gcs -am
```

The command should print what is the template location on Cloud Storage:

```
Flex Template was staged! gs://{BUCKET}/{PATH}
```


#### Running the Template

**Using the staged template**:

You can use the path above to share or run the template.

To start a job with the template at any time using `gcloud`, you can use:

```shell
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Kafka_to_GCS"
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>
export REGION=us-central1

### Mandatory
export BOOTSTRAP_SERVERS=<bootstrapServers>
export INPUT_TOPICS=<inputTopics>
export OUTPUT_FILE_FORMAT="TEXT"
export OUTPUT_DIRECTORY=<outputDirectory>
export NUM_SHARDS=0

### Optional
export WINDOW_DURATION="5m"
export OUTPUT_FILENAME_PREFIX="output"

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


**Using the plugin**:

Instead of just generating the template in the folder, it is possible to stage
and run the template in a single command. This may be useful for testing when
changing the templates.

```shell
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>
export REGION=us-central1

### Mandatory
export BOOTSTRAP_SERVERS=<bootstrapServers>
export INPUT_TOPICS=<inputTopics>
export OUTPUT_FILE_FORMAT="TEXT"
export OUTPUT_DIRECTORY=<outputDirectory>
export NUM_SHARDS=0

### Optional
export WINDOW_DURATION="5m"
export OUTPUT_FILENAME_PREFIX="output"

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="kafka-to-gcs-job" \
-DtemplateName="Kafka_to_GCS" \
-Dparameters="bootstrapServers=$BOOTSTRAP_SERVERS,inputTopics=$INPUT_TOPICS,outputFileFormat=$OUTPUT_FILE_FORMAT,windowDuration=$WINDOW_DURATION,outputDirectory=$OUTPUT_DIRECTORY,outputFilenamePrefix=$OUTPUT_FILENAME_PREFIX,numShards=$NUM_SHARDS" \
-pl v2/kafka-to-gcs -am
```
