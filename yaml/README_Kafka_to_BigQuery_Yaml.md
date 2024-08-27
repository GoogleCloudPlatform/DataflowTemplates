
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
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **outputTableSpec** : BigQuery table location to write the output to. The name should be in the format `<project>:<dataset>.<table_name>`. The table's schema must match input objects.

### Optional parameters

* **readBootstrapServers** : Kafka Bootstrap Server list, separated by commas. (Example: localhost:9092,127.0.0.1:9093).
* **kafkaReadTopics** : Kafka topic(s) to read input from. (Example: topic1,topic2).
* **outputDeadletterTable** : BigQuery table for failed messages. Messages failed to reach the output table for different reasons (e.g., mismatched schema, malformed json) are written to this table. If it doesn't exist, it will be created during pipeline execution. If not specified, "outputTableSpec_error_records" is used instead. (Example: your-project-id:your-dataset.your-table-name).
* **messageFormat** : The message format. Can be AVRO or JSON. Defaults to: JSON.
* **schema** : Kafka schema. A schema is required if data format is JSON, AVRO or PROTO.
* **numStorageWriteApiStreams** : Number of streams defines the parallelism of the BigQueryIO’s Write transform and roughly corresponds to the number of Storage Write API’s streams which will be used by the pipeline. See https://cloud.google.com/blog/products/data-analytics/streaming-data-into-bigquery-using-storage-write-api for the recommended values. Defaults to: 1.
* **storageWriteApiTriggeringFrequencySec** : Triggering frequency will determine how soon the data will be visible for querying in BigQuery. See https://cloud.google.com/blog/products/data-analytics/streaming-data-into-bigquery-using-storage-write-api for the recommended values. Defaults to: 1.



## Getting Started

### Requirements

* Java 11
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
-DtemplateName="Kafka_to_BigQuery_Yaml" \
-f python
```


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
export OUTPUT_TABLE_SPEC=<outputTableSpec>

### Optional
export READ_BOOTSTRAP_SERVERS=<readBootstrapServers>
export KAFKA_READ_TOPICS=<kafkaReadTopics>
export OUTPUT_DEADLETTER_TABLE=<outputDeadletterTable>
export MESSAGE_FORMAT=JSON
export SCHEMA=<schema>
export NUM_STORAGE_WRITE_API_STREAMS=1
export STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC=1

gcloud dataflow flex-template run "kafka-to-bigquery-yaml-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "readBootstrapServers=$READ_BOOTSTRAP_SERVERS" \
  --parameters "kafkaReadTopics=$KAFKA_READ_TOPICS" \
  --parameters "outputTableSpec=$OUTPUT_TABLE_SPEC" \
  --parameters "outputDeadletterTable=$OUTPUT_DEADLETTER_TABLE" \
  --parameters "messageFormat=$MESSAGE_FORMAT" \
  --parameters "schema=$SCHEMA" \
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
export OUTPUT_TABLE_SPEC=<outputTableSpec>

### Optional
export READ_BOOTSTRAP_SERVERS=<readBootstrapServers>
export KAFKA_READ_TOPICS=<kafkaReadTopics>
export OUTPUT_DEADLETTER_TABLE=<outputDeadletterTable>
export MESSAGE_FORMAT=JSON
export SCHEMA=<schema>
export NUM_STORAGE_WRITE_API_STREAMS=1
export STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC=1

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="kafka-to-bigquery-yaml-job" \
-DtemplateName="Kafka_to_BigQuery_Yaml" \
-Dparameters="readBootstrapServers=$READ_BOOTSTRAP_SERVERS,kafkaReadTopics=$KAFKA_READ_TOPICS,outputTableSpec=$OUTPUT_TABLE_SPEC,outputDeadletterTable=$OUTPUT_DEADLETTER_TABLE,messageFormat=$MESSAGE_FORMAT,schema=$SCHEMA,numStorageWriteApiStreams=$NUM_STORAGE_WRITE_API_STREAMS,storageWriteApiTriggeringFrequencySec=$STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC" \
-f python
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
    outputTableSpec = "<outputTableSpec>"
    # readBootstrapServers = "localhost:9092,127.0.0.1:9093"
    # kafkaReadTopics = "topic1,topic2"
    # outputDeadletterTable = "your-project-id:your-dataset.your-table-name"
    # messageFormat = "JSON"
    # schema = "<schema>"
    # numStorageWriteApiStreams = "1"
    # storageWriteApiTriggeringFrequencySec = "1"
  }
}
```
