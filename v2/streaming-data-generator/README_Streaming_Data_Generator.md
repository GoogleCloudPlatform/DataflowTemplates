
Streaming Data Generator template
---
A pipeline to publish messages at specified QPS.This template can be used to
benchmark performance of streaming pipelines.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/streaming-data-generator)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Streaming_Data_Generator).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **qps**: Indicates rate of messages per second to be published to Pub/Sub.

### Optional parameters

* **schemaTemplate**: Pre-existing schema template to use. The value must be one of: [GAME_EVENT].
* **schemaLocation**: Cloud Storage path of schema location. For example, `gs://<bucket-name>/prefix`.
* **topic**: The name of the topic to which the pipeline should publish data. For example, `projects/<project-id>/topics/<topic-name>`.
* **messagesLimit**: Indicates maximum number of output messages to be generated. 0 means unlimited. Defaults to: 0.
* **outputType**: The message Output type. Default is JSON.
* **avroSchemaLocation**: Cloud Storage path of Avro schema location. Mandatory when output type is AVRO or PARQUET. For example, `gs://your-bucket/your-path/schema.avsc`.
* **sinkType**: The message Sink type. Default is PUBSUB.
* **outputTableSpec**: Output BigQuery table. Mandatory when sinkType is BIGQUERY For example, `<project>:<dataset>.<table_name>`.
* **writeDisposition**: BigQuery WriteDisposition. For example, WRITE_APPEND, WRITE_EMPTY or WRITE_TRUNCATE. Defaults to: WRITE_APPEND.
* **outputDeadletterTable**: Messages failed to reach the output table for all kind of reasons (e.g., mismatched schema, malformed json) are written to this table. If it doesn't exist, it will be created during pipeline execution. For example, `your-project-id:your-dataset.your-table-name`.
* **windowDuration**: The window duration/size in which data will be written to Cloud Storage. Allowed formats are: Ns (for seconds, example: 5s), Nm (for minutes, example: 12m), Nh (for hours, example: 2h). For example, `1m`. Defaults to: 1m.
* **outputDirectory**: The path and filename prefix for writing output files. Must end with a slash. DateTime formatting is used to parse directory path for date & time formatters. For example, `gs://your-bucket/your-path/`.
* **outputFilenamePrefix**: The prefix to place on each windowed file. For example, `output-`. Defaults to: output-.
* **numShards**: The maximum number of output shards produced when writing. A higher number of shards means higher throughput for writing to Cloud Storage, but potentially higher data aggregation cost across shards when processing output Cloud Storage files. Default value is decided by Dataflow.
* **driverClassName**: JDBC driver class name to use. For example, `com.mysql.jdbc.Driver`.
* **connectionUrl**: Url connection string to connect to the JDBC source. For example, `jdbc:mysql://some-host:3306/sampledb`.
* **username**: User name to be used for the JDBC connection.
* **password**: Password to be used for the JDBC connection.
* **connectionProperties**: Properties string to use for the JDBC connection. Format of the string must be [propertyName=property;]*. For example, `unicode=true;characterEncoding=UTF-8`.
* **statement**: SQL statement which will be executed to write to the database. The statement must specify the column names of the table in any order. Only the values of the specified column names will be read from the json and added to the statement. For example, `INSERT INTO tableName (column1, column2) VALUES (?,?)`.
* **projectId**: GCP Project Id of where the Spanner table lives.
* **spannerInstanceName**: Cloud Spanner instance name.
* **spannerDatabaseName**: Cloud Spanner database name.
* **spannerTableName**: Cloud Spanner table name.
* **maxNumMutations**: Specifies the cell mutation limit (maximum number of mutated cells per batch). Default value is 5000.
* **maxNumRows**: Specifies the row mutation limit (maximum number of mutated rows per batch). Default value is 1000.
* **batchSizeBytes**: Specifies the batch size limit (max number of bytes mutated per batch). Default value is 1MB.
* **commitDeadlineSeconds**: Specifies the deadline in seconds for the Commit API call.
* **bootstrapServer**: Kafka Bootstrap Server  For example, `localhost:9092`.
* **kafkaTopic**: Kafka topic to write to. For example, `topic`.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/streaming-data-generator/src/main/java/com/google/cloud/teleport/v2/templates/StreamingDataGenerator.java)

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
-DtemplateName="Streaming_Data_Generator" \
-f v2/streaming-data-generator
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Streaming_Data_Generator
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Streaming_Data_Generator"

### Required
export QPS=<qps>

### Optional
export SCHEMA_TEMPLATE=<schemaTemplate>
export SCHEMA_LOCATION=<schemaLocation>
export TOPIC=<topic>
export MESSAGES_LIMIT=0
export OUTPUT_TYPE=JSON
export AVRO_SCHEMA_LOCATION=<avroSchemaLocation>
export SINK_TYPE=PUBSUB
export OUTPUT_TABLE_SPEC=<outputTableSpec>
export WRITE_DISPOSITION=WRITE_APPEND
export OUTPUT_DEADLETTER_TABLE=<outputDeadletterTable>
export WINDOW_DURATION=1m
export OUTPUT_DIRECTORY=<outputDirectory>
export OUTPUT_FILENAME_PREFIX=output-
export NUM_SHARDS=0
export DRIVER_CLASS_NAME=<driverClassName>
export CONNECTION_URL=<connectionUrl>
export USERNAME=<username>
export PASSWORD=<password>
export CONNECTION_PROPERTIES=<connectionProperties>
export STATEMENT=<statement>
export PROJECT_ID=<projectId>
export SPANNER_INSTANCE_NAME=<spannerInstanceName>
export SPANNER_DATABASE_NAME=<spannerDatabaseName>
export SPANNER_TABLE_NAME=<spannerTableName>
export MAX_NUM_MUTATIONS=<maxNumMutations>
export MAX_NUM_ROWS=<maxNumRows>
export BATCH_SIZE_BYTES=<batchSizeBytes>
export COMMIT_DEADLINE_SECONDS=<commitDeadlineSeconds>
export BOOTSTRAP_SERVER=<bootstrapServer>
export KAFKA_TOPIC=<kafkaTopic>

gcloud dataflow flex-template run "streaming-data-generator-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "qps=$QPS" \
  --parameters "schemaTemplate=$SCHEMA_TEMPLATE" \
  --parameters "schemaLocation=$SCHEMA_LOCATION" \
  --parameters "topic=$TOPIC" \
  --parameters "messagesLimit=$MESSAGES_LIMIT" \
  --parameters "outputType=$OUTPUT_TYPE" \
  --parameters "avroSchemaLocation=$AVRO_SCHEMA_LOCATION" \
  --parameters "sinkType=$SINK_TYPE" \
  --parameters "outputTableSpec=$OUTPUT_TABLE_SPEC" \
  --parameters "writeDisposition=$WRITE_DISPOSITION" \
  --parameters "outputDeadletterTable=$OUTPUT_DEADLETTER_TABLE" \
  --parameters "windowDuration=$WINDOW_DURATION" \
  --parameters "outputDirectory=$OUTPUT_DIRECTORY" \
  --parameters "outputFilenamePrefix=$OUTPUT_FILENAME_PREFIX" \
  --parameters "numShards=$NUM_SHARDS" \
  --parameters "driverClassName=$DRIVER_CLASS_NAME" \
  --parameters "connectionUrl=$CONNECTION_URL" \
  --parameters "username=$USERNAME" \
  --parameters "password=$PASSWORD" \
  --parameters "connectionProperties=$CONNECTION_PROPERTIES" \
  --parameters "statement=$STATEMENT" \
  --parameters "projectId=$PROJECT_ID" \
  --parameters "spannerInstanceName=$SPANNER_INSTANCE_NAME" \
  --parameters "spannerDatabaseName=$SPANNER_DATABASE_NAME" \
  --parameters "spannerTableName=$SPANNER_TABLE_NAME" \
  --parameters "maxNumMutations=$MAX_NUM_MUTATIONS" \
  --parameters "maxNumRows=$MAX_NUM_ROWS" \
  --parameters "batchSizeBytes=$BATCH_SIZE_BYTES" \
  --parameters "commitDeadlineSeconds=$COMMIT_DEADLINE_SECONDS" \
  --parameters "bootstrapServer=$BOOTSTRAP_SERVER" \
  --parameters "kafkaTopic=$KAFKA_TOPIC"
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
export QPS=<qps>

### Optional
export SCHEMA_TEMPLATE=<schemaTemplate>
export SCHEMA_LOCATION=<schemaLocation>
export TOPIC=<topic>
export MESSAGES_LIMIT=0
export OUTPUT_TYPE=JSON
export AVRO_SCHEMA_LOCATION=<avroSchemaLocation>
export SINK_TYPE=PUBSUB
export OUTPUT_TABLE_SPEC=<outputTableSpec>
export WRITE_DISPOSITION=WRITE_APPEND
export OUTPUT_DEADLETTER_TABLE=<outputDeadletterTable>
export WINDOW_DURATION=1m
export OUTPUT_DIRECTORY=<outputDirectory>
export OUTPUT_FILENAME_PREFIX=output-
export NUM_SHARDS=0
export DRIVER_CLASS_NAME=<driverClassName>
export CONNECTION_URL=<connectionUrl>
export USERNAME=<username>
export PASSWORD=<password>
export CONNECTION_PROPERTIES=<connectionProperties>
export STATEMENT=<statement>
export PROJECT_ID=<projectId>
export SPANNER_INSTANCE_NAME=<spannerInstanceName>
export SPANNER_DATABASE_NAME=<spannerDatabaseName>
export SPANNER_TABLE_NAME=<spannerTableName>
export MAX_NUM_MUTATIONS=<maxNumMutations>
export MAX_NUM_ROWS=<maxNumRows>
export BATCH_SIZE_BYTES=<batchSizeBytes>
export COMMIT_DEADLINE_SECONDS=<commitDeadlineSeconds>
export BOOTSTRAP_SERVER=<bootstrapServer>
export KAFKA_TOPIC=<kafkaTopic>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="streaming-data-generator-job" \
-DtemplateName="Streaming_Data_Generator" \
-Dparameters="qps=$QPS,schemaTemplate=$SCHEMA_TEMPLATE,schemaLocation=$SCHEMA_LOCATION,topic=$TOPIC,messagesLimit=$MESSAGES_LIMIT,outputType=$OUTPUT_TYPE,avroSchemaLocation=$AVRO_SCHEMA_LOCATION,sinkType=$SINK_TYPE,outputTableSpec=$OUTPUT_TABLE_SPEC,writeDisposition=$WRITE_DISPOSITION,outputDeadletterTable=$OUTPUT_DEADLETTER_TABLE,windowDuration=$WINDOW_DURATION,outputDirectory=$OUTPUT_DIRECTORY,outputFilenamePrefix=$OUTPUT_FILENAME_PREFIX,numShards=$NUM_SHARDS,driverClassName=$DRIVER_CLASS_NAME,connectionUrl=$CONNECTION_URL,username=$USERNAME,password=$PASSWORD,connectionProperties=$CONNECTION_PROPERTIES,statement=$STATEMENT,projectId=$PROJECT_ID,spannerInstanceName=$SPANNER_INSTANCE_NAME,spannerDatabaseName=$SPANNER_DATABASE_NAME,spannerTableName=$SPANNER_TABLE_NAME,maxNumMutations=$MAX_NUM_MUTATIONS,maxNumRows=$MAX_NUM_ROWS,batchSizeBytes=$BATCH_SIZE_BYTES,commitDeadlineSeconds=$COMMIT_DEADLINE_SECONDS,bootstrapServer=$BOOTSTRAP_SERVER,kafkaTopic=$KAFKA_TOPIC" \
-f v2/streaming-data-generator
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
cd v2/streaming-data-generator/terraform/Streaming_Data_Generator
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

resource "google_dataflow_flex_template_job" "streaming_data_generator" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Streaming_Data_Generator"
  name              = "streaming-data-generator"
  region            = var.region
  parameters        = {
    qps = "<qps>"
    # schemaTemplate = "<schemaTemplate>"
    # schemaLocation = "<schemaLocation>"
    # topic = "<topic>"
    # messagesLimit = "0"
    # outputType = "JSON"
    # avroSchemaLocation = "<avroSchemaLocation>"
    # sinkType = "PUBSUB"
    # outputTableSpec = "<outputTableSpec>"
    # writeDisposition = "WRITE_APPEND"
    # outputDeadletterTable = "<outputDeadletterTable>"
    # windowDuration = "1m"
    # outputDirectory = "<outputDirectory>"
    # outputFilenamePrefix = "output-"
    # numShards = "0"
    # driverClassName = "<driverClassName>"
    # connectionUrl = "<connectionUrl>"
    # username = "<username>"
    # password = "<password>"
    # connectionProperties = "<connectionProperties>"
    # statement = "<statement>"
    # projectId = "<projectId>"
    # spannerInstanceName = "<spannerInstanceName>"
    # spannerDatabaseName = "<spannerDatabaseName>"
    # spannerTableName = "<spannerTableName>"
    # maxNumMutations = "<maxNumMutations>"
    # maxNumRows = "<maxNumRows>"
    # batchSizeBytes = "<batchSizeBytes>"
    # commitDeadlineSeconds = "<commitDeadlineSeconds>"
    # bootstrapServer = "<bootstrapServer>"
    # kafkaTopic = "<kafkaTopic>"
  }
}
```
