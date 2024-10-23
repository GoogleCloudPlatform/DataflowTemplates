
Pub/Sub Proto to BigQuery with Python UDF template
---
The Pub/Sub proto to BigQuery template is a streaming pipeline that ingests proto
data from a Pub/Sub subscription into a BigQuery table. Any errors that occur
while writing to the BigQuery table are streamed into a Pub/Sub unprocessed
topic.

A Python user-defined function (UDF) can be provided to transform data. Errors
while executing the UDF can be sent to either a separate Pub/Sub topic or the
same unprocessed topic as the BigQuery errors.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/pubsub-proto-to-bigquery)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=PubSub_Proto_to_BigQuery_Xlang).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **protoSchemaPath** : The Cloud Storage location of the self-contained proto schema file. For example, gs://path/to/my/file.pb. You can generate this file with the `--descriptor_set_out` flag of the protoc command. The `--include_imports` flag guarantees that the file is self-contained.
* **fullMessageName** : The full proto message name. For example, `package.name`. `MessageName`, where `package.name` is the value provided for the `package` statement and not the `java_package` statement.
* **inputSubscription** : The Pub/Sub input subscription to read from. (Example: projects/<PROJECT_ID>/subscription/<SUBSCRIPTION_ID>).
* **outputTableSpec** : The BigQuery output table location to write the output to. For example, `<PROJECT_ID>:<DATASET_NAME>.<TABLE_NAME>`.Depending on the `createDisposition` specified, the output table might be created automatically using the user provided Avro schema.
* **outputTopic** : The Pub/Sub topic to use for unprocessed records. (Example: projects/<PROJECT_ID>/topics/<TOPIC_NAME>).

### Optional parameters

* **preserveProtoFieldNames** : To preserve the original proto field name in JSON, set this property to true. To use more standard JSON names, set to false. For example, `false` would change `field_name` to `fieldName`. Defaults to: false.
* **bigQueryTableSchemaPath** : The Cloud Storage path to the BigQuery schema path. If this value isn't provided, then the schema is inferred from the Proto schema. (Example: gs://MyBucket/bq_schema.json).
* **udfOutputTopic** : The Pub/Sub topic storing the UDF errors. If this value isn't provided, UDF errors are sent to the same topic as `outputTopic`. (Example: projects/your-project-id/topics/your-topic-name).
* **useStorageWriteApiAtLeastOnce** : When using the Storage Write API, specifies the write semantics. To use at-least-once semantics (https://beam.apache.org/documentation/io/built-in/google-bigquery/#at-least-once-semantics), set this parameter to true`. To use exactly-once semantics, set the parameter to `false`. This parameter applies only when `useStorageWriteApi` is `true`. The default value is `false`.
* **writeDisposition** : The BigQuery WriteDisposition (https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobconfigurationload) value. For example, `WRITE_APPEND`, `WRITE_EMPTY`, or `WRITE_TRUNCATE`. Defaults to `WRITE_APPEND`.
* **createDisposition** : The BigQuery CreateDisposition (https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobconfigurationload). For example, `CREATE_IF_NEEDED` and `CREATE_NEVER`. Defaults to `CREATE_IF_NEEDED`.
* **pythonExternalTextTransformGcsPath** : The Cloud Storage path pattern for the Python code containing your user-defined functions. (Example: gs://your-bucket/your-function.py).
* **pythonExternalTextTransformFunctionName** : The name of the function to call from your Python file. Use only letters, digits, and underscores. (Example: 'transform' or 'transform_udf1').
* **useStorageWriteApi** : If true, the pipeline uses the BigQuery Storage Write API (https://cloud.google.com/bigquery/docs/write-api). The default value is `false`. For more information, see Using the Storage Write API (https://beam.apache.org/documentation/io/built-in/google-bigquery/#storage-write-api).
* **numStorageWriteApiStreams** : When using the Storage Write API, specifies the number of write streams. If `useStorageWriteApi` is `true` and `useStorageWriteApiAtLeastOnce` is `false`, then you must set this parameter. Defaults to: 0.
* **storageWriteApiTriggeringFrequencySec** : When using the Storage Write API, specifies the triggering frequency, in seconds. If `useStorageWriteApi` is `true` and `useStorageWriteApiAtLeastOnce` is `false`, then you must set this parameter.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/pubsub-binary-to-bigquery/src/main/java/com/google/cloud/teleport/v2/templates/PubsubProtoToBigQuery.java)

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
-DtemplateName="PubSub_Proto_to_BigQuery_Xlang" \
-f v2/pubsub-binary-to-bigquery
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/PubSub_Proto_to_BigQuery_Xlang
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/PubSub_Proto_to_BigQuery_Xlang"

### Required
export PROTO_SCHEMA_PATH=<protoSchemaPath>
export FULL_MESSAGE_NAME=<fullMessageName>
export INPUT_SUBSCRIPTION=<inputSubscription>
export OUTPUT_TABLE_SPEC=<outputTableSpec>
export OUTPUT_TOPIC=<outputTopic>

### Optional
export PRESERVE_PROTO_FIELD_NAMES=false
export BIG_QUERY_TABLE_SCHEMA_PATH=<bigQueryTableSchemaPath>
export UDF_OUTPUT_TOPIC=<udfOutputTopic>
export USE_STORAGE_WRITE_API_AT_LEAST_ONCE=false
export WRITE_DISPOSITION=WRITE_APPEND
export CREATE_DISPOSITION=CREATE_IF_NEEDED
export PYTHON_EXTERNAL_TEXT_TRANSFORM_GCS_PATH=<pythonExternalTextTransformGcsPath>
export PYTHON_EXTERNAL_TEXT_TRANSFORM_FUNCTION_NAME=<pythonExternalTextTransformFunctionName>
export USE_STORAGE_WRITE_API=false
export NUM_STORAGE_WRITE_API_STREAMS=0
export STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC=<storageWriteApiTriggeringFrequencySec>

gcloud dataflow flex-template run "pubsub-proto-to-bigquery-xlang-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "protoSchemaPath=$PROTO_SCHEMA_PATH" \
  --parameters "fullMessageName=$FULL_MESSAGE_NAME" \
  --parameters "preserveProtoFieldNames=$PRESERVE_PROTO_FIELD_NAMES" \
  --parameters "bigQueryTableSchemaPath=$BIG_QUERY_TABLE_SCHEMA_PATH" \
  --parameters "udfOutputTopic=$UDF_OUTPUT_TOPIC" \
  --parameters "useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE" \
  --parameters "inputSubscription=$INPUT_SUBSCRIPTION" \
  --parameters "outputTableSpec=$OUTPUT_TABLE_SPEC" \
  --parameters "writeDisposition=$WRITE_DISPOSITION" \
  --parameters "createDisposition=$CREATE_DISPOSITION" \
  --parameters "outputTopic=$OUTPUT_TOPIC" \
  --parameters "pythonExternalTextTransformGcsPath=$PYTHON_EXTERNAL_TEXT_TRANSFORM_GCS_PATH" \
  --parameters "pythonExternalTextTransformFunctionName=$PYTHON_EXTERNAL_TEXT_TRANSFORM_FUNCTION_NAME" \
  --parameters "useStorageWriteApi=$USE_STORAGE_WRITE_API" \
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
export PROTO_SCHEMA_PATH=<protoSchemaPath>
export FULL_MESSAGE_NAME=<fullMessageName>
export INPUT_SUBSCRIPTION=<inputSubscription>
export OUTPUT_TABLE_SPEC=<outputTableSpec>
export OUTPUT_TOPIC=<outputTopic>

### Optional
export PRESERVE_PROTO_FIELD_NAMES=false
export BIG_QUERY_TABLE_SCHEMA_PATH=<bigQueryTableSchemaPath>
export UDF_OUTPUT_TOPIC=<udfOutputTopic>
export USE_STORAGE_WRITE_API_AT_LEAST_ONCE=false
export WRITE_DISPOSITION=WRITE_APPEND
export CREATE_DISPOSITION=CREATE_IF_NEEDED
export PYTHON_EXTERNAL_TEXT_TRANSFORM_GCS_PATH=<pythonExternalTextTransformGcsPath>
export PYTHON_EXTERNAL_TEXT_TRANSFORM_FUNCTION_NAME=<pythonExternalTextTransformFunctionName>
export USE_STORAGE_WRITE_API=false
export NUM_STORAGE_WRITE_API_STREAMS=0
export STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC=<storageWriteApiTriggeringFrequencySec>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="pubsub-proto-to-bigquery-xlang-job" \
-DtemplateName="PubSub_Proto_to_BigQuery_Xlang" \
-Dparameters="protoSchemaPath=$PROTO_SCHEMA_PATH,fullMessageName=$FULL_MESSAGE_NAME,preserveProtoFieldNames=$PRESERVE_PROTO_FIELD_NAMES,bigQueryTableSchemaPath=$BIG_QUERY_TABLE_SCHEMA_PATH,udfOutputTopic=$UDF_OUTPUT_TOPIC,useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE,inputSubscription=$INPUT_SUBSCRIPTION,outputTableSpec=$OUTPUT_TABLE_SPEC,writeDisposition=$WRITE_DISPOSITION,createDisposition=$CREATE_DISPOSITION,outputTopic=$OUTPUT_TOPIC,pythonExternalTextTransformGcsPath=$PYTHON_EXTERNAL_TEXT_TRANSFORM_GCS_PATH,pythonExternalTextTransformFunctionName=$PYTHON_EXTERNAL_TEXT_TRANSFORM_FUNCTION_NAME,useStorageWriteApi=$USE_STORAGE_WRITE_API,numStorageWriteApiStreams=$NUM_STORAGE_WRITE_API_STREAMS,storageWriteApiTriggeringFrequencySec=$STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC" \
-f v2/pubsub-binary-to-bigquery
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
cd v2/pubsub-binary-to-bigquery/terraform/PubSub_Proto_to_BigQuery_Xlang
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

resource "google_dataflow_flex_template_job" "pubsub_proto_to_bigquery_xlang" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/PubSub_Proto_to_BigQuery_Xlang"
  name              = "pubsub-proto-to-bigquery-xlang"
  region            = var.region
  parameters        = {
    protoSchemaPath = "<protoSchemaPath>"
    fullMessageName = "<fullMessageName>"
    inputSubscription = "projects/<PROJECT_ID>/subscription/<SUBSCRIPTION_ID>"
    outputTableSpec = "<outputTableSpec>"
    outputTopic = "projects/<PROJECT_ID>/topics/<TOPIC_NAME>"
    # preserveProtoFieldNames = "false"
    # bigQueryTableSchemaPath = "gs://MyBucket/bq_schema.json"
    # udfOutputTopic = "projects/your-project-id/topics/your-topic-name"
    # useStorageWriteApiAtLeastOnce = "false"
    # writeDisposition = "WRITE_APPEND"
    # createDisposition = "CREATE_IF_NEEDED"
    # pythonExternalTextTransformGcsPath = "gs://your-bucket/your-function.py"
    # pythonExternalTextTransformFunctionName = "'transform' or 'transform_udf1'"
    # useStorageWriteApi = "false"
    # numStorageWriteApiStreams = "0"
    # storageWriteApiTriggeringFrequencySec = "<storageWriteApiTriggeringFrequencySec>"
  }
}
```
