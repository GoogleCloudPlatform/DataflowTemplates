# Pub/Sub Proto to BigQuery

A Dataflow pipeline to stream
[Google Protobuf](https://developers.google.com/protocol-buffers) messages from
Pub/Sub to BigQuery. Any non-transient errors while writing to BigQuery will be
pushed to a Pub/Sub dead-letter topic.

This Template allows for modifying the data using a JavaScript UDF. Failures in
the UDF can be written to either the same dead-letter topic as the BigQuery
write errors or to a separate dead-letter topic.

NOTE: The JavaScript UDF currently does not support ES6.

## Requirements

These are common requirements for all of the templates in this collection.

*   Java 8
*   Maven
*   Docker

## Getting Started

### Building Template

This is a Flex Template meaning that the pipeline code will be containerized and
the container will be used to launch the Dataflow pipeline.

### Compiling the pipeline

Execute the following command from the directory containing the parent pom.xml
(v2/):

```shell
mvn clean compile -pl pubsub-binary-to-bigquery -am
```

### Executing unit tests

Execute the following command from the directory containing the parent pom.xml
(v2/):

```shell
mvn clean test -pl pubsub-binary-to-bigquery -am
```

## Uploading Templates

NOTE: This requires [Cloud SDK](https://cloud.google.com/sdk/downloads) version
284.0.0 or higher.

The Template should be built from the directory containing the parent pom.xml
(v2/).

### Building Container Image

__Set environment variables that will be used in the build process.__

```sh
export PROJECT=my-project
export IMAGE_NAME=pubsub-proto-to-bigquery
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java8-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export APP_ROOT=/template/pubsub-proto-to-bigquery
export COMMAND_SPEC=${APP_ROOT}/resources/pubsub-proto-to-bigquery-command-spec.json
```

__Build and push image to Google Container Repository__

```sh
mvn clean package -Dimage=${TARGET_GCR_IMAGE} \
                  -Dbase-container-image=${BASE_CONTAINER_IMAGE} \
                  -Dbase-container-image.version=${BASE_CONTAINER_IMAGE_VERSION} \
                  -Dapp-root=${APP_ROOT} \
                  -Dcommand-spec=${COMMAND_SPEC} \
                  -pl pubsub-binary-to-bigquery -am
```

### Creating Image Spec

Create a file with the metadata required for launching the Flex template. Once
created, this file should be placed in GCS.

The `pubsub-proto-to-bigquery-image-spec-template.json` file in this directory
contains most of the content for this file. Simply update `image` property to
the value of `${TARGET_GCR_IMAGE}` defined earlier.

### Pub/Sub Proto to BigQuery Execution

The template requires the following parameters:

*   protoSchemaPath: GCS path to the self-contained proto schema. For example,
    gs://MyBucket/schema.pb. This can be created by passing
    `descriptor_set_out=schema.pb` to the `protoc` command. Use
    `--include-imports` to make sure that the file is self-contained.
*   fullMessageName: The full proto message name, for instance
    package.name.MessageName. Messages are separated by the '.' delimiter, such
    as package.name.OuterMessage.InnerMessage. Be sure to use the value set with
    the `package` statement, not the `java_package` statement.
*   inputSubscription: The name of the subscription from which data is read. For
    example, projects/\<project-id\>/subscriptions/\<subscription-name\>.
*   outputTopic: Pub/Sub topic to write unprocessed records. For example,
    projects/\<project-id\>/topics/\<topic-name\>.
*   outputTableSpec: Output BigQuery table. For example,
    \<project\>:\<dataset\>.\<table_name\>. If the table already exists, it must
    have a schema already set.

The template has the following optional parameters:

*   preserveProtoFieldNames: "true" to preserve the snake_case field names from
    the proto schema. "false" to convert the names to lowerCamelCase. This will
    determine the field names for the BigQuery table and input to the JavaScript
    UDF. (Default: "false")
*   bigQueryTableSchemaPath: GCS path to BigQuery schema file. For example,
    gs://MyBucket/bq_schema.json. If this is not set, then the schema is
    inferred from the Proto schema.
*   javascriptTextTransformGcsPath: GCS path to JavaScript UDF file. For
    example, gs://MyBucket/udf.js.
*   javascriptTextTransformFunctionName: Name of the JavaScript UDF function.
    This must be provided if `javascriptTextTransformGcsPath` is provided.
*   udfOutputTopic: Output Pub/Sub topic to send UDF failures to. For example,
    projects/\<project-id\>/topics/\<topic-name\>. If this is not provided, UDF
    failures will be sent to `outputTopic`.
*   writeDisposition: BigQuery WriteDisposition. For example, WRITE_APPEND,
    WRITE_EMPTY or WRITE_TRUNCATE. Default: WRITE_APPEND
*   createDisposition: BigQuery CreateDisposition. For example,
    CREATE_IF_NEEDED, CREATE_NEVER. Default: CREATE_IF_NEEDED

Templates can be executed using the `gcloud` SDK:

```
gcloud beta dataflow flex-template run my-job-name \
 --template-file-gcs-location=gs://path-to-image-spec-file \
 --parameters="\
 protoSchemaPath=gs://path-to-proto-schema-file,\
 fullMessageName=my.package.MyMessage,\
 inputSubscription=projects/my-project/subscriptions/input-subscription,\
 outputTopic=projects/my-project/topics/deadletter-topic,\
 outputTableSpec=my-project:my_dataset.my_table"
```

The template can also be launched from the portal by selecting "Custom Template"
from the list of templates.
