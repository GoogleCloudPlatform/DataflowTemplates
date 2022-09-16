# Pub/Sub Proto to Bigtable

A Dataflow pipeline to stream
[Google Protobuf](https://developers.google.com/protocol-buffers) messages from
Pub/Sub to Bigtable. Any non-transient errors while writing to Bigtable will be
pushed to a Pub/Sub dead-letter topic.

This Template allows for modifying the data using a JavaScript UDF. Failures in
the UDF can be written to either the same dead-letter topic as the Bigtable
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

Execute the following command from the root directory:

```shell
mvn clean compile -f unified-templates.xml -pl pubsub-binary-to-bigtable -am
```

### Executing unit tests

Execute the following command from the root directory:

```shell
mvn clean test -f unified-templates.xml -pl pubsub-binary-to-bigtable -am
```

## Uploading Templates

NOTE: This requires [Cloud SDK](https://cloud.google.com/sdk/downloads) version
284.0.0 or higher.

The Template should be build from the root pom.xml.

### Building Container Image

__Set environment variables that will be used in the build process.__

```sh
export PROJECT=my-project
export IMAGE_NAME=pubsub-proto-to-bigtable
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java8-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export APP_ROOT=/template/pubsub-proto-to-bigtable
export COMMAND_SPEC=${APP_ROOT}/resources/pubsub-proto-to-bigtable-command-spec.json
```

__Build and push image to Google Container Repository__

```sh
mvn clean package -Dimage=${TARGET_GCR_IMAGE} \
                  -Dbase-container-image=${BASE_CONTAINER_IMAGE} \
                  -Dbase-container-image.version=${BASE_CONTAINER_IMAGE_VERSION} \
                  -Dapp-root=${APP_ROOT} \
                  -Dcommand-spec=${COMMAND_SPEC} \
                  -f unified-templates.xml -pl v2/pubsub-binary-to-bigtable -am
```

### Creating Image Spec

Create a file with the metadata required for launching the Flex template. Once
created, this file should be placed in GCS.

The `pubsub-proto-to-bigtable-image-spec-template.json` file in this directory
contains most of the content for this file. Simply update `image` property to
the value of `${TARGET_GCR_IMAGE}` defined earlier.

### Pub/Sub Proto to Bigtable Execution

The template requires the following parameters:

*   inputSubscription: The name of the subscription from which data is read. For
    example, projects/\<project-id\>/subscriptions/\<subscription-name\>.
*   deadLetterTopic: Pub/Sub topic to write unprocessed records. For example,
    projects/\<project-id\>/topics/\<topic-name\>.
*   bigtableWriteProjectId: Bigtable project id to write to
*   bigtableWriteInstanceId: Bigtable instance id to write to
*   bigtableWriteTableId: Bigtable table id to write to
*   bigtableWriteColumnFamily: Bigtable table column family id to write to

The template has the following optional parameters:
*   bigtableWriteAppProfile: Bigtable app profile to use for the export

Templates can be executed using the `gcloud` SDK:

```
gcloud beta dataflow flex-template run my-job-name \
 --template-file-gcs-location=gs://path-to-image-spec-file \
 --parameters="\
 inputSubscription=projects/my-project/subscriptions/input-subscription,\
 deadLetterTopic=projects/my-project/topics/deadletter-topic,\
 bigtableWriteProjectId=${BIGTABLE_WRITE_PROJECT_ID},\
 bigtableWriteInstanceId=${BIGTABLE_WRITE_INSTANCE_ID},\
 bigtableWriteTableId=${BIGTABLE_WRITE_TABLE_ID}"
```

The template can also be launched from the portal by selecting "Custom Template"
from the list of templates.

### Proto Schema for the Payload that is sent to Pub/Sub

```
syntax = "proto3";

package com.google.cloud.teleport.v2.proto;

option java_package = "com.google.cloud.teleport.v2.proto";
option java_outer_classname = "BigtableProto";
option java_multiple_files = true;

// Protobuf schema for the content of a Bigtable row
message BigtableCell {
  // Bigtable row's Column Family Name
  bytes column_family = 1;
  // Bigtable row's Column Qualifier Name
  bytes column_qualifier = 2;
  // Timestamp of the cell in Microseconds
  uint64 timestamp = 3;
  // Value of the cell
  bytes value = 4;
}

// Protobuf schema for the content of the export summary file representing the
// whole database.
message BigtableRow {
  // Bigtable's Row key
  bytes row_key = 1;
  // List of cells to be added in the row
  repeated BigtableCell cells = 2;
}

```