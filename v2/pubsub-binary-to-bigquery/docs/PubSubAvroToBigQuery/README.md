# Pub/Sub Avro to BigQuery

A Dataflow pipeline to stream [Apache Avro](https://avro.apache.org/) records
from Pub/Sub to BigQuery. Any non-transient errors writing to the BigQuery table
will be pushed to a Pub/Sub topic used as a dead-letter.

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

The Template should be build from the parent pom.xml (v2/).

This Template can also be launched directly from the Google Cloud Console. These
steps are primarily for development purposes.

### Building Container Image

__Set environment variables that will be used in the build process.__

```sh
export PROJECT=my-project
export IMAGE_NAME=pubsub-avro-to-bigquery
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java8-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export APP_ROOT=/template/pubsub-avro-to-bigquery
export COMMAND_SPEC=${APP_ROOT}/resources/pubsub-avro-to-bigquery-command-spec.json
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

The `pubsub-avro-to-bigquery-image-spec-template.json` file in this directory
contains most of the content for this file. Simply update `image` property to
the value of `${TARGET_GCR_IMAGE}` defined earlier.

### Running the Template

The template requires the following parameters:

*   schemaPath: GCS path to Avro schema file. For example,
    gs://MyBucket/file.avsc.
*   inputSubscription: The name of the subscription from which data is read. For
    example, projects/<project-id>/subscriptions/<subscription-name>.
*   outputTopic: Pub/Sub topic to write dead-letter records. For example,
    projects/<project-id>/topics/<topic-name>.
*   outputTableSpec: Output BigQuery table. For example,
    <project>:<dataset>.<table_name>. If the table already exists, it must have
    a schema already set.

The template has the following optional parameters:

*   writeDisposition: BigQuery WriteDisposition. For example, WRITE_APPEND,
    WRITE_EMPTY or WRITE_TRUNCATE. Default: WRITE_APPEND
*   createDisposition: BigQuery CreateDisposition. For example,
    CREATE_IF_NEEDED, CREATE_NEVER. Default: CREATE_IF_NEEDED

Template can be executed using the `gcloud` sdk:

```sh
gcloud beta dataflow flex-template run my-job-name \
 --template-file-gcs-location=gs://path-to-image-spec-file \
 --parameters="\
 schemaPath=gs://path-to-avro-schema-file,\
 inputSubscription=projects/my-project/subscriptions/input-subscription,\
 outputTopic=projects/my-project/topics/deadletter-topic,\
 outputTableSpec=my-project:my_dataset.my_table"
```

The template can also be launched from the portal by selecting "Custom Template"
from the list of templates.
