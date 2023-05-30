# Dataflow Flex Template to ingest data from Pub/Sub to JMS Server

A Dataflow pipeline to stream messages from Pub/Sub subscription to JMS Queue/Topic.

## Requirements

These are common requirements for all  the templates in this collection.

*   Java 11
*   Maven
*   Docker

## Getting Started

### Building Template

This is a Flex Template meaning that the pipeline code will be containerized and
the container will be used to launch the Dataflow pipeline.

### Compiling the pipeline

Execute the following command from the directory containing the parent pom.xml
(DataflowTemplates/):

```shell
mvn clean compile -pl v2/pubsub-to-jms -am
```

### Executing unit tests

Execute the following command from the directory containing the parent pom.xml
(DataflowTemplates/):

```shell
mvn clean test -pl v2/pubsub-to-jms -am
```

## Uploading Templates

NOTE: This requires [Cloud SDK](https://cloud.google.com/sdk/downloads) version
284.0.0 or higher.

The Template should be build from the parent pom.xml (DataflowTemplates/).

This Template can also be launched directly from the Google Cloud Console. These
steps are primarily for development purposes.

### Building Container Image

__Set environment variables that will be used in the build process.__

```sh
export PROJECT=my-project
export IMAGE_NAME=pubsub-to-jms-image
export MODULE_NAME=pubsub-to-jms
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java11-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export APP_ROOT="/template/${MODULE_NAME}"
export COMMAND_SPEC="${APP_ROOT}/resources/${MODULE_NAME}-command-spec.json"
```

__Build and push image to Google Container Repository__

```sh
mvn clean package -pl "v2/${MODULE_NAME}" -am \
  -Dimage="${TARGET_GCR_IMAGE}" \
  -Dbase-container-image="${BASE_CONTAINER_IMAGE}" \
  -Dbase-container-image.version="${BASE_CONTAINER_IMAGE_VERSION}" \
  -Dapp-root="${APP_ROOT}" \
  -Dcommand-spec="${COMMAND_SPEC}" \
  -Djib.applicationCache="/tmp/"
```

### Creating Image Spec

Create a file with the metadata required for launching the Flex template. Once
created, this file should be placed in GCS.

The `pubsub-to-jms-metadata.json` file in this directory
contains most of the content for this file. To build image spec file on GCS, use following-:
```shell
export BUCKET_NAME=dataflow-template-bucket  #bucket where image spec file will be stored
export METADATA_FILEPATH=v2/pubsub-to-jms/src/main/resources/pubsub-to-jms-metadata.json
export TEMPLATE_SPEC_GCSPATH="gs://${BUCKET_NAME}/templates/specs/pubsub-to-jms"

gcloud dataflow flex-template build "${TEMPLATE_SPEC_GCSPATH}" \
    --image "${TARGET_GCR_IMAGE}" \
    --sdk-language "JAVA" \
    --metadata-file "${METADATA_FILEPATH}"
```

### Running the Template

The template requires the following parameters:

* inputSubscription: Pub/Sub Subscription to read messages. For example-
  projects/<project-id>/subscriptions/<subscription-name>.
* jmsServer: JMS (ActiveMQ) Server IP. For example-
  tcp://<ActiveMQ-HostIP>:<PORT>
* outputName: Output JMS Queue/Topic to read from. For
  example- outputQueue.
* outputType: JMS Destination Type , can be queue or topic



The template has the following optional parameters:
* username: JMS server username for authentication. For example- exampleusername.
* password: Password for the provided JMS username.

Template can be executed using the `gcloud` sdk:

```sh
export JOB_NAME=your-job-name
export PROJECT=exampleproject
export REGION=us-central1   #update based on requirement
gcloud dataflow flex-template run "$JOB_NAME-$(date +'%Y%m%d%H%M%S')" \
  --project "$PROJECT" --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters inputSubscription="projects/exampleproject/subscriptions/examplesubscription" \
  --parameters jmsServer="tcp://10.128.15.197:61616" \
  --parameters outputName="test"  \
  --parameters outputType="queue" \
  --parameters username="" \
  --parameters password=""
```
*Note-: Please keep username and password empty while no authentication required*

The template can also be launched from the portal by selecting "Custom Template"
from the list of templates.
