# Dataflow Flex Template to ingest data from Azure Event hub to Pub/Sub

A Dataflow pipeline to stream records from Azure Event hub (within an Event hub namespace) to Cloud Pub/Sub topic.

## Requirements

These are common requirements for all of the templates in this collection.

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
mvn clean compile -pl v2/azure-eventhub-to-pubsub -am
```

### Executing unit tests

Execute the following command from the directory containing the parent pom.xml
(DataflowTemplates/):

```shell
mvn clean test -pl v2/azure-eventhub-to-pubsub -am
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
export IMAGE_NAME=azure-eventhub-to-pubsub
export MODULE_NAME=azure-eventhub-to-pubsub
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java11-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export APP_ROOT="/template/${MODULE_NAME}"
export COMMAND_SPEC="${APP_ROOT}/resources/${MODULE_NAME}-command-spec.json"
```

__Build and push image to Google Container Repository__

```sh
mvn clean intall -pl "v2/${MODULE_NAME}" -am \
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

The `azure-eventhub-to-pubsub-metadata.json` file in this directory
contains most of the content for this file. To build image spec file on GCS, use following-:
```shell
export BUCKET_NAME=demo-bucket  #bucket where image spec file will be stored
export METADATA_FILEPATH=v2/azure-eventhub-to-pubsub/src/main/resources/azure-eventhub-to-pubsub-metadata.json
export TEMPLATE_SPEC_GCSPATH="gs://${BUCKET_NAME}/templates/specs/azure-eventhub-to-pubsub"

gcloud dataflow flex-template build "${TEMPLATE_SPEC_GCSPATH}" \
    --image "${TARGET_GCR_IMAGE}" \
    --sdk-language "JAVA" \
    --metadata-file "${METADATA_FILEPATH}"
```

### Running the Template

The template requires the following parameters:

* brokerServer: Azure Eventhub namespace to be used as kafka broker. For example-
  `mytestnamespace.servicebus.windows.net:9093` , replace: this with your appropriate namespace and service bus URL.
* inputTopic: Input Event hub name with Azure Event Hub namespace. It can be treated as Kafka Topic. For
  example- `testtopic`.
* outputTopic: Pub/Sub topic to output records. For example-
  `projects/<project-id>/topics/<topic-name>`.
* secret: Secret (within Secret Manager) to be used. It follows the pattern of `projects/{project}/secrets/{secret}/versions/{secret_version}`. This Secret will hold the SAS connection string which would be required for authentication with Azure Eventhub Namespace.

Note: More information on using connection string to connect to Azure Eventhub can be found [here](https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-java-get-started-send?tabs=connection-string%2Croles-azure-portal). In this template, the connection string needs to be stored as a secret in GCP secret manager and is provided to template via parameter (secretId, secretVersion).

Template can be executed using the `gcloud` sdk:

```sh
export JOB_NAME=your-job-name
export PROJECT=exampleproject
export REGION=us-central1   #update based on requirement
gcloud dataflow flex-template run "$JOB_NAME-$(date +'%Y%m%d%H%M%S')" \
  --project "$PROJECT" --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters brokerServer="mytestnamespace.servicebus.windows.net:9093" \
  --parameters inputTopic="testinput"  \
  --parameters outputTopic="projects/exampleproject/topics/testoutput" \
  --parameters secret="projects/sampleproject/secrets/azurekey/versions/2"
```

The template can also be launched from the portal by selecting "Custom Template"
from the list of templates.
