# Dataflow Flex Template to ingest data from AWS Kinesis to Google Cloud Pub/Sub

This directory contains a Dataflow Flex Template that creates a pipeline
to read data from a single data stream from
AWS Kinesis and write data into a single topic
in [Google Pub/Sub].

Supported data formats:
- Serializable plaintext formats, such as JSON
- [PubSubMessage]

Supported input source configurations:
- Single Kinesis Data stream
- AWS account access key and secret key ID
- GCP Secrets manager (https://cloud.google.com/secret-manager)

Supported destination configuration:
- Single Google Pub/Sub topic

In a simple scenario, the template will access Google secrets manager
with user provided secrets, extract the aws access key and secret key,
create an Apache Beam pipeline that will read messages
from a source AWS Kinesis Data stream, and stream the text messages
into specified Pub/Sub destination topic.


## Requirements

- Java 11
- GCP Secrets Manager with secrets , authenticated and with access provided
- Kinesis Datastream created up and running
- Destination PubSub output topic created

## Getting Started

This section describes what is needed to get the template up and running.
- Set up the environment
- Build AWS Kinesis to Google Pub/Sub Dataflow Flex Template
- Create a Dataflow job to ingest data using the template

### Setting Up Project Environment

#### Pipeline variables:

```
PROJECT=<my-project>
BUCKET_NAME=<my-bucket>
REGION=<my-region>
```

#### Template Metadata Storage Bucket Creation

The Dataflow Flex template has to store its metadata in a bucket in
[Google Cloud Storage](https://cloud.google.com/storage), so it can be executed from the Google Cloud Platform.
Create the bucket in Google Cloud Storage if it doesn't exist yet:

```
gsutil mb gs://${BUCKET_NAME}
```

#### Containerization variables:

```
IMAGE_NAME="$USERNAME-kinesis-to-pubsub"
MODULE_NAME=kinesis-to-pubsub
TARGET_GCR_IMAGE="gcr.io/$PROJECT/$IMAGE_NAME"
BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java11-template-launcher-base
BASE_CONTAINER_IMAGE_VERSION=latest
APP_ROOT="/template/$MODULE_NAME"
COMMAND_SPEC="$APP_ROOT/resources/$MODULE_NAME-command-spec.json"
```

## Build AWS Kinesis to Google Cloud Pub/Sub Flex Dataflow Template

Dataflow Flex Templates package the pipeline as a Docker image and stage these images
on your project's [Container Registry](https://cloud.google.com/container-registry).

### Assembling the Uber-JAR

The Dataflow Flex Templates require your Java project to be built into
an Uber JAR file.

Navigate to the v2 folder:

```
cd /path/to/DataflowTemplates/v2
```

Build the Uber JAR:

```
mvn package -am -pl kinesis-to-pubsub \
 -Dimage="$TARGET_GCR_IMAGE" \
 -Dbase-container-image="$BASE_CONTAINER_IMAGE" \
 -Dbase-container-image.version="$BASE_CONTAINER_IMAGE_VERSION" \
 -Dapp-root="$APP_ROOT" \
 -Dcommand-spec="$COMMAND_SPEC" \
 -Djib.applicationCache="/tmp/" \
```

ℹ️ An **Uber JAR** - also known as **fat JAR** - is a single JAR file that contains
both target package *and* all its dependencies.

The result of the `package` task execution is a `kinesis-to-pubsub-1.0-SNAPSHOT.jar`
file that is generated under the `target` folder in kinesis-to-pubsub directory.

### Creating the Dataflow Flex Template

To execute the template you need to create the template spec file containing all
the necessary information to run the job. This template already has the following
[metadata file](src/main/resources/kinesis-to-gcs-image-spec.json) in resources.

Navigate to the template folder:

```
cd /path/to/DataflowTemplates/v2/kinesis-to-pubsub
```

Build the Dataflow Flex Template:

```
gcloud dataflow flex-template build "$TEMPLATE_SPEC_GCSPATH" \
    --image "$TARGET_GCR_IMAGE" \
    --sdk-language "JAVA" \
    --metadata-file "$METADATA_FILEPATH"
```

### Executing Template

To deploy the pipeline, you should refer to the template file and pass the
[parameters](https://cloud.google.com/dataflow/docs/guides/specifying-exec-params#setting-other-cloud-dataflow-pipeline-options)
required by the pipeline.

The template requires the following parameters:
-secretId1 : First Secret ID containing AWS Key ID
-secretId2 : Second Secret ID containing AWS Secret Access Key
-awsRegion : AWS Region
-awsDataFormat : Data format in which data will be sent
-kinesisDataStream : Name of the Kinesis Data stream
-outputPubsubTopic : Name of the Pub/Sub Topic to which data needs to be written

You can do this in 2 different ways:
1. Using [Dataflow Google Cloud Console](https://console.cloud.google.com/dataflow/jobs)

2. Using `gcloud` CLI tool
    ```bash
    gcloud dataflow flex-template run "$JOB_NAME-$(date +'%Y%m%d%H%M%S')" \
   --project "$PROJECT" --region "$REGION" \
   --template-file-gcs-location "$TEMPLATE_SPEC_PUBSUB_PATH" \
   --parameters secretId1="projects/projectId/secrets/secret_id_1/versions/1"\
   --parameters secretId2="projects/projectId/secrets/secret_id_2/versions/1" \
   --parameters awsRegion="us-west-2" \
   --parameters awsDataFormat="json" \
   --parameters kinesisDataStream="kinesis-input-datastream-name" \
   --parameters outputPubsubTopic="projects/'$PROJECT'/topics/your-topic-name"
    ```
