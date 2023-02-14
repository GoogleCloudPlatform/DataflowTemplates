# Dataflow Flex Template to ingest data from  Google Cloud Pub/Sub to Apache Kafka

This directory contains a Dataflow Flex Template that creates a pipeline
to read data from a topic from [Google Pub/Sub](https://cloud.google.com/pubsub)
and write data into a single topic
in [Apache Kafka](https://kafka.apache.org/)  .

Supported data formats:
- [PubSubMessage](https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage)


Supported input configuration:
- Single Google Pub/Sub topic

Supported output source configurations:
- Apache Kafka SSL connection
- Secrets vault service [HashiCorp Vault](https://www.vaultproject.io/)



Supported SSL certificate location:
- Bucket in [Google Cloud Storage](https://cloud.google.com/storage)

In a simple scenario, the template will create an Apache Beam pipeline that will read messages
from a Pub/Sub Topic and stream the text messages to Kafka Topic.
The template supports using SSL certificate location in Google Cloud Storage Bucket, currently supporting HashiCorp Vault to get the path details.

## Requirements

- Java 11
- Kafka Bootstrap Server up and running
- PubSub input topic created
- Kafka output Topic
- (Optional) An existing HashiCorp Vault
- (Optional) A configured secure SSL connection for Kafka

## Getting Started

This section describes what is needed to get the template up and running.
- Set up the environment
- Build  Google Pub/Sub Dataflow to Apache Kafka Flex Template
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
IMAGE_NAME=<my-image-name>
TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
BASE_CONTAINER_IMAGE=<my-base-container-image>
BASE_CONTAINER_IMAGE_VERSION=<my-base-container-image-version>
```
OPTIONAL
```
JS_PATH=gs://path/to/udf
JS_FUNC_NAME=my-js-function
```

## Build Google Cloud Pub/Sub to Apache Kafka Flex Dataflow Template

Dataflow Flex Templates package the pipeline as a Docker image and stage these images
on your project's [Container Registry](https://cloud.google.com/container-registry).

### Assembling the Uber-JAR and registering Docker Container

The Dataflow Flex Templates require your Java project to be built into
an Uber JAR file.

Navigate to the v2 folder:

```
cd /path/to/DataflowTemplates/v2
```
mvn clean install --pl ${MODULE_NAME} -am \
-Dimage=${TARGET_GCR_IMAGE} \
-Dbase-container-image=${BASE_CONTAINER_IMAGE} \
-Dbase-container-image.version=${BASE_CONTAINER_IMAGE_VERSION} \
-Dapp-root=${APP_ROOT} \
-Dcommand-spec=${COMMAND_SPEC} \
-Djib.applicationCache="/tmp/"

### Creating the Dataflow Flex Template

To execute the template you need to create the template spec file containing all
the necessary information to run the job. This template already has the following
[metadata file](src/main/resources/pubsub_to_kafka_metadata.json) in resources.

Navigate to the template folder:

```
cd /path/to/DataflowTemplates/v2/pubsub-to-kafka
```

Build the Dataflow Flex Template:

```
export METADATA_FILEPATH=src/main/resources/pubsub_to_kafka_metadata.json
export TEMPLATE_SPEC_GCSPATH=gs://${BUCKET_NAME}/templates/specs/pubsubtokafka
gcloud beta dataflow flex-template build $TEMPLATE_SPEC_GCSPATH \
    --image "${TARGET_GCR_IMAGE}" \
    --sdk-language "JAVA" \
    --metadata-file "${METADATA_FILEPATH}"
```

### Executing Template

To deploy the pipeline, you should refer to the template file and pass the
[parameters](https://cloud.google.com/dataflow/docs/guides/specifying-exec-params#setting-other-cloud-dataflow-pipeline-options)
required by the pipeline.

The template requires the following parameters:
- inputTopic: Pub/Sub topic in the format of 'projects/yourproject/topics/yourtopic'
- bootstrapServer: Comma separated kafka bootstrap servers in format ip:port
- outputTopic: Kafka topic to write the output,

The template allows for the user to supply the following optional parameters:
- javascriptTextTransformGcsPath: Path to javascript function in GCS
- javascriptTextTransformFunctionName: Name of javascript function
- outputDeadLetterTopic: Topic for messages failed to reach the output topic(aka. DeadLetter topic)
- secretStoreUrl: URL to Kafka credentials in HashiCorp Vault secret storage in the format
  'http(s)://vaultip:vaultport/path/to/credentials'
- vaultToken: Token to access HashiCorp Vault secret storage

You can do this in 3 different ways:
1. Using [Dataflow Google Cloud Console](https://console.cloud.google.com/dataflow/jobs)

2. Using `gcloud` CLI tool
    ```bash
    gcloud dataflow flex-template run "pubsub-to-kafka-`date +%Y%m%d-%H%M%S`" \
    --template-file-gcs-location "${TEMPLATE_SPEC_GCSPATH}" \
    --parameters bootstrapServer="10.128.0.15:9092" \
    --parameters outputTopic="quickstart-events" \
    --parameters inputTopic="projects/${PROJECT}/topics/testpubsub" \
    --parameters outputDeadLetterTopic="projects/${PROJECT}/topics/deadletterpubsub" \
    --parameters javascriptTextTransformGcsPath=${JS_PATH} \
    --parameters javascriptTextTransformFunctionName=${JS_FUNC_NAME} \
    --parameters secretStoreUrl="http(s)://host:port/path/to/credentials" \
    --parameters vaultToken="your-token" \
    --region "${REGION}"
    ```
3. With a REST API request
    ```
    API_ROOT_URL="https://dataflow.googleapis.com"
    TEMPLATES_LAUNCH_API="${API_ROOT_URL}/v1b3/projects/${PROJECT}/locations/${REGION}/flexTemplates:launch"
    JOB_NAME="kafka-to-pubsub-`date +%Y%m%d-%H%M%S-%N`"

    time curl -X POST -H "Content-Type: application/json" \
        -H "Authorization: Bearer $(gcloud auth print-access-token)" \
        -d '
         {
             "launch_parameter": {
                 "jobName": "'$JOB_NAME'",
                 "containerSpecGcsPath": "'$TEMPLATE_PATH'",
                 "parameters": {
                     "bootstrapServer": "broker_1:9092",
                     "inputTopic": "projects/'$PROJECT'/topics/your-topic-name",
                     "outputTopic": "topic1",
                     "outputDeadLetterTopic": "projects/'$PROJECT'/topics/your-dead-letter-topic-name",
                     "javascriptTextTransformGcsPath": '$JS_PATH',
                     "javascriptTextTransformFunctionName": '$JS_FUNC_NAME',
                     "secretStoreUrl": "http(s)://host:port/path/to/credentials",
                     "vaultToken": "your-token"
                 }
             }
         }
        '
        "${TEMPLATES_LAUNCH_API}"
    ```

_Note_: Credentials inside secret storage should have appropriate SSL configuration with following parameters:
- `bucket` - the bucket in Google Cloud Storage with SSL certificate
- `ssl.truststore.location` - the location of the trust store file
- `ssl.truststore.password` - the password for the trust store file
- `ssl.keystore.location` - the location of the key store file
- `ssl.keystore.password` - the store password for the key store file
- `ssl.key.password` - the password of the private key in the key store file
