# Dataflow Flex Template to ingest data from Apache Kafka to Google Cloud Pub/Sub

This directory contains a Dataflow Flex Template that creates a pipeline 
to read data from a single or multiple topics from 
[Apache Kafka](https://kafka.apache.org/) and write data into a single topic 
in [Google Pub/Sub](https://cloud.google.com/pubsub).

Supported data formats:
- Serializable plaintext formats, such as JSON
- [PubSubMessage](https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage)

Supported input source configurations:
- Single or multiple Apache Kafka bootstrap servers
- Apache Kafka SASL/SCRAM authentication over plaintext or SSL connection
- Secrets vault service [HashiCorp Vault](https://www.vaultproject.io/)

Supported destination configuration:
- Single Google Pub/Sub topic

Supported SSL certificate location:
- Bucket in [Google Cloud Storage](https://cloud.google.com/storage)

In a simple scenario, the template will create an Apache Beam pipeline that will read messages 
from a source Kafka server with a source topic, and stream the text messages 
into specified Pub/Sub destination topic. 
Other scenarios may need Kafka SASL/SCRAM authentication, that can be performed over plain text or SSL encrypted connection. 
The template supports using a single Kafka user account to authenticate in the provided source Kafka servers and topics. 
To support SASL authentication over SSL the template will need access to a secrets vault service with 
Kafka username and password, and with SSL certificate location in Google Cloud Storage Bucket, currently supporting HashiCorp Vault. 

## Requirements

- Java 8
- Kafka Bootstrap Server(s) up and running
- Source Kafka Topic(s)
- Destination PubSub output topic created
- (Optional) An existing HashiCorp Vault
- (Optional) A configured secure SSL connection for Kafka

## Getting Started

This section describes what is needed to get the template up and running.
- Set up the environment
- Build Apache Kafka to Google Pub/Sub Dataflow Flex Template
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
TEMPLATE_PATH="gs://${BUCKET_NAME}/templates/kafka-pubsub.json"
```
OPTIONAL
```
JS_PATH=gs://path/to/udf
JS_FUNC_NAME=my-js-function
```

## Build Apache Kafka to Google Cloud Pub/Sub Flex Dataflow Template

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
mvn package -am -pl kafka-to-pubsub
```

ℹ️ An **Uber JAR** - also known as **fat JAR** - is a single JAR file that contains 
both target package *and* all its dependencies.

The result of the `package` task execution is a `kafka-to-pubsub-1.0-SNAPSHOT.jar` 
file that is generated under the `target` folder in kafka-to-pubsub directory.

### Creating the Dataflow Flex Template

To execute the template you need to create the template spec file containing all
the necessary information to run the job. This template already has the following 
[metadata file](src/main/resources/kafka_to_pubsub_metadata.json) in resources.

Navigate to the template folder:

```
cd /path/to/DataflowTemplates/v2/kafka-to-pubsub
```

Build the Dataflow Flex Template:

```
gcloud dataflow flex-template build ${TEMPLATE_PATH} \
       --image-gcr-path "${TARGET_GCR_IMAGE}" \
       --sdk-language "JAVA" \
       --flex-template-base-image ${BASE_CONTAINER_IMAGE} \
       --metadata-file "src/main/resources/kafka_to_pubsub_metadata.json" \
       --jar "target/kafka-to-pubsub-1.0-SNAPSHOT.jar" \
       --env FLEX_TEMPLATE_JAVA_MAIN_CLASS="com.google.cloud.teleport.v2.templates.KafkaToPubsub"
```

### Executing Template

To deploy the pipeline, you should refer to the template file and pass the 
[parameters](https://cloud.google.com/dataflow/docs/guides/specifying-exec-params#setting-other-cloud-dataflow-pipeline-options) 
required by the pipeline.

The template requires the following parameters:
- bootstrapServers: Comma separated kafka bootstrap servers in format ip:port
- inputTopics: Comma separated list of Kafka topics to read from
- outputTopic: Pub/Sub topic to write the output, in the format of 'projects/yourproject/topics/yourtopic'

The template allows for the user to supply the following optional parameters:
- javascriptTextTransformGcsPath: Path to javascript function in GCS
- javascriptTextTransformFunctionName: Name of javascript function
- outputDeadLetterTopic: Topic for messages failed to reach the output topic(aka. DeadLetter topic)
- secretStoreUrl: URL to Kafka credentials in HashiCorp Vault secret storage in the format 'http(s)://vaultip:vaultport/path/to/credentials'
- vaultToken: Token to access HashiCorp Vault secret storage
- kafkaOptionsGcsPath: Path to Kafka credentials in GCS. This parameter can be used instead of
  secretStoreUrl and vaultToken

_Note_: If you want to provide Kafka credentials you should specify **either** secretStoreUrl and
vaultToken or kafkaOptionsGcsPath.

You can do this in 3 different ways:
1. Using [Dataflow Google Cloud Console](https://console.cloud.google.com/dataflow/jobs)

2. Using `gcloud` CLI tool
    ```bash
    gcloud dataflow flex-template run "kafka-to-pubsub-`date +%Y%m%d-%H%M%S`" \
        --template-file-gcs-location "${TEMPLATE_PATH}" \
        --parameters bootstrapServers="broker_1:9092,broker_2:9092" \
        --parameters inputTopics="topic1,topic2" \
        --parameters outputTopic="projects/${PROJECT}/topics/your-topic-name" \
        --parameters outputDeadLetterTopic="projects/${PROJECT}/topics/your-topic-name" \
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
                     "bootstrapServers": "broker_1:9091, broker_2:9092",
                     "inputTopics": "topic1, topic2",
                     "outputTopic": "projects/'$PROJECT'/topics/your-topic-name",
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

## Providing Kafka Credentials for SSL SCRAM

Credentials for Kafka can be provided either via JSON in GCS or via key/value pairs in HashiCorp
Vault secret storage. These credentials should have appropriate configuration with following
parameters:

For SSL configuration

- `bucket` - the bucket in Google Cloud Storage with SSL certificate
- `ssl.truststore.location` - the location of the trust store file
- `ssl.truststore.password` - the password for the trust store file
- `ssl.keystore.location` - the location of the key store file
- `ssl.keystore.password` - the store password for the key store file
- `ssl.key.password` - the password of the private key in the key store file

For SCRAM configuration

- `username` - username for Kafka SCRAM authentication
- `password` - password for Kafka SCRAM authentication

The sample JSON with SSL SCRAM parameters:

```json
{
  "bucket": "kafka_to_pubsub",
  "ssl.key.password": "secret",
  "ssl.keystore.password": "secret",
  "ssl.keystore.location": "ssl_cert/kafka.keystore.jks",
  "ssl.truststore.password": "secret",
  "ssl.truststore.location": "ssl_cert/kafka.truststore.jks",
  "username": "admin",
  "password": "admin-secret"
}
```

You can copy [this example JSON file](src/main/resources/example_kafka_credentials.json), update it
per your settings, and then upload to GCS.

## Support

This template is created and contributed by **[Akvelon](https://akvelon.com/)** team.

If you would like to see new features, or you found some critical bugs, please
[contact with us](mailto:info@akvelon.com) or [share your feedback](https://akvelon.com/feedback/).
