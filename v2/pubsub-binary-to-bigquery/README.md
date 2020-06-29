# Cloud Pub/Sub binary to BigQuery

A collection of Dataflow Flex templates to stream binary objects (Avro, Proto etc.) from Cloud Pub/Sub to BigQuery.

* [Cloud Pub/Sub Avro to BigQuery](#pubsub-avro-to-bigquery-streaming)
* Cloud Pub/Sub Protobuf to BigQuery (__coming soon!__)

### Requirements
These are common requirements for all of the templates in this collection.
* Java 8
* Maven
* Docker

### Pub/Sub Avro to BigQuery streaming

A Dataflow pipeline to stream [Apache Avro](https://avro.apache.org/) records from Cloud Pub/Sub to BigQuery.
Any non-transient errors writing to the BigQuery table will be pushed to a Pub/Sub topic used as a dead-letter.

#### Getting Started

### Building Template
This is a Flex template meaning that the pipeline code will be containerized and the container will be
used to launch the Dataflow pipeline.

#### Compiling the pipeline
Execute the following command from the directory containing the parent pom.xml (v2/):

```shell script
mvn clean compile -pl pubsub-binary-to-bigquery -am
```


#### Executing unit tests
Execute the following command from the directory containing the parent pom.xml (v2/):

```shell script
mvn clean test -pl pubsub-binary-to-bigquery -am
```

#### Building Container Image

Execute the following command from the directory containing the parent pom.xml (v2/):

* Set environment variables that will be used in the build process.

```sh
export PROJECT=my-project
export IMAGE_NAME=pubsub-avro-to-bigquery
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java8-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export APP_ROOT=/template/pubsub-avro-to-bigquery
export COMMAND_SPEC=${APP_ROOT}/resources/pubsub-avro-to-bigquery-command-spec.json
```

* Build and push image to Google Container Repository

```sh
mvn clean package -Dimage=${TARGET_GCR_IMAGE} \
                  -Dbase-container-image=${BASE_CONTAINER_IMAGE} \
                  -Dbase-container-image.version=${BASE_CONTAINER_IMAGE_VERSION} \
                  -Dapp-root=${APP_ROOT} \
                  -Dcommand-spec=${COMMAND_SPEC} \
                  -pl pubsub-binary-to-bigquery -am
```

#### Creating Image Spec

Create a file with the metadata required for launching the Flex template. Once created, this file should
be placed in GCS.

__Note:__ The ```image``` property would point to the ```${TARGER_GCR_IMAGE}``` defined previously.

```json
{
  "image": "gcr.io/project-id/image-name",
  "metadata": {
    "name": "Pub/Sub Avro to BigQuery streaming",
    "description": "Stream Avro records from Pub/Sub to BigQuery",
    "parameters": [
      {
        "name": "schemaPath",
        "label": "GCS path to the Avro schema file",
        "helpText": "GCS path to Avro schema file. For example, gs://MyBucket/file.avsc",
        "isOptional": false,
        "regexes": [
          "^gs:\\/\\/[^\\n\\r]+$"
        ],
        "paramType": "GCS_READ_FILE"
      },
      {
        "name": "inputSubscription",
        "label": "PubSub subscription name",
        "helpText": "The name of the subscription from which data is read. For example, projects/<project-id>/subscriptions/<subscription-name>",
        "isOptional": false,
        "regexes": [
          "^projects\\/[^\\n\\r\\/]+\\/subscriptions\\/[^\\n\\r\\/]+$"
        ],
        "paramType": "PUBSUB_SUBSCRIPTION"
      },
      {
        "name": "outputTopic",
        "label": "Dead-letter output topic",
        "helpText": "Pub/Sub topic to write dead-letter records. For example, projects/<project-id>/topics/<topic-name>.",
        "isOptional": false,
        "regexes": [
          "^projects\\/[^\\n\\r\\/]+\\/topics\\/[^\\n\\r\\/]+$"],
        "paramType": "PUBSUB_TOPIC"
      },
      {
        "name": "outputTableSpec",
        "label": "Output BigQuery table",
        "helpText": "Output BigQuery table. For example, <project>:<dataset>.<table_name>",
        "isOptional": false,
        "regexes": [
          ".+:.+\\..+"
        ],
        "paramType": "TEXT"
      },
      {
        "name": "writeDisposition",
        "label": "BigQuery WriteDisposition",
        "helpText": "BigQuery WriteDisposition. For example, WRITE_APPEND, WRITE_EMPTY or WRITE_TRUNCATE. Default: WRITE_APPEND",
        "isOptional": true,
        "regexes": [
          "^(WRITE_APPEND|WRITE_EMPTY|WRITE_TRUNCATE)$"
        ],
        "paramType": "TEXT"
      },
      {
        "name": "createDisposition",
        "label": "BigQuery CreateDisposition",
        "helpText": "BigQuery CreateDisposition. For example, CREATE_IF_NEEDED, CREATE_NEVER. Default: CREATE_IF_NEEDED",
        "isOptional": true,
        "regexes": [
          "^(CREATE_IF_NEEDED|CREATE_NEVER)$"
        ],
        "paramType": "TEXT"
      }
    ]
  },
  "sdkInfo": {
    "language": "JAVA"
  }
}
```

### Executing the Flex template

The template requires the following parameters:

* schemaPath: GCS path to Avro schema file. For example, gs://MyBucket/file.avsc.
* inputSubscription: The name of the subscription from which data is read. For example, projects/<project-id>/subscriptions/<subscription-name>.
* outputTopic: Pub/Sub topic to write dead-letter records. For example, projects/<project-id>/topics/<topic-name>.
* outputTableSpec: Output BigQuery table. For example, <project>:<dataset>.<table_name>

The template has the following optional parameters:

* writeDisposition: BigQuery WriteDisposition. For example, WRITE_APPEND, WRITE_EMPTY or WRITE_TRUNCATE. Default: WRITE_APPEND
* createDisposition: BigQuery CreateDisposition. For example, CREATE_IF_NEEDED, CREATE_NEVER. Default: CREATE_IF_NEEDED

Template can be executed using the ```gcloud``` sdk:

__**Note:**__ To use the gcloud command-line tool to run Flex templates, you must have [Cloud SDK](https://cloud.google.com/sdk/downloads) version 284.0.0 or higher.

```sh
gcloud beta dataflow flex-template run my-job-name \
 --template-file-gcs-location=gs://path-to-image-spec-file \
 --parameters="\
 schemaPath=gs://path-to-avro-schema-file,\
 inputSubscription=projects/my-project/subscriptions/input-subscription,\
 outputTopic=projects/my-project/topics/deadletter-topic,\
 outputTableSpec=my-project:my_dataset.my_table"
```
