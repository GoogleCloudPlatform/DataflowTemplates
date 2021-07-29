# PubSub to Elasticsearch Dataflow Template

The [PubSubToElasticsearch](../../src/main/java/com/google/cloud/teleport/v2/elasticsearch/templates/PubSubToElasticsearch.java) pipeline
ingests data from a PubSub subscription, optionally applies a Javascript UDF if supplied and writes the data to Elasticsearch.

## Getting Started

### Requirements
* Java 8
* Maven
* PubSub Subscription exists
* Elasticsearch host(s) exists and is operational

### Building Template
This is a Flex Template meaning that the pipeline code will be containerized, and the container will be
used to launch the Dataflow pipeline.

#### Building Container Image
* Set environment variables.
```sh
export PROJECT=<my-project>
export IMAGE_NAME=<my-image-name>
export BUCKET_NAME=gs://<bucket-name>
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java8-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export TEMPLATE_MODULE=pubsub-to-elasticsearch
export APP_ROOT=/template/googlecloud-to-elasticsearch
export COMMAND_SPEC=${APP_ROOT}/resources/${TEMPLATE_MODULE}-command-spec.json
export TEMPLATE_IMAGE_SPEC=${BUCKET_NAME}/images/${TEMPLATE_MODULE}-image-spec.json

export CONNECTION_URL=<url-or-cloud_id>
export SUBSCRIPTION=<my-subscription>
export WRITE_DATASET=<write-dataset>
export WRITE_NAMESPACE=<write-namespace>
export DEADLETTER_TABLE=<my-project:my-dataset.my-deadletter-table>
export WRITE_ELASTICSEARCH_USERNAME=<write-username>
export WRITE_ELASTICSEARCH_PASSWORD=<write-password>
```

* Build and push image to Google Container Repository

```sh
mvn clean package -Dimage=${TARGET_GCR_IMAGE} \
                  -Dbase-container-image=${BASE_CONTAINER_IMAGE} \
                  -Dbase-container-image.version=${BASE_CONTAINER_IMAGE_VERSION} \
                  -Dapp-root=${APP_ROOT} \
                  -Dcommand-spec=${COMMAND_SPEC} \
                  -am -pl ${TEMPLATE_MODULE}
```

#### Creating Image Spec

* Create file in Cloud Storage with path to container image in Google Container Repository.
```sh
echo '{
    "image":"'${TARGET_GCR_IMAGE}'",
    "metadata":{
      "name":"Pub/Sub to Elasticsearch",
      "description":"Replicates data from Pub/Sub topic into an Elasticsearch index",
      "parameters":[
          {
              "name":"inputSubscription",
              "label":"The Cloud Pub/Sub subscription to consume from.",
              "helpText":"The Cloud Pub/Sub subscription to consume from. The name should be in the format of projects/<project-id>/subscriptions/<subscription-name>.",
              "paramType":"TEXT",
              "isOptional":false
          },
          {
              "name":"connectionUrl",
              "label":"Elasticsearch URL in format http://hostname:[port] or Base64 encoded CloudId",
              "helpText":"Elasticsearch URL in format http://hostname:[port] or Base64 encoded CloudId",
              "paramType":"TEXT",
              "isOptional":false
          },
          {
              "name":"writeElasticsearchUsername",
              "label":"Write Elasticsearch username for elasticsearch endpoint",
              "helpText":"Write Elasticsearch username for elasticsearch endpoint",
              "paramType":"TEXT",
              "isOptional":false
          },
          {
              "name":"writeElasticsearchPassword",
              "label":"Write Elasticsearch password for elasticsearch endpoint",
              "helpText":"Write Elasticsearch password for elasticsearch endpoint",
              "paramType":"TEXT",
              "isOptional":false
          },
          {
              "name":"writeDataset",
              "label":"The type of logs sent via Pub/Sub for which we have out of the box dashboard. Known log types values are audit, vpcflow, and firewall. If no known log type is detected, we default to pubsub",
              "helpText":"The type of logs sent via Pub/Sub for which we have out of the box dashboard. Known log types values are audit, vpcflow, and firewall. If no known log type is detected, we default to pubsub",
              "paramType":"TEXT",
              "isOptional":false
          },
          {
              "name":"writeNamespace",
              "label":"The namespace for dataset. Default is default",
              "helpText":"The namespace for dataset. Default is default",
              "paramType":"TEXT",
              "isOptional":false
          },
          {
              "name":"deadletterTable",
              "label":"Deadletter table in BigQuery for failed inserts",
              "helpText":"Deadletter table in BigQuery for failed inserts in form: project-id:dataset.table",
              "paramType":"TEXT",
              "isOptional":false
          },
          {
              "name":"batchSize",
              "label":"Batch size in number of documents",
              "helpText":"Batch size in number of documents. Default: 1000",
              "paramType":"TEXT",
              "isOptional":true
          },
          {
              "name":"batchSizeBytes",
              "label":"Batch size in number of bytes",
              "helpText":"Batch size in number of bytes. Default: 5242880 (5mb)",
              "paramType":"TEXT",
              "isOptional":true
          },
          {
              "name":"maxRetryAttempts",
              "label":"Max retry attempts",
              "helpText":"Max retry attempts, must be > 0. Default: no retries",
              "paramType":"TEXT",
              "isOptional":true
          },
          {
              "name":"maxRetryDuration",
              "label":"Max retry duration in milliseconds",
              "helpText":"Max retry duration in milliseconds, must be > 0. Default: no retries",
              "paramType":"TEXT",
              "isOptional":true
          },
          {
              "name":"usePartialUpdates",
              "label":"Set to true to issue partial updates",
              "helpText":"Set to true to issue partial updates. Default: false",
              "paramType":"TEXT",
              "isOptional":true
          },
          {
              "name":"autoscalingAlgorithm","label":"Autoscaling algorithm to use",
              "helpText":"Autoscaling algorithm to use: THROUGHPUT_BASED",
              "paramType":"TEXT",
              "isOptional":true
          },
          {
              "name":"numWorkers","label":"Number of workers Dataflow will start with",
              "helpText":"Number of workers Dataflow will start with",
              "paramType":"TEXT",
              "isOptional":true
          },
          {
              "name":"maxNumWorkers","label":"Maximum number of workers Dataflow job will use",
              "helpText":"Maximum number of workers Dataflow job will use",
              "paramType":"TEXT",
              "isOptional":true
          },
          {
              "name":"workerMachineType","label":"Worker Machine Type to use in Dataflow Job",
              "helpText":"Machine Type to Use: n1-standard-4",
              "paramType":"TEXT",
              "isOptional":true
          }
      ]
    },
    "sdk_info":{"language":"JAVA"}
}' > image_spec.json
gsutil cp image_spec.json ${TEMPLATE_IMAGE_SPEC}
rm image_spec.json
```

### Testing Template

The template unit tests can be run using:
```sh
mvn test
```

### Executing Template

The template requires the following parameters:
* connectionUrl: Elasticsearch URL in format http://hostname:[port] or Base64 encoded CloudId
* writeDataset: The type of logs sent via Pub/Sub for which we have out of the box dashboard. Known log types values are audit, vpcflow, and firewall. If no known log type is detected, we default to pubsub
* writeNamespace: The namespace for dataset. Default is default
* inputSubscription: PubSub subscription to read from, ex: projects/my-project/subscriptions/my-subscription
* deadletterTable: Deadletter table for failed inserts in form: project-id:dataset.table
* writeElasticsearchUsername: Write Elasticsearch username used to connect to Elasticsearch endpoint
* writeElasticsearchPassword: Write Elasticsearch password used to connect to Elasticsearch endpoint

The template has the following optional parameters:
* batchSize: Batch size in number of documents. Default: 1000
* batchSizeBytes: Batch size in number of bytes. Default: 5242880 (5mb)
* javascriptTextTransformGcsPath: Gcs path to javascript udf source. Udf will be preferred option for transformation if supplied. Default: null
* javascriptTextTransformFunctionName: UDF Javascript Function Name. Default: null
* maxRetryAttempts: Max retry attempts, must be > 0. Default: no retries
* maxRetryDuration: Max retry duration in milliseconds, must be > 0. Default: no retries
* usePartialUpdates: Set to true to issue partial updates. Default: false

Template can be executed using the following gcloud command.
```sh
export JOB_NAME="${TEMPLATE_MODULE}-`date +%Y%m%d-%H%M%S-%N`"
gcloud beta dataflow flex-template run ${JOB_NAME} \
        --project=${PROJECT} --region=us-central1 \
        --template-file-gcs-location=${TEMPLATE_IMAGE_SPEC} \
        --parameters inputSubscription=${SUBSCRIPTION},connectionUrl=${CONNECTION_URL},writeDataset=${WRITE_DATASET},writeNamespace=${WRITE_NAMESPACE},writeElasticsearchUsername=${WRITE_ELASTICSEARCH_USERNAME},writeElasticsearchPassword=${WRITE_ELASTICSEARCH_PASSWORD},deadletterTable=${DEADLETTER_TABLE}
```