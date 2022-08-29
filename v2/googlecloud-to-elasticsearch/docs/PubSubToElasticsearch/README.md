# Pub/Sub to Elasticsearch Dataflow Template

The Pub/Sub to Elasticsearch [template](../../src/main/java/com/google/cloud/teleport/v2/elasticsearch/templates/PubSubToElasticsearch.java) is a streaming pipeline that reads messages from a Pub/Sub subscription and writes them to Elasticsearch as documents. [Elasticsearch](https://www.elastic.co/elasticsearch/) is a real-time, distributed storage, search, and analytics engine brought to you by Elastic, the creators of the [Elastic Stack](https://www.elastic.co/elastic-stack/) (Elasticsearch, Logstash, Kibana, and Beats). [Kibana](https://www.elastic.co/kibana/) is a free and open user interface that lets you visualize your Elasticsearch data and navigate the Elastic Stack. You can use Kibana to search, view, and interact with data stored in Elasticsearch indices.

The Dataflow template will use Elasticsearch’s [Data streams](https://www.elastic.co/guide/en/elasticsearch/reference/master/data-streams.html) feature to store time series data across multiple indices while giving you a single named resource for requests. Data streams are well-suited for logs, metrics, traces, and other continuously generated data stored in Pub/Sub.

## Getting Started

The simplest way to set up Elasticsearch is to create a managed deployment with Elasticsearch Service on [Elastic Cloud](https://www.elastic.co/guide/en/elasticsearch/reference/current/getting-started.html#run-elasticsearch). You can deploy Elasticsearch on many [Google Cloud regions](https://www.elastic.co/guide/en/cloud/current/ec-regions-templates-instances.html) globally. If you prefer to manage your own environment, you can install and run [Elasticsearch using Docker](https://www.elastic.co/guide/en/elasticsearch/reference/current/getting-started.html#run-elasticsearch).

## Google Cloud Integration for Elastic

This Dataflow template adds out of the box support to stream, parse and visualize logs from several Google Cloud services (datasets) such as:
* [Cloud Audit Logs](https://cloud.google.com/logging/docs/audit)
* [Firewall](https://cloud.google.com/vpc/docs/firewall-rules-logging)
* [VPC Flow Logs](https://cloud.google.com/vpc/docs/using-flow-logs)

To enable the out of the box integration:
1. Install the Google Cloud [Logs integration](https://www.elastic.co/integrations?solution=observability&category=google-cloud) from Kibana UI
2. [Export logs](https://cloud.google.com/logging/docs/export) from above data source to a separate Pub/Sub subscription. You can then use the Dataflow template's `dataset` parameter to specify the data source name and their corresponding `inputSubscription`.

### Requirements for this pipeline
* Java 11
* Maven
* The source Pub/Sub subscription exists
* Elasticsearch version 7.0 and above
* A publicly reachable Elasticsearch host on GCP instance or on Elastic Cloud

### Parameters

The template requires the following parameters:
* connectionUrl: Elasticsearch URL in format http://hostname:[port] or CloudId
* inputSubscription: PubSub subscription to read from, ex: projects/my-project/subscriptions/my-subscription
* errorOutputTopic: Error output topic in Pub/Sub for failed inserts
* apiKey: Base64 Encoded API Key for access without requiring basic authentication. Refer  https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-create-api-key.html#security-api-create-api-key-request.

The template has the following optional parameters:
* elasticsearchUsername: Elasticsearch username used to connect to Elasticsearch endpoint. Overrides ApiKey option if specified.
* elasticsearchPassword: Elasticsearch password used to connect to Elasticsearch endpoint. Overrides ApiKey option if specified.
* dataset: The type of logs sent via Pub/Sub for which we have out of the box dashboard. Known log types values are `audit`, `vpcflow`, and `firewall`. If no known log type is detected, we default to `pubsub` type.
* namespace: An arbitrary grouping, such as an environment (dev, prod, or qa), a team, or a strategic business unit. Default is `default`
* batchSize: Batch size in number of documents. Default: 1000
* batchSizeBytes: Batch size in number of bytes. Default: 5242880 (5mb)
* javaScriptTextTransformGCSPath: GCS path to JavaScript UDF source. UDF will be preferred option for transformation if supplied. Default: null
* javaScriptTextTransformFunctionName: UDF JavaScript Function Name. Default: null
* maxRetryAttempts: Max retry attempts, must be > 0. Default: no retries
* maxRetryDuration: Max retry duration in milliseconds, must be > 0. Default: no retries
* propertyAsIndex: A property in the document being indexed whose value will specify _index metadata to be included with document in bulk request (takes precendence over an index UDF)
* javaScriptIndexFnGcsPath: GCS path of storage location for JavaScript UDF that will specify _index metadata to be included with document in bulk request
* javaScriptIndexFnName: Function name for JavaScript UDF that will specify _index metadata to be included with document in bulk request
* propertyAsId: A property in the document being indexed whose value will specify _id metadata to be included with document in bulk request (takes precendence over an index UDF)
* javaScriptIdFnGcsPath: GCS path of storage location for JavaScript UDF that will specify _id metadata to be included with document in bulk request
* javaScriptIdFnName: Function name for JavaScript UDF that will specify _id metadata to be included with document in bulk request
* javaScriptTypeFnGcsPath: GCS path of storage location for JavaScript UDF that will specify _type metadata to be included with document in bulk request
* javaScriptTypeFnName: Function name for JavaScript UDF that will specify _type metadata to be included with document in bulk request
* javaScriptIsDeleteFnGcsPath: GCS path of storage location for JavaScript UDF that will determine if document should be deleted rather than inserted or updated, function should return string value "true" or "false"
* javaScriptIsDeleteFnName: Function name for JavaScript UDF that will determine if document should be deleted rather than inserted or updated, function should return string value "true" or "false"
* usePartialUpdate:  Whether to use partial document updates (update rather than create or index, allows partial document updates)
* bulkInsertMethod: Whether to use INDEX (index, allows upserts) or the default CREATE (create, errors on duplicate _id) with Elasticsearch bulk requests

### Building Template
This is a Flex Template meaning that the pipeline code will be containerized, and the container will be used to launch the Dataflow pipeline.

#### Building Container Image
* Set environment variables.
```sh
export PROJECT=<my-project>
export IMAGE_NAME=<my-image-name>
export BUCKET_NAME=gs://<bucket-name>
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java11-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export TEMPLATE_MODULE=pubsub-to-elasticsearch
export APP_ROOT=/template/${TEMPLATE_MODULE}
export COMMAND_SPEC=${APP_ROOT}/resources/${TEMPLATE_MODULE}-command-spec.json
export TEMPLATE_IMAGE_SPEC=${BUCKET_NAME}/images/${TEMPLATE_MODULE}-image-spec.json

gcloud config set project ${PROJECT}
```

* Build and push image to Google Container Repository

```sh
mvn clean package -Dimage=${TARGET_GCR_IMAGE} \
                  -Dbase-container-image=${BASE_CONTAINER_IMAGE} \
                  -Dbase-container-image.version=${BASE_CONTAINER_IMAGE_VERSION} \
                  -Dapp-root=${APP_ROOT} \
                  -Dcommand-spec=${COMMAND_SPEC} \
                  -am -pl googlecloud-to-elasticsearch
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
              "label":"Elasticsearch URL in the format https://hostname:[port] or specify CloudID if using Elastic Cloud",
              "helpText":"Elasticsearch URL in the format https://hostname:[port] or specify CloudID if using Elastic Cloud",
              "paramType":"TEXT",
              "isOptional":false
          },
          {
              "name":"apiKey",
              "label":"Base64 Encoded API Key for access without requiring basic authentication",
              "helpText":"Base64 Encoded API Key for access without requiring basic authentication",
              "paramType":"TEXT",
              "isOptional":false
          },
          {
              "name":"dataset",
              "label":"The type of logs sent via Pub/Sub for which we have out of the box dashboard. Known log types values are audit, vpcflow, and firewall. If no known log type is detected, we default to pubsub",
              "helpText":"The type of logs sent via Pub/Sub for which we have out of the box dashboard. Known log types values are audit, vpcflow, and firewall. If no known log type is detected, we default to pubsub",
              "paramType":"TEXT",
              "isOptional":true
          },
          {
              "name":"namespace",
              "label":"The namespace for dataset. Default is default",
              "helpText":"An arbitrary grouping, such as an environment (dev, prod, or qa), a team, or a strategic business unit. Default is default",
              "paramType":"TEXT",
              "isOptional":true
          },
          {
              "name":"errorOutputTopic",
              "label":"Error output topic in Pub/Sub for failed inserts",
              "helpText":"Error output topic in Pub/Sub for failed inserts",
              "paramType":"PUBSUB_TOPIC",
              "isOptional":false
          },
          {
              "name":"elasticsearchUsername",
              "label":"Username for Elasticsearch endpoint. Overrides ApiKey option if specified.",
              "helpText":"Username for Elasticsearch endpoint. Overrides ApiKey option if specified.",
              "paramType":"TEXT",
              "isOptional":true
          },
          {
              "name":"elasticsearchPassword",
              "label":"Password for Elasticsearch endpoint. Overrides ApiKey option if specified.",
              "helpText":"Password for Elasticsearch endpoint. Overrides ApiKey option if specified.",
              "paramType":"TEXT",
              "isOptional":true
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
              "name":"propertyAsIndex",
              "label":"Document property used to specify _index metadata with document in bulk request",
              "helpText":"A property in the document being indexed whose value will specify _index metadata to be included with document in bulk request (takes precendence over an index UDF)",
              "paramType":"TEXT",
              "isOptional":true
          },
          {
              "name":"javaScriptIndexFnGcsPath",
              "label":"GCS path to JavaScript UDF source for function that will specify _index metadata to be included with document in bulk request",
              "helpText":"GCS path to JavaScript UDF source. Default: null",
              "paramType":"TEXT",
              "isOptional":true
          },
          {
              "name":"javaScriptIndexFnName",
              "label":"UDF JavaScript Function Name for function that will specify _index metadata to be included with document in bulk request",
              "helpText":"UDF JavaScript Function Name. Default: null",
              "paramType":"TEXT",
              "isOptional":true
          },
          {
              "name":"propertyAsId",
              "label":"Document property used to specify _id metadata with document in bulk request",
              "helpText":"A property in the document being indexed whose value will specify _id metadata to be included with document in bulk request (takes precendence over an index UDF)",
              "paramType":"TEXT",
              "isOptional":true
          },
          {
              "name":"javaScriptIdFnGcsPath",
              "label":"GCS path to JavaScript UDF source function that will specify _id metadata to be included with document in bulk request",
              "helpText":"GCS path to JavaScript UDF source. Default: null",
              "paramType":"TEXT",
              "isOptional":true
          },
          {
              "name":"javaScriptIdFnName",
              "label":"UDF JavaScript Function Name for function that will specify _id metadata to be included with document in bulk request",
              "helpText":"UDF JavaScript Function Name. Default: null",
              "paramType":"TEXT",
              "isOptional":true
          },
          {
              "name":"javaScriptTypeFnGcsPath",
              "label":"GCS path to JavaScript UDF source for function that will specify _type metadata to be included with document in bulk request",
              "helpText":"GCS path to JavaScript UDF source. Default: null",
              "paramType":"TEXT",
              "isOptional":true
          },
          {
              "name":"javaScriptTypeFnName",
              "label":"UDF JavaScript Function Name for function that will specify _type metadata to be included with document in bulk request",
              "helpText":"UDF JavaScript Function Name. Default: null",
              "paramType":"TEXT",
              "isOptional":true
          },
          {
              "name":"javaScriptIsDeleteFnGcsPath",
              "label":"GCS path to JavaScript UDF source for function that will determine if document should be deleted rather than inserted or updated, function should return string value \"true\" or \"false\"",
              "helpText":"GCS path to JavaScript UDF source. Default: null",
              "paramType":"TEXT",
              "isOptional":true
          },
          {
              "name":"javaScriptIsDeleteFnName",
              "label":"UDF JavaScript Function Name for function that will determine if document should be deleted rather than inserted or updated, function should return string value \"true\" or \"false\"",
              "helpText":"UDF JavaScript Function Name. Default: null",
              "paramType":"TEXT",
              "isOptional":true
          },
          {
              "name":"usePartialUpdate",
              "label":"Use partial updates (update rather than create or index, allowing partial docs) with Elasticsearch requests",
              "helpText":"Whether to use partial updates (update rather than create or index, allowing partial docs) with Elasticsearch requests",
              "paramType":"TEXT",
              "isOptional":true
          },
          {
              "name":"bulkInsertMethod",
              "label":"Use INDEX (index, allows upserts) or CREATE (create, errors on duplicate _id) in bulk requests",
              "helpText":"Whether to use INDEX (index, allows upserts) or the default CREATE (create, errors on duplicate _id) with Elasticsearch bulk requests",
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


Template can be executed using the following gcloud command.
```sh
export JOB_NAME="${TEMPLATE_MODULE}-`date +%Y%m%d-%H%M%S-%N`"
export PROJECT=<my-project>
export BUCKET_NAME=gs://<bucket-name>
export TEMPLATE_MODULE=pubsub-to-elasticsearch
export TEMPLATE_IMAGE_SPEC=${BUCKET_NAME}/images/${TEMPLATE_MODULE}-image-spec.json

export CONNECTION_URL=<url-or-cloud_id>
export SUBSCRIPTION=<my-subscription>
export DATASET=<dataset>
export NAMESPACE=<namespace>
export ERROR_OUTPUT_TOPIC=<error-output-topic>
export API_KEY=<api-key>
export ELASTICSEARCH_USERNAME=<username>
export ELASTICSEARCH_PASSWORD=<password>

gcloud dataflow flex-template run ${JOB_NAME} \
        --project=${PROJECT} --region=us-central1 \
        --template-file-gcs-location=${TEMPLATE_IMAGE_SPEC} \
        --parameters inputSubscription=${SUBSCRIPTION},connectionUrl=${CONNECTION_URL},dataset=${DATASET},namespace=${NAMESPACE},apiKey=${API_KEY},errorOutputTopic=${ERROR_OUTPUT_TOPIC},elasticsearchUsername=${ELASTICSEARCH_USERNAME},elasticsearchPassword=${ELASTICSEARCH_PASSWORD}
```