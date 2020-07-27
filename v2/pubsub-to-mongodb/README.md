# PubSub to MongoDB Dataflow Template

The [PubSubToMongoDB](src/main/java/com/google/cloud/teleport/v2/templates/PubSubToMongoDB.java) pipeline
ingests data from a PubSub subscription, optionally applies a Javascript UDF if supplied and writes the data to MongoDB.

## Getting Started

### Requirements
* Java 8
* Maven
* PubSub Subscription exists
* MongoDB host exists and is operational

### Building Template
This is a Flex Template meaning that the pipeline code will be containerized and the container will be used to launch the Dataflow pipeline.

#### Building Container Image
* Set environment variables.
Set the pipeline vars
```sh
export PROJECT=<my-project>
export IMAGE_NAME=<my-image-name>
export BUCKET_NAME=gs://<bucket-name>
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java8-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export TEMPLATE_MODULE=csv-to-elasticsearch
export APP_ROOT=/template/${TEMPLATE_MODULE}
export COMMAND_SPEC=${APP_ROOT}/resources/${TEMPLATE_MODULE}-command-spec.json
export TEMPLATE_IMAGE_SPEC=${BUCKET_NAME}/images/${TEMPLATE_MODULE}-image-spec.json

export SUBSCRIPTION=<my-subscription>
export MONGODB_HOSTNAME=<my-host:port>
export MONGODB_DATABASE_NAME=<testdb>
export MONGODB_COLLECTION_NAME=<testCollection>
export DEADLETTER_TABLE=<my-project:my-dataset.my-deadletter-table>
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
```json
{
  "docker_template_spec": {
    "docker_image": "gcr.io/project/my-image-name"
  }
}
```

### Testing Template

The template unit tests can be run using:
```sh
mvn test
```

### Executing Template

The template requires the following parameters:
* mongoDBUri: List of MongoDB node to connect to, ex: my-node1:port
* database: The database in mongoDB where the collection exists, ex: my-db
* collection: The collection in mongoDB database to put the documents to, ex: my-collection
* inputSubscription: PubSub subscription to read from, ex: projects/my-project/subscriptions/my-subscription
* deadletterTable: Deadletter table for failed inserts in form: project-id:dataset.table

The template has the following optional parameters:
* batchSize: Batch size in number of documents. Default: 1024
* batchSizeBytes: Batch size in number of bytes. Default: 5242880 (5mb)
* javascriptTextTransformGcsPath: Gcs path to javascript udf source. Udf will be preferred option for transformation if supplied. Default: null
* javascriptTextTransformFunctionName: UDF Javascript Function Name. Default: null
* maxConnectionIdleTime:  Maximum Connection idle time e.g 10000. Default: 60000
* sslEnabled: Specify if SSL is enabled. Default:false
* ignoreSSLCertificate: Specify whether to ignore SSL certificate. Default: false
* withOrdered: Enable ordered nulk insertions. Default: true
* withSSLInvalidHostnameAllowed: Enable InvalidHostnameAllowed for SSL Connection. Default:false
* Will override type provided.

Template can be executed using the following gcloud command.
```sh
export JOB_NAME="${TEMPLATE_MODULE}-`date +%Y%m%d-%H%M%S-%N`"
gcloud beta dataflow flex-template run ${JOB_NAME} \
        --project=${PROJECT} --region=us-central1 \
        --template-file-gcs-location=${TEMPLATE_IMAGE_SPEC} \
        --parameters inputSubscription=${SUBSCRIPTION},mongoDBUri=${MONGODB_HOSTNAME},database=${MONGODB_DATABASE_NAME},collection=${MONGODB_COLLECTION_NAME},deadletterTable=${DEADLETTER_TABLE}
```
