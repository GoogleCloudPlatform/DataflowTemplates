# PubSub to MongoDB Dataflow Template

The [PubSubToMongoDB](src/main/java/com/google/cloud/teleport/v2/templates/BigQueryToMongoDb.java) pipeline
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
export PROJECT=<project-id>
export IMAGE_NAME="mongodb-to-bigquery"
export BUCKET_NAME=gs://<bucket-name>
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java8-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export TEMPLATE_MODULE=mongodb-to-bigquery
export APP_ROOT=/template/${TEMPLATE_MODULE}
export COMMAND_SPEC=${APP_ROOT}/resources/${TEMPLATE_MODULE}-command-spec.json
export TEMPLATE_IMAGE_SPEC=${BUCKET_NAME}/images/${TEMPLATE_MODULE}-image-spec.json

export SUBSCRIPTION=<my-subscription>
export MONGODB_HOSTNAME="mongodb+srv://<username>:<password>@<server-connection-string>"
export MONGODB_DATABASE_NAME=<database name>
export MONGODB_COLLECTION_NAME=<Collection name>

export DEADLETTER_TABLE=<my-project:my-dataset.my-deadletter-table>
```
```sh
export PROJECT="gsidemo-246315"
export IMAGE_NAME="bigquery-to-mongodb"
export BUCKET_NAME='gs://vshanbh'
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java8-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export TEMPLATE_MODULE='bigquery-to-mongodb'
export APP_ROOT=/template/${TEMPLATE_MODULE}
export COMMAND_SPEC=${APP_ROOT}/resources/${TEMPLATE_MODULE}-command-spec.json
export TEMPLATE_IMAGE_SPEC=${BUCKET_NAME}/images/${TEMPLATE_MODULE}-image-spec.json

export SUBSCRIPTION=<my-subscription>
export MONGODB_HOSTNAME="mongodb+srv://<username>:<password>@<server-connection-string>"
export MONGODB_DATABASE_NAME=<database name>
export MONGODB_COLLECTION_NAME=<Collection name>
export OUTPUT_SPEC_TABLE=<table spec>

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

* Create spec file in Cloud Storage under the path ${TEMPLATE_IMAGE_SPEC} describing container image location and metadata.
```json
{
	"image": "gcr.io/project-id/image-name",
	"metadata": {
		"name": "MongoDb To BigQuery",
		"description": "A pipeline reads from MongoDB and writes to BigQuery.",
		"parameters": [
          {
            "name": "mongoDBUri",
            "label": "MongoDB Connection URI",
            "helpText": "URI to connect to MongoDb Atlas",
            "is_optional": false,
            "paramType": "TEXT"
          }, 
          {
            "name": "database",
            "label": "mongo database",
            "helpText": "Database in MongoDB to store the collection. ex: my-db.",
            "is_optional": false,
            "paramType": "TEXT"
          }, 
          {
            "name": "collection",
            "label": "mongo collection",
            "helpText": "Name of the collection inside MongoDB database. ex: my-collection.",
            "is_optional": false,
            "paramType": "TEXT"
          }, 
          {
            "name": "outputTableSpec",
            "label": "outputTableSpec",
            "helpText": "BigQuery destination table spec. e.g bigquery-project:dataset.output_table",
            "paramType": "TEXT"
          }
		]
	   },
	"sdk_info": {
			"language": "JAVA"
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

Template can be executed using the following gcloud command.
```sh
export JOB_NAME="${TEMPLATE_MODULE}-`date +%Y%m%d-%H%M%S-%N`"
gcloud beta dataflow flex-template run ${JOB_NAME} \
        --project=${PROJECT} --region=us-east1 \
        --template-file-gcs-location=${TEMPLATE_IMAGE_SPEC} \
        --parameters mongoDbUri=${MONGODB_HOSTNAME},database=${MONGODB_DATABASE_NAME},collection=${MONGODB_COLLECTION_NAME},outputTableSpec="gsidemo-246315.mflix.IoT_separate"
```
