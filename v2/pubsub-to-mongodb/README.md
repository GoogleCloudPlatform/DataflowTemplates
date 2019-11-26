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
This is a Flex Template meaning that the pipeline code will be containerized and the container will be
used to launch the Dataflow pipeline.

#### Building Container Image
* Set environment variables.
Set the pipeline vars
```sh
API_ROOT_URL=https://dataflow.googleapis.com
PROJECT_NAME=my-project
BUCKET_NAME=my-bucket
INPUT_SUBSCRIPTION=my-subscription
TEMPLATES_LAUNCH_API="${API_ROOT_URL}/v1b3/projects/${PROJECT_NAME}/templates:launch"
MONGODB_DATABASE_NAME=testdb
MONGODB_HOSTNAME=my-host:port
MONGODB_COLLECTION_NAME=testCollection
DEADLETTERTABLE=project:deadletter_table_name
```
* Set containerization vars
 ```sh
IMAGE_NAME=my-image-name
TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
BASE_CONTAINER_IMAGE=my-base-container-image
BASE_CONTAINER_IMAGE_VERSION=my-base-container-image-version
APP_ROOT=/path/to/app-root
COMMAND_SPEC=/path/to/command-spec
```

* Build and push image to Google Container Repository
```sh
mvn clean package \
-Dimage=${TARGET_GCR_IMAGE} \
-Dbase-container-image=${BASE_CONTAINER_IMAGE} \
-Dbase-container-image.version=${BASE_CONTAINER_IMAGE_VERSION} \
-Dapp-root=${APP_ROOT} \
-Dcommand-spec=${COMMAND_SPEC}
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

Template can be executed using the following API call:
```sh
API_ROOT_URL="https://dataflow.googleapis.com"
TEMPLATES_LAUNCH_API="${API_ROOT_URL}/v1b3/projects/${PROJECT_NAME}/templates:launch"
JOB_NAME="pubsub-to-mongodb-`date +%Y%m%d-%H%M%S-%N`"
time curl -X POST \
   -H "Content-Type: application/json" \
   -H "Authorization: Bearer $(gcloud auth print-access-token)" \
   "${TEMPLATES_LAUNCH_API}"`\
   `"?validateOnly=false"` \
   `"&dynamicTemplate.gcsPath=gs://${BUCKET_NAME}/pubsub-to-mongodb-image-spec.json"` \
   `"&dynamicTemplate.stagingLocation=gs://${BUCKET_NAME}/staging" \
   -d'{
       "jobName":"'$JOB_NAME'",
       "parameters": {
            "inputSubscription":"'$INPUT_SUBSCRIPTION'",
            "database":"'$MONGODB_DATABASE_NAME'",
            "collection":"'$MONGODB_COLLECTION_NAME'",
            "mongoDBUri":"'$MONGODB_HOSTNAME'",
            "deadletterTable":"'$DEADLETTERTABLE'"
       }
   }'
```
