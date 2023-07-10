# PubSub to MongoDB Dataflow Template

The [PubSubToMongoDB](src/main/java/com/google/cloud/teleport/v2/templates/PubSubToMongoDB.java) pipeline
ingests data from a PubSub subscription, optionally applies a Javascript UDF if supplied and writes the data to MongoDB.

## Getting Started

### Requirements
* Java 11
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
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java11-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export TEMPLATE_MODULE=pubsub-to-mongodb
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
mvn clean package -PtemplatesStage  \                 
  -DskipTests \
  -DprojectId=${PROJECT} \
  -DbucketName=${BUCKET_NAME} \
  -DstagePrefix="images" \
  -DtemplateName="Cloud_PubSub_to_MongoDB" \
  -pl pubsub-to-mongodb -am


#mvn clean package -Dimage=${TARGET_GCR_IMAGE} \
#                  -Dbase-container-image=${BASE_CONTAINER_IMAGE} \
#                  -Dbase-container-image.version=${BASE_CONTAINER_IMAGE_VERSION} \
#                  -Dapp-root=${APP_ROOT} \
#                  -Dcommand-spec=${COMMAND_SPEC} \
#                  -am -pl ${TEMPLATE_MODULE}
```

#### Creating Image Spec

* Create spec file in Cloud Storage under the path ${TEMPLATE_IMAGE_SPEC} describing container image location and metadata.
```json
{
	"image": "gcr.io/project-id/pubsub-to-mongodb",
	"metadata": {
		"name": "PubSub To MongoDB",
		"description": "A pipeline reads from pubsub and writes to mongodb.",
		"parameters": [
				{
		        "name": "inputSubscription",
		        "label": "PubSub Subscription name",
		        "helpText": "Name of pubsub Subscription. ex: projects/<project-id>/subscriptions/<subscription-name>",
		        "is_optional": false,
		        "regexes": ["^projects\\/[^\\n\\r\\/]+\\/subscriptions\\/[^\\n\\r\\/]+$"],
		        "paramType": "PUBSUB_SUBSCRIPTION"
		        },
		       {
		        "name": "mongoDBUri",
		        "label": "MongoDB Connection URI",
		        "helpText": "MongoDB Host name in format mongodb+srv://<username>:<password>@hostname",
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
		        "label": "BigQuery DeadLetter Table",
		        "name": "deadletterTable",
		        "helpText": "Messages failed to reach the output table for all kind of reasons (e.g., mismatched schema, malformed json) are written to this table. It should be in the format of \"your-project-id:your-dataset.your-table-name\". If it doesn't exist, it will be created during pipeline execution. If not specified, \"outputTableSpec_error_records\" is used instead.",
		        "is_optional": false,
		        "regexes": [".+:.+\\..+"],
		        "paramType": "TEXT"
		      },
		      {
		        "name": "batchSize",
		        "label": "Batch Size",
		        "helpText": "Batch Size used for batch insertion of documents into mongodb.",
		        "is_optional": true,
		        "regexes": ["^[0-9]+$"],
		        "paramType": "TEXT"
		      },
		       {
		        "name": "batchSizeBytes",
		        "label": "Batch Size in Bytes",
		        "helpText": "Batch Size in bytes used for batch insertion of documents into mongodb.",
		        "is_optional": true,
		        "regexes": ["^[0-9]+$"],
		        "paramType": "TEXT"
		      },
		       {
		        "name": "maxConnectionIdleTime",
		        "label": "Max Connection idle time",
		        "helpText": "Maximum idle time allowed in seconds before connection time out occurs.",
		        "is_optional": true,
		        "regexes": ["^[0-9]+$"],
		        "paramType": "TEXT"
		      },
		       {
		        "name": "sslEnabled",
		        "label": "SSL Enabled",
		        "helpText": "Indicates whether connection to mongodb is ssl enabled or not.",
		        "is_optional": true,
		        "regexes": ["^(true|false)$"],
		        "paramType": "TEXT"
		      },
		       {
		        "name": "ignoreSSLCertificate",
		        "label": "Ignore SSL Certificate",
		        "helpText": "Indicates whether ssl certifcate should be ignored or not.",
		        "is_optional": true,
		        "regexes": ["^(true|false)$"],
		        "paramType": "TEXT"
		      },
		       {
		        "name": "withOrdered",
		        "label": "withOrdered",
		        "helpText": "Enables ordered bulk insertions into mongodb.",
		        "is_optional": true,
		        "regexes": ["^(true|false)$"],
		        "paramType": "TEXT"
		      },
		       {
		        "name": "withSSLInvalidHostNameAllowed",
		        "label": "withSSLInvalidHostNameAllowed",
		        "helpText": "Indicates whether invalid host name is allowed for ssl connection.",
		        "is_optional": true,
		        "regexes": ["^(true|false)$"],
		        "paramType": "TEXT"
		      },
		       {
		        "name": "javascriptTextTransformGcsPath",
		        "label": "GCS location of your JavaScript UDF",
		        "helpText": "The full URL of your .js file. Example: gs://your-bucket/your-function.js",
		        "is_optional": true,
		        "regexes": ["^gs:\\/\\/[^\\n\\r]+$"],
		        "paramType": "GCS_READ_FILE"
		      },
		       {
		        "name": "javascriptTextTransformFunctionName",
		        "label": "The name of the JavaScript function you wish to call as your UDF",
		        "helpText": "The function name should only contain letters, digits and underscores. Example: 'transform' or 'transform_udf1'.",
		        "is_optional": true,
		        "regexes": ["[a-zA-Z0-9_]+"],
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
        --project=${PROJECT} --region=us-central1 \
        --template-file-gcs-location=${TEMPLATE_IMAGE_SPEC} \
        --parameters inputSubscription=${SUBSCRIPTION},mongoDBUri=${MONGODB_HOSTNAME},database=${MONGODB_DATABASE_NAME},collection=${MONGODB_COLLECTION_NAME},deadletterTable=${DEADLETTER_TABLE}
```
