# BigQuery to MongoDB Dataflow Template

The [BigQueryToMongoDB](../../src/main/java/com/google/cloud/teleport/v2/mongodb/templates/BigQueryToMongoDb.java) pipeline
The BigQuery to MongoDB template is a batch pipeline that reads rows from a BigQUery and writes them to MongoDB as documents. Currently the pipeline stores each rows as a document. For releases, we are planning to add user defined functions to modify the schema before writing the documents to MongoDB. 

### Template parameters
**mongoDbUri** : MongoDB Connection URI. For example: _mongodb+srv://<username>:<password>@<server-connection-string>_.

**database** : Database in MongoDB to store the collection. For example: _my-db_.

**collection** : Name of the collection inside MongoDB database. For example: _my-collection_.

**inputTableSpec** : BigQuery input table spec. For example: _bigquery-project:dataset.output_table_.

## Getting Started

### Requirements
* Java 8
* Maven
* Bigquery dataset
* MongoDB host exists and is operational

### Building Template
This is a Flex Template meaning that the pipeline code will be containerized and the container will be used to launch the Dataflow pipeline.

#### Building Container Image
* Set environment variables.
  Set the pipeline vars
```sh
export PROJECT=<project-id>
export IMAGE_NAME="bigquery-to-mongodb"
export BUCKET_NAME=gs://<bucket-name>
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java8-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export TEMPLATE_MODULE="googlecloud-to-mongodb"
export APP_ROOT=/template/${TEMPLATE_MODULE}
export COMMAND_SPEC=${APP_ROOT}/resources/bigquery-to-mongodb-command-spec.json
export TEMPLATE_IMAGE_SPEC=${BUCKET_NAME}/images/bigquery-to-mongodb-image-spec.json

export MONGODB_HOSTNAME="mongodb+srv://<username>:<password>@<server-connection-string>"
export MONGODB_DATABASE_NAME=<database name>
export MONGODB_COLLECTION_NAME=<Collection name>
export INPUT_TABLE_SPEC=<input table spec>
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
  "image": "gcr.io/gsidemo-246315/bigquery-to-mongodb",
  "metadata": {
    "name": "MongoDB To BigQuery",
    "description": "A pipeline reads from BigQuery and writes to MongoDb.",
    "parameters": [
      {
        "name": "mongoDbUri",
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
        "name": "inputTableSpec",
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
* deadletterTable: Deadletter table for failed inserts in form: project-id:dataset.table


Template can be executed using the following gcloud command.
```sh
export JOB_NAME="${TEMPLATE_MODULE}-`date +%Y%m%d-%H%M%S-%N`"
gcloud beta dataflow flex-template run ${JOB_NAME} \
        --project=${PROJECT} --region=us-east1 \
        --template-file-gcs-location=${TEMPLATE_IMAGE_SPEC} \
        --parameters mongoDbUri=${MONGODB_HOSTNAME},database=${MONGODB_DATABASE_NAME},collection=${MONGODB_COLLECTION_NAME},inputTableSpec=${INPUT_TABLE_SPEC}
```
