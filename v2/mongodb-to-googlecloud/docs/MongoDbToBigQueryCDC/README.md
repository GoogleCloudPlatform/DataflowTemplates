# MongoDB to BigQuery Dataflow Template

The [MongoDbToBigQueryCdc](src/main/java/com/google/cloud/teleport/v2/templates/MongoDbToBigQueryCdc.java) pipeline
MongoDb changestream pushes the changes to pubsub queue, The MongoDbToBigQueryCdc pipeline reads the documents pushed to pubsub and transform them as per the userOption provided at the runtime and Writes the data to BigQuery as a new TableRow.

## Getting Started

### Requirements
* Java 8
* Maven
* MongoDB host exists and is operational
* Bigquery dataset exists
* Changestream running that pushes the changes from mongoDb to PubSub topic.

### Building Template
This is a Flex Template meaning that the pipeline code will be containerized and the container will be used to launch the Dataflow pipeline.

#### Building Container Image
* Set environment variables.
  Set the pipeline vars
```sh
export PROJECT=<project-id>
export IMAGE_NAME="mongodb-to-bigquery-cdc"
export BUCKET_NAME=gs://<bucket-name>
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java8-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export TEMPLATE_MODULE=mongodb-to-googlecloud
export APP_ROOT=/template/${TEMPLATE_MODULE}
export COMMAND_SPEC=${APP_ROOT}/resources/mongodb-to-bigquery-cdc-command-spec.json
export TEMPLATE_IMAGE_SPEC=${BUCKET_NAME}/images/mongodb-to-bigquery-image-spec.json

export MONGODB_HOSTNAME="mongodb+srv://<username>:<password>@<server-connection-string>"
export MONGODB_DATABASE_NAME=<database name>
export MONGODB_COLLECTION_NAME=<Collection name>
export OUTPUT_TABLE_SPEC=<output tabel spec>
export USER_OPTION = <user-option>
export INPUT_TOPIC=<input-topic>

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
  "image": "gcr.io/<project-name>/mongodb-to-bigquery-cdc",
  "metadata": {
    "name": "MongoDb To BigQuery CDC",
    "description": "A pipeline reads changestream from MongoDb and writes to BigQuery.",
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
        "name": "outputTableSpec",
        "label": "outputTableSpec",
        "helpText": "BigQuery destination table spec. e.g bigquery-project:dataset.output_table",
        "paramType": "TEXT"
      },
      {
        "name": "userOption",
        "label": "User option",
        "helpText": " ",
        "is_optional": true,
        "paramType": "TEXT"
      },
      {
        "name": "inputTopic",
        "label": "input Pubsub Topic name",
        "helpText": "Topic Name to read from e.g. projects/<project-name>/topics/<topic-name>",
        "is_optional": false,
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
* outputTableSpec: The BQ table where we want to write the data read from MongoDb collection to.
* inputTopic: the Topic where the changes are pushed from MongoDb changestream

The template has the following optional parameters:
* userOption: The user option to Flatten the document or store it as a jsonString. To Flatten the document pass the parameter as "FLATTEN".

Template can be executed using the following gcloud command.
```sh
export JOB_NAME="${TEMPLATE_MODULE}-`date +%Y%m%d-%H%M%S-%N`"
gcloud beta dataflow flex-template run ${JOB_NAME} \
        --project=${PROJECT} --region=us-east1 \
        --template-file-gcs-location=${TEMPLATE_IMAGE_SPEC} \
        --parameters mongoDbUri=${MONGODB_HOSTNAME},database=${MONGODB_DATABASE_NAME},collection=${MONGODB_COLLECTION_NAME},outputTableSpec=${OUTPUT_TABLE_SPEC},inputTopic=${INPUT_TOPIC},userOption=${USER_OPTION}
```
