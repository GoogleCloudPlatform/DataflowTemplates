# BigQuery to Elasticsearch Dataflow Template

The [BigQueryToElasticsearch](src/main/java/com/google/cloud/teleport/v2/templates/BigQueryToElasticsearch.java) pipeline ingests 
data from a BigQuery table into Elasticsearch. The template can either read the entire table or read using a supplied query.

Pipeline flow is illustrated below

![alt text](img/bq-to-elasticsearch-dataflow.png "BQ to Elasticsearch pipeline flow")

## Getting Started

### Requirements
* Java 8
* Maven
* BigQuery table exists
* Elasticsearch nodes are reachable from the Dataflow workers

### Building Template
This is a Flex Template meaning that the pipeline code will be containerized and the container will be 
run on Dataflow.

#### Building Container Image
* Set environment variables
```sh
export PROJECT=my-project
export IMAGE_NAME=my-image-name
export BUCKET_NAME=gs://<bucket-name>
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java8-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export APP_ROOT=/template/<template-class>
export COMMAND_SPEC=${APP_ROOT}/resources/csv-to-elasticsearch-command-spec.json
export NODE_ADDRESSES=comma-separated-list-nodes
export INPUT_TABLE_SPEC=my-project:my-dataset.my-table
export INDEX=my-index
export DOCUMENT_TYPE=my-type
```
* Build and push image to Google Container Repository
```sh
mvn clean package -Dimage=${TARGET_GCR_IMAGE} \
                  -Dbase-container-image=${BASE_CONTAINER_IMAGE} \
                  -Dbase-container-image.version=${BASE_CONTAINER_IMAGE_VERSION} \
                  -Dapp-root=${APP_ROOT} \
                  -Dcommand-spec=${COMMAND_SPEC}
```

#### Creating Image Spec

Create file in Cloud Storage with path to container image in Google Container Repository.
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
* inputTableSpec: Table in BigQuery to read from in form of: my-project:my-dataset.my-table. Either this or query must be provided.
* nodeAddresses: Comma separated list of Elasticsearch nodes to connect to, ex: http://my-node1,http://my-node2
* index: The index toward which the requests will be issued, ex: my-index
* documentType: The document type toward which the requests will be issued, ex: my-document-type
* useLegacySql: Set to true to use legacy SQL (only applicable if supplying query). Default: false

The template has the following optional parameters:
* query: Query to run against input table,
    * For Standard SQL  ex: 'SELECT max_temperature FROM \`clouddataflow-readonly.samples.weather_stations\`'
    * For Legacy SQL ex: 'SELECT max_temperature FROM [clouddataflow-readonly:samples.weather_stations]'
* batchSize: Batch size in number of documents. Default: 1000
* batchSizeBytes: Batch size in number of bytes. Default: 5242880 (5mb)
* maxRetryAttempts: Max retry attempts, must be > 0. Default: no retries
* maxRetryDuration: Max retry duration in milliseconds, must be > 0. Default: no retries
* usePartialUpdates: Set to true to issue partial updates. Default: false
* idFnPath: Path to javascript file containing function to extract Id from document, ex: gs://path/to/idFn.js. Default: null
* idFnName: Name of javascript function to extract Id from document. Default: null
* indexFnPath: Path to javascript file containing function to extract Index from document, ex: gs://path/to/indexFn.js. Default: null
* indexFnName: Name of javascript function to extract Index from document. Default: null
    * Will override index provided.
* typeFnPath: Path to javascript file containing function to extract Type from document, ex: gs://path/to/typeFn.js. Default: null
* typeFnName: Name of javascript function to extract Type from document. Default: null
    * Will override type provided.

Template can be executed using the following API call:
```sh
API_ROOT_URL="https://dataflow.googleapis.com"
TEMPLATES_LAUNCH_API="${API_ROOT_URL}/v1b3/projects/${PROJECT}/templates:launch"
JOB_NAME="csv-to-elasticsearch-`date +%Y%m%d-%H%M%S-%N`"
time curl -X POST -H "Content-Type: application/json"     \
     -H "Authorization: Bearer $(gcloud auth print-access-token)" \
     "${TEMPLATES_LAUNCH_API}"`
     `"?validateOnly=false"`
     `"&dynamicTemplate.gcsPath=gs://path/to/image/spec"`
     `"&dynamicTemplate.stagingLocation=gs://path/to/stagingLocation" \
     -d '
      {
       "jobName":"'$JOB_NAME'",
       "parameters": {
                   "inputTableSpec":"'$INPUT_TABLE_SPEC'",
                   "nodeAddresses":"'$NODE_ADDRESSES'",
                   "index":"'$INDEX'",
                   "documentType":"'$DOCUMENT_TYPE'"
        }
       }
      '
```
