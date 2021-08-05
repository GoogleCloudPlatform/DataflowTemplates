# BigQuery to Elasticsearch Dataflow Template

The [BigQueryToElasticsearch](../../src/main/java/com/google/cloud/teleport/v2/elasticsearch/templates/BigQueryToElasticsearch.java) pipeline ingests
data from a BigQuery table into Elasticsearch. The template can either read the entire table or read using a supplied query.

Pipeline flow is illustrated below:

![alt text](../img/bigquery-to-elasticsearch-dataflow.png "BigQuery to Elasticsearch pipeline flow")

## Getting Started

### Requirements
* Java 8
* Maven
* BigQuery table exists
* Elasticsearch Instance exists

### Building Template
This is a Flex Template meaning that the pipeline code will be containerized, and the container will be
run on Dataflow.

#### Building Container Image
* Set environment variables that will be used in the build process.
```sh
export PROJECT=<my-project>
export IMAGE_NAME=<my-image-name>
export BUCKET_NAME=gs://<bucket-name>
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java8-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export TEMPLATE_MODULE=bigquery-to-elasticsearch
export APP_ROOT=/template/googlecloud-to-elasticsearch
export COMMAND_SPEC=${APP_ROOT}/resources/${TEMPLATE_MODULE}-command-spec.json
export TEMPLATE_IMAGE_SPEC=${BUCKET_NAME}/images/${TEMPLATE_MODULE}-image-spec.json

export INPUT_TABLE_SPEC=<my-project:my-dataset.my-table>
export CONNECTION_URL=<url-or-cloud_id>
export INDEX=<my-index>
export USE_LEGACY_SQL=false
export ELASTICSEARCH_USERNAME=<username>
export ELASTICSEARCH_PASSWORD=<password>

gcloud config set project ${PROJECT}
```
* Build and push image to Google Container Repository
```sh
mvn clean package \
    -Dimage=${TARGET_GCR_IMAGE} \
    -Dbase-container-image=${BASE_CONTAINER_IMAGE} \
    -Dbase-container-image.version=${BASE_CONTAINER_IMAGE_VERSION} \
    -Dapp-root=${APP_ROOT} \
    -Dcommand-spec=${COMMAND_SPEC} \
    -am -pl ${TEMPLATE_MODULE}
```

#### Creating Image Spec

Create file in Cloud Storage with path to container image in Google Container Repository.
```sh
echo '{
    "image":"'${TARGET_GCR_IMAGE}'",
    "metadata":{
      "name":"BigQuery to Elasticsearch",
      "description":"Replicates BigQuery data into an Elasticsearch index",
      "parameters":[
          {
              "name":"inputTableSpec",
              "label":"Table in BigQuery to read from",
              "helpText":"Table in BigQuery to read from in form of: my-project:my-dataset.my-table. Either this or query must be provided.",
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
              "name":"elasticsearchUsername",
              "label":"Username for Elasticsearch endpoint",
              "helpText":"Username for Elasticsearch endpoint",
              "paramType":"TEXT",
              "isOptional":false
          },
          {
              "name":"elasticsearchPassword",
              "label":"Password for Elasticsearch endpoint",
              "helpText":"Password for Elasticsearch endpoint",
              "paramType":"TEXT",
              "isOptional":false
          },
          {
              "name":"index",
              "label":"Elasticsearch index",
              "helpText":"The index toward which the requests will be issued, ex: my-index",
              "paramType":"TEXT",
              "isOptional":false
          },
          {
              "name":"useLegacySql",
              "label":"Set to true to use legacy SQL",
              "helpText":"Set to true to use legacy SQL (only applicable if supplying query). Default: false",
              "paramType":"TEXT",
              "isOptional":true
          },
          {
              "name":"query",
              "label":"Query to run against input table",
              "helpText":"Query to run against input table,.    * For Standard SQL  ex: 'SELECT max_temperature FROM \`clouddataflow-readonly.samples.weather_stations\`'.    * For Legacy SQL ex: 'SELECT max_temperature FROM [clouddataflow-readonly:samples.weather_stations]'",
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
* inputTableSpec: Table in BigQuery to read from in form of: my-project:my-dataset.my-table. Either this or query must be provided.
* connectionUrl: Elasticsearch URL in format http://hostname:[port] or Base64 encoded CloudId
* index: The index toward which the requests will be issued, ex: my-index
* elasticsearchUsername: Elasticsearch username used to connect to Elasticsearch endpoint
* elasticsearchPassword: Elasticsearch password used to connect to Elasticsearch endpoint

The template has the following optional parameters:
* useLegacySql: Set to true to use legacy SQL (only applicable if supplying query). Default: false
* query: Query to run against input table,
    * For Standard SQL  ex: 'SELECT max_temperature FROM \`clouddataflow-readonly.samples.weather_stations\`'
    * For Legacy SQL ex: 'SELECT max_temperature FROM [clouddataflow-readonly:samples.weather_stations]'
* batchSize: Batch size in number of documents. Default: 1000
* batchSizeBytes: Batch size in number of bytes. Default: 5242880 (5mb)
* maxRetryAttempts: Max retry attempts, must be > 0. Default: no retries
* maxRetryDuration: Max retry duration in milliseconds, must be > 0. Default: no retries

Template can be executed using the following gcloud command:
```sh
export JOB_NAME="${TEMPLATE_MODULE}-`date +%Y%m%d-%H%M%S-%N`"
gcloud beta dataflow flex-template run ${JOB_NAME} \
        --project=${PROJECT} --region=us-central1 \
        --template-file-gcs-location=${TEMPLATE_IMAGE_SPEC} \
        --parameters inputTableSpec=${INPUT_TABLE_SPEC},connectionUrl=${CONNECTION_URL},index=${INDEX},elasticsearchUsername=${ELASTICSEARCH_USERNAME},elasticsearchPassword=${ELASTICSEARCH_PASSWORD},useLegacySql=${USE_LEGACY_SQL}
```
