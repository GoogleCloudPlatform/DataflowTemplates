# AstraDB to BigQuery Dataflow Template

The [AstraDbToBigQuery](../../src/main/java/com/google/cloud/teleport/v2/astradb/templates/AstraDbToBigQuery.java) pipeline Reads the data from AstraDb table, Writes the data to BigQuery.
The AstraDB to BigQuery template is a batch pipeline that reads records from AstarDB and writes them to BigQuery as specified in userOption.

If the destination table does not exist in BigQuery the pipeline will create it with the following values:
- The `Dataset Id` id will be the source Cassandra Keyspace.
- The `Table Id` id will be the source Cassandra Table.

The schema of the destination table will be inferred from the source Cassandra table.
- List, Set will be mapped to BigQuery REPEATED fields.
- Map will be mapped to BigQuery RECORD fields.
- All other types will be mapped to BigQuery fields with the corresponding types.
- Cassandra UDT and Tuples are not supported.

## Getting Started

### Requirements
* Java 11
* Maven
* AstraDB accounts with a token
* Google Cloud Platform project with BigQuery and Dataflow APIs enabled

### Template parameters

- `**astraToken**` (_required_) : Token (credentials) to connect to Astra deployed as a secret in Google Secret Manager, here you provide the full resources id `projects/<projectId>/secrets/<secret-name>/versions/<version>`

- `**astraSecureConnectBundle**`(_required_) : Zip archive containing certificates to open a mTLS connection. This file is stored as secret in Google Secret Manager, here you provide the full resources id `projects/<projectId>/secrets/<secret-name>/versions/<version>` 

- `**astraKeyspace**`(_required_) : Name of the keyspace where the source data is located in Astra.

- `**astraTable**`(_required_) : Name of the table where the source data is located in Astra.

- `**astraQuery**`(_optional_) : Optional query to limit which data is read from Astra. If not provided, the whole table will be read.

- `**outputTableSpec**` (_optional_) : BigQuery destination table spec. e.g _bigquery-project:dataset.output_table_,If not provided the table will be created with the following convention: cassandra keyspace=dataset name and cassandra table= table name

### Building Template
This is a Flex Template meaning that the pipeline code will be containerized and the container will be used to launch the Dataflow pipeline.

#### Building Container Image
* Set environment variables.
  Set the pipeline vars
```sh
export PROJECT=<project-id>
export IMAGE_NAME="astradb-to-bigquery"
export BUCKET_NAME=gs://<bucket-name>
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java11-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export TEMPLATE_MODULE="astradb-to-bigquery"
export APP_ROOT="/template/astradb-to-bigquery"
export COMMAND_SPEC=${APP_ROOT}/resources/astradb-to-bigquery-command-spec.json
export TEMPLATE_IMAGE_SPEC=${BUCKET_NAME}/images/astradb-to-bigquery-image-spec.json

export ASTRA_TOKEN="projects/<project-id>/secrets/<token-secret-name>/versions/<version>"
export ASTRA_SECURE_CONNECT_BUNDLE="projects/<project-id>/secrets/<scb-secret-name>/versions/<version>"
export ASTRA_KEYSPACE=<keyspace-name>
export ASTRA_TABLE=<table-name>
export OUTPUT_TABLE_SPEC=<output tabel spec>

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
  "name": "AstraDB to BigQuery",
  "description": "A pipeline reads from AstraDB and writes to BigQuery.",
  "parameters": [
    {
      "name": "astraToken",
      "label": "Astra Token",
      "helpText": "Astra Token secret resource id",
      "is_optional": false,
      "paramType": "TEXT"
    },
    {
      "name": "astraSecureConnectBundle",
      "label": "Connect Bundle Zip archive secret resource id",
      "helpText": "Stored as a secret, this zip contains X509 certificate and private key to connect to AstraDB",
      "is_optional": false,
      "paramType": "TEXT"
    },
    {
      "name": "astraKeyspace",
      "label": "AstraDB Keyspace",
      "helpText": "Name of the keyspace in Astra",
      "is_optional": false,
      "paramType": "TEXT"
    },
    {
      "name": "astraTable",
      "label": "AstraDB table",
      "helpText": "Name of the Table in Astra",
      "is_optional": false,
      "paramType": "TEXT"
    },
    {
      "name": "astraQuery",
      "label": "AstraDB query",
      "helpText": "Query to filter some records from Astra",
      "is_optional": true,
      "paramType": "TEXT"
    },
    {
      "name": "outputTableSpec",
      "label": "BigQuery table spec to write the data",
      "helpText": "BigQuery destination table spec. e.g bigquery-project:dataset.output_table",
      "is_optional": true,
      "paramType": "TEXT"
    }
  ]
}
```

### Staging the Template

```bash
mvn clean package -PtemplatesStage  \
  -DskipTests \
 -DprojectId="integrations-379317" \
 -DbucketName="gs://dataflow-staging-us-central1-747469159044" \
 -DstagePrefix="images/$(date +%Y_%m_%d)_01" \
 -DtemplateName="AstraDb_to_BigQuery_Text_Flex" \
 -pl v2/astradb-to-bigquery -am
```

### Testing Template

The template unit tests can be run using:
```sh
mvn test
```

### Executing Template

The template requires the following parameters:
- `astraToken`: Token (credentials) to connect to Astra deployed as a secret in Google Secret Manager, here you provide the full resources id `projects/<projectId>/secrets/<secret-name>/versions/<version>`
- `astraSecureConnectBundle`: Zip archive containing certificates to open a mTLS connection. This file is stored as secret in Google Secret Manager, here you provide the full resources id `projects/<projectId>/secrets/<secret-name>/versions/<version>`
- `astraKeyspace`: Name of the keyspace where the source data is located in Astra.
- `astraTable`: Name of the table where the source data is located in Astra.

The template has the following optional parameters:
- `astraQuery` : Optional query to limit which data is read from Astra. If not provided, the whole table will be read. 
- `outputTableSpec` : BigQuery destination table spec. e.g _bigquery-project:dataset.output_table_,If not provided the table will be created with the following convention: cassandra keyspace=dataset name and cassandra table= table name

Template can be executed using the following gcloud command.
```sh
export JOB_NAME="${TEMPLATE_MODULE}-`date +%Y%m%d-%H%M%S-%N`"
gcloud beta dataflow flex-template run ${JOB_NAME} \
        --project=${PROJECT} --region=us-east1 \
        --template-file-gcs-location=${TEMPLATE_IMAGE_SPEC} \
        --parameters astraToken=${ASTRA_TOKEN},astraSecureConnectBundle=${ASTRA_SECURE_CONNECT_BUNDLE},astraKeyspace=${ASTRA_KEYSPACE},astraTable=${ASTRA_TABLE},outputTableSpec=${OUTPUT_TABLE_SPEC}
```
