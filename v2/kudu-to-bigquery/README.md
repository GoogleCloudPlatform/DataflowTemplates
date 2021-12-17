# Kudu to BigQuery Dataflow Template

## Pipeline

The [KuduToBigQuery](src/main/java/com/google/cloud/teleport/v2/templates/KuduToBigQuery.java) pipeline is a
batch pipeline which ingests table from Kudu and outputs the resulting records to BigQuery table.

## Getting Started

### Requirements
* Java 8
* Maven
* The Kudu table exists.
* The BigQuery output table exists.

### Building Template
This is a flex template meaning that the pipeline code will be containerized and the container will be used to launch the pipeline.


#### Building Container Image
* Set Environment Variables
```sh
export PROJECT=my-project
export IMAGE_NAME=my-image-name
export OUTPUT_TABLE=${PROJECT}:dataflow_template.bigquery_table
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java8-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export APP_ROOT=/template/kudu-to-bigquery
export COMMAND_SPEC=${APP_ROOT}/resources/kudu-to-bigquery-command-spec.json

```
* Build and push image to Google Container Repository
```sh
mvn clean package -Dimage=${TARGET_GCR_IMAGE} \
                  -Dbase-container-image=${BASE_CONTAINER_IMAGE} \
                  -Dbase-container-image.version=${BASE_CONTAINER_IMAGE_VERSION} \
                  -Dapp-root=${APP_ROOT} \
                  -Dcommand-spec=${COMMAND_SPEC}
```

#### Creating Template Spec

Create template spec in Cloud Storage with path to container image in Google Container Repository and pipeline metadata.

```json
{
	"image": "gcr.io/project-id/image-name",
	"metadata": {
		"name": "Kudu to BigQuery",
		"description": "Ingests table from Kudu and outputs the resulting records to BigQuery table",
		"parameters": [{
				"name": "kuduMasterAddresses",
				"label": "Kudu Master Addresses",
				"helpText": " Comma separated string of Kudu master node addresses, the minimum number of node is 1. For example kudu cluster with 3 master nodes master-1:7051,master-2:7051,master-3:7051",
        "regexes": [
         "^([A-Za-z0-9_.-]+:[0-9]+,)*([A-Za-z0-9_.-]+:[0-9]+){1}$"
        ],
				"paramType": "TEXT"
			},
      {
				"name": "kuduTableSource",
				"label": "kuduTableSource",
				"helpText": "Kudu table to write to BigQuery. e.g impala::default.kudu_table",
				"paramType": "TEXT"
			},
      {
				"name": "bigQueryTableSpec",
				"label": "bigQueryTableSpec",
				"helpText": "BigQuery destination table spec. e.g bigquery-project:dataset.output_table",
        "regexes": [
					".+:.+\\..+"
				],
				"paramType": "TEXT"
			},
      {
				"name": "includeColumns",
				"label": "includeColumns",
				"helpText": "Comma separated string of columns from Kudu table. e.g col1,col2,col3",
        "regexes": [
					"^([a-z0-9_]+,)*([a-z0-9_]+){1}$"
				],
				"paramType": "TEXT",
        "isOptional":true
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
* kuduMasterAddresses: Comma separated string of Kudu master node addresses, the minimum number of node is 1. For example kudu cluster with 3 master nodes master-1:7051,master-2:7051,master-3:7051
* kuduTableSource: Kudu table to write to BigQuery. e.g impala::default.kudu_table
* bigQueryTableSpec: BigQuery destination table spec. e.g bigquery-project:dataset.output_table

The template allows for the user to supply the following optional parameters:
* includeColumns: Comma separated string of columns from Kudu table. e.g col1,col2,col3

Template can be executed using the following gcloud command:
```sh
TEMPLATE_SPEC_GCSPATH=gs://path/to/template-spec
KUDU_MASTER_ADDRESSES=kudu-master-node-1:port,kudu-master-node-2:port,kudu-master-node-3:port
KUDU_TABLE_SOURCE=kudu-table-name
BIGQUERY_TABLE_SPEC=project-id:dataset.bigquery-table
INCLUDE_COLUMNS=col1,col2,col3
REGION=us-central1

gcloud beta dataflow jobs run $JOB_NAME \
        --project=$PROJECT --region=us-central1 --flex-template  \
        --gcs-location=$TEMPLATE_SPEC_GCSPATH \
        --region=$REGION \
        --parameters "^--^kuduMasterAddresses=$KUDU_MASTER_ADDRESSES--kuduTableSource=$KUDU_TABLE_SOURCE--bigQueryTableSpec=$BIGQUERY_TABLE_SPEC--includeColumns=$INCLUDE_COLUMNS
```
 *Note*: Additional options such as max workers, service account can be specified in the parameters section as shown below:bigQueryLoadingTemporaryDirectory=$BIGQUERY_LOADING_TEMPORARY_DIRECTORY+

 ```sh
  --parameters ^--^kuduMasterAddresses=$KUDU_MASTER_ADDRESSES--kuduTableSource=$KUDU_TABLE_SOURCE--bigQueryTableSpec=$BIGQUERY_TABLE_SPEC--includeColumns=$INCLUDE_COLUMNS--maxNumWorkers=5--serviceAccount=$serviceAccount
```

The "^--^" symbol is a custom delimiter declaration to replace "," default parameter delimiter to new symbol which in this example using "--" character. The custom delimiter is required since KUDU_MASTER_ADDRESSES may contain ",". Please check "$gcloud topic escaping" for more details.

For kudu table that managed by impala, you can specify the KUDU_TABLE_SOURCE in this format
impala::[database].[kudu-table-name]
