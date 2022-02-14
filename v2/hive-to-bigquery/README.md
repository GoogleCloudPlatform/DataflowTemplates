# Hive to BigQuery Dataflow Template

The [HiveToBigQuery](src/main/java/com/google/cloud/teleport/v2/templates
/HiveToBigQuery.java) pipeline is a
batch pipeline which ingests data from Hive and outputs
 the resulting records to BigQuery.

### Requirements
* Java 8
* Maven
* Hive 2
* Docker
* The Hive input table exists.
* The Hive metastore URI and HDFS URI are reachable from the Dataflow worker
 machines.

## Getting Started
### Building Template
This is a Flex template meaning that the pipeline code will be containerized and the container will be used to launch the Dataflow pipeline.

#### Compiling the pipeline
Execute the following command from the directory containing the parent pom.xml (v2/):

```shell script
mvn clean compile -pl hive-to-bigquery -am
```

#### Execuing unit tests
Execute the following command from the directory containing the parent pom.xml (v2/):

```shell script
mvn clean test -pl hive-to-bigquery -am
```

### Building Container Image
Execute the following command from the directory containing the parent pom.xml (v2/):
* Set environment variables that will be used in the build process.

```sh
export PROJECT=my-project
export IMAGE_NAME=hive-to-bigquery
export BUCKET_NAME=gs://<bucket-name>
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java8-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export APP_ROOT=/template/${IMAGE_NAME}
export COMMAND_SPEC=${APP_ROOT}/resources/${IMAGE_NAME}-command-spec.json
export TEMPLATE_IMAGE_SPEC=${BUCKET_NAME}/images/${IMAGE_NAME}-image-spec.json

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
    -am -pl ${IMAGE_NAME}
```

#### Creating Image Spec
Create a file with the metadata required for launching the Flex template. Once created, this file should
be placed in GCS.

__Note:__ The ```image``` property would point to the ```${TARGER_GCR_IMAGE}``` defined previously.

```sh
echo '{
  "image": "'${TARGET_GCR_IMAGE}'",
  "metadata": {
    "name": "Replicates from a Hive table into BigQuery",
    "description": "Hive to BigQuery",
    "parameters": [
      {
        "name": "metastoreUri",
        "label": "metastoreUri",
        "helpText": "thrift server URI such as thrift://thrift-server-host:port",
        "paramType": "TEXT",
        "isOptional": false
      },
      {
        "name": "hiveDatabaseName",
        "label": "hiveDatabaseName",
        "helpText": "Input Hive Database Name",
        "paramType": "TEXT",
        "is_optional": false
      },
      {
        "name": "hiveTableName",
        "label": "hiveTableName",
        "helpText": "Input Hive table name",
        "paramType": "TEXT",
        "is_optional": false
      },
      {
        "name": "outputTableSpec",
        "label": "outputTableSpec",
        "helpText": "Output BigQuery table spec such as myproject:mydataset.mytable",
        "paramType": "TEXT",
        "is_optional": false
      },
      {
        "name": "hivePartitionCols",
        "label": "hivePartitionCols",
        "helpText": "the name of the columns that are partitions such as [\"col1\", \"col2\"]",
        "paramType": "TEXT",
        "is_optional": true
      },
      {
        "name": "filterString",
        "label": "filterString",
        "helpText": "the filter details",
        "paramType": "TEXT",
        "is_optional": true
      },
      {
        "name": "partitionType",
        "label": "partitionType",
        "helpText": "partition type in BigQuery. Currently, only Time is
        available",
        "paramType": "TEXT",
        "is_optional": true
      },
      {
        "name": "partitionCol",
        "label": "partitionCol",
        "helpText": "the name of column that is the partition in BigQuery",
        "paramType": "TEXT",
        "is_optional": true
      },
      {
          "name": "maxRetryAttempts",
          "label": "Max retry attempts",
          "helpText": "Max retry attempts, must be > 0. Default: no retries",
          "paramType": "TEXT",
          "isOptional": true
      },
      {
          "name": "maxRetryDuration",
          "label": "Max retry duration in milliseconds",
          "helpText": "Max retry duration in milliseconds, must be > 0. Default: no retries",
          "paramType": "TEXT",
          "isOptional": true
      },
      {
          "name": "autoscalingAlgorithm","label":"Autoscaling algorithm to use",
          "helpText": "Autoscaling algorithm to use: THROUGHPUT_BASED",
          "paramType": "TEXT",
          "isOptional": true
      },
      {
          "name": "numWorkers","label":"Number of workers Dataflow will start with",
          "helpText": "Number of workers Dataflow will start with",
          "paramType": "TEXT",
          "isOptional": true
      },
      {
          "name": "maxNumWorkers","label":"Maximum number of workers Dataflow job will use",
          "helpText": "Maximum number of workers Dataflow job will use",
          "paramType": "TEXT",
          "isOptional": true
      },
      {
          "name": "workerMachineType","label":"Worker Machine Type to use in Dataflow Job",
          "helpText": "Machine Type to Use: n1-standard-4",
          "paramType": "TEXT",
          "isOptional": true
      }
    ]
  },
  "sdkInfo": {
    "language": "JAVA"
  }
}' > image_spec.json
gsutil cp image_spec.json ${TEMPLATE_IMAGE_SPEC}
rm image_spec.json
```

### Executing Template

The template requires the following parameters:

* metastoreUri: thrift server URI
* hiveDatabaseName: Input Hive database name
* hiveTableName: Input Hive table name
* outputTableSpec: Output BigQuery table spec

The templatehas the following optional parameters:
* hivePartitionCols: the name of the columns that are partitions in Hive table
* filterString: the filter details
* partitionType: partition type in BigQuery
* partitionCol: the name of the column that is partition in BigQuery

Template can be executed using the ```gcloud``` sdk.
__**Note:**__ To use the gcloud command-line tool to run Flex templates, you must have [Cloud SDK](https://cloud.google.com/sdk/downloads) version 284.0.0 or higher.

```sh
export JOB_NAME="${IMAGE_NAME}-`date +%Y%m%d-%H%M%S-%N`"
gcloud dataflow flex-template run ${JOB_NAME} \
        --template-file-gcs-location=${TEMPLATE_IMAGE_SPEC} \
        --parameters="metastoreUri=thrift://my-ip-address:9083,hiveDatabaseName=myhivedbname,hiveTableName=myhivetable,outputTableSpec=my-project:my_dataset.my_table"
```
