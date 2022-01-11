# DataStream to BigQuery Dataflow Template

The [DataStreamToBigQuery](src/main/java/com/google/cloud/teleport/v2/templates/DataStreamToBigQuery.java) pipeline
ingests data supplied by DataStream, optionally applies a Javascript or Python UDF if supplied
and writes the data to BigQuery staging tables.  A Merge is run against BigQuery to load data into
the final table.

## Getting Started

### Requirements
* Java 8
* Maven
* DataStream stream is created and sending data to storage
* PubSub Subscription exists or GCS Bucket contains data

### Building Template
This is a Flex Template meaning that the pipeline code will be containerized and the container will be
used to launch the Dataflow pipeline.

#### Building Container Image
* Set environment variables.

```sh
export PROJECT=<my-project>
export IMAGE_NAME=datastream-to-bigquery
export BUCKET_NAME=gs://<bucket-name>
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java8-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export APP_ROOT=/template/${IMAGE_NAME}
export DATAFLOW_JAVA_COMMAND_SPEC=${APP_ROOT}/resources/${IMAGE_NAME}-command-spec.json
export TEMPLATE_IMAGE_SPEC=${BUCKET_NAME}/images/${IMAGE_NAME}-image-spec.json

export TOPIC=projects/${PROJECT}/topics/<topic-name>
export SUBSCRIPTION=projects/${PROJECT}/subscriptions/<subscription-name>
export DEADLETTER_TABLE=${PROJECT}:${DATASET_TEMPLATE}.dead_letter

gcloud config set project ${PROJECT}
```

* Build and push image to Google Container Repository

```sh
mvn clean package \
-Dimage=${TARGET_GCR_IMAGE} \
-Dbase-container-image=${BASE_CONTAINER_IMAGE} \
-Dbase-container-image.version=${BASE_CONTAINER_IMAGE_VERSION} \
-Dapp-root=${APP_ROOT} \
-Dcommand-spec=${DATAFLOW_JAVA_COMMAND_SPEC} \
-am -pl ${IMAGE_NAME}
```

#### Creating Image Spec

* Create file in Cloud Storage with path to container image in Google Container Repository.
```sh
echo '{
    "image":"'${TARGET_GCR_IMAGE}'",
    "metadata":{"name":"PubSub CDC to BigQuery",
    "description":"Replicate Pub/Sub Data into BigQuery Tables",
    "parameters":[
        {
            "name":"inputSubscription",
            "label":"PubSub Subscription Name",
            "helpText":"Full subscription reference",
            "paramType":"TEXT"
        },
        {
            "name":"autoMapTables",
            "label":"Automatically add new BigQuery tables and columns as they appear",
            "helpText":"Automatically add new BigQuery tables and columns as they appear",
            "paramType":"TEXT",
            "isOptional":true
        },
        {
            "name":"outputDatasetTemplate",
            "label":"The BigQuery Dataset Name or column template",
            "helpText":"The BigQuery Dataset Name or column template",
            "paramType":"TEXT",
            "isOptional":true
        },
        {
            "name":"outputTableNameTemplate",
            "label":"The BigQuery Table Name or column template",
            "helpText":"The BigQuery Table Name or column template",
            "paramType":"TEXT",
            "isOptional":true
        },
        {
            "name":"outputTableSpec",
            "label":"DEPRECATED: Use outputDatasetTemplate AND outputTableNameTemplate",
            "helpText":"DEPRECATED: Use outputDatasetTemplate AND outputTableNameTemplate",
            "paramType":"TEXT",
            "isOptional":true
        },
        {"name":"outputDeadletterTable",
            "label":"Deadletter Queue Table",
            "helpText":"DLQ Table Ref: PROJECT:dataset.dlq",
            "paramType":"TEXT"
        },
        {
            "name":"autoscalingAlgorithm","label":"Autoscaling algorithm to use",
            "helpText":"Autoscaling algorithm to use: THROUGHPUT_BASED",
            "paramType":"TEXT",
            "isOptional":true
        },
        {
            "name":"numWorkers","label":"Number of workers Dataflow will start with",
            "helpText":"Number of workers Dataflow will start with",
            "paramType":"TEXT",
            "isOptional":true
        },

        {
            "name":"maxNumWorkers","label":"Maximum number of workers Dataflow job will use",
            "helpText":"Maximum number of workers Dataflow job will use",
            "paramType":"TEXT",
            "isOptional":true
        },
        {
            "name":"workerMachineType","label":"Worker Machine Type to use in Dataflow Job",
            "helpText":"Machine Type to Use: n1-standard-4",
            "paramType":"TEXT",
            "isOptional":true
        }
    ]},
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
* inputSubscription: PubSub subscription to read from (ie. projects/<project-id>/subscriptions/<subscription-name>)
* outputDatasetTemplate: The name of the dataset or templated logic to extract it (ie. 'prefix_{schema_name}')
* outputTableNameTemplate: The name of the table or templated logic to extract it (ie. 'prefix_{table_name}')
* outputDeadletterTable: Deadletter table for failed inserts in form: project-id:dataset.table

The template has the following optional parameters:
* javascriptTextTransformGcsPath: Gcs path to javascript udf source. Udf will be preferred option for transformation if supplied. Default: null
* javascriptTextTransformFunctionName: UDF Javascript Function Name. Default: null

* maxRetryAttempts: Max retry attempts, must be > 0. Default: no retries
* maxRetryDuration: Max retry duration in milliseconds, must be > 0. Default: no retries

Template can be executed using the following API call:
```sh
export JOB_NAME="${IMAGE_NAME}-`date +%Y%m%d-%H%M%S-%N`"
gcloud beta dataflow flex-template run ${JOB_NAME} \
        --project=${PROJECT} --region=us-central1 \
        --template-file-gcs-location=${TEMPLATE_IMAGE_SPEC} \
        --parameters inputSubscription=${SUBSCRIPTION},outputDeadletterTable=${DEADLETTER_TABLE}
```
