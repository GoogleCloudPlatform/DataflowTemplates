# PubSub CDC to BigQuery Dataflow Template

The [PubSubCdcToBigQuery](src/main/java/com/google/cloud/teleport/v2/templates/PubSubCdcToBigQuery.java) pipeline
ingests data from a PubSub subscription, optionally applies a JavaScript or Python UDF if supplied
and writes the data to BigQuery.

## Getting Started

### Requirements
* Java 11
* Maven
* PubSub Subscription exists

### Building Template
This is a Flex Template meaning that the pipeline code will be containerized and the container will be
used to launch the Dataflow pipeline.

#### Building Container Image
* Set environment variables.

```sh
export PROJECT=<my-project>
export IMAGE_NAME=pubsub-cdc-to-bigquery
export BUCKET_NAME=gs://<bucket-name>
export DATASET_TEMPLATE=<dataset-name>
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java11-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export APP_ROOT=/template/pubsub-cdc-to-bigquery
export DATAFLOW_JAVA_COMMAND_SPEC=${APP_ROOT}/resources/pubsub-cdc-to-bigquery-command-spec.json
export TEMPLATE_IMAGE_SPEC=${BUCKET_NAME}/images/pubsub-cdc-to-bigquery-image-spec.json

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
-am -pl pubsub-cdc-to-bigquery
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
        {
            "name":"outputDeadletterTable",
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
        },
        {
            "name":"javascriptTextTransformGcsPath","label":"JavaScript File Path",
            "helpText":"JS File Path",
            "paramType":"TEXT",
            "isOptional":true
        },
        {
            "name":"javascriptTextTransformFunctionName","label":"UDF JavaScript Function Name",
            "helpText":"JS Function Name",
            "paramType":"TEXT",
            "isOptional":true
        },
        {
            "name":"pythonTextTransformGcsPath","label":"Python File Path",
            "helpText":"PY File Path",
            "paramType":"TEXT",
            "isOptional":true
        },
        {
            "name":"pythonTextTransformFunctionName","label":"UDF Python Function Name",
            "helpText":"PY Function Name",
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
* javascriptTextTransformGcsPath: Gcs path to JavaScript UDF source. Udf will be preferred option for transformation if supplied. Default: null
* javascriptTextTransformFunctionName: UDF JavaScript Function Name. Default: null
* pythonTextTransformGcsPath: Gcs path to Python UDF source. UDF will be preffered option for transformation if supplied. Default: null
* pythonTextTransformFunctionName: UDF Python Function Name. Default: null
* schemaFilePath: Gcs path to a file containing desired data types for BigQuery table creation. Default: null. Default: null

Template can be executed using the following API call:

```sh
export JOB_NAME="pubsub-cdc-to-bigquery-`date +%Y%m%d-%H%M%S-%N`"
gcloud beta dataflow flex-template run ${JOB_NAME} \
        --project=${PROJECT} --region=us-central1 \
        --template-file-gcs-location=${TEMPLATE_IMAGE_SPEC} \
        --parameters inputSubscription=${SUBSCRIPTION}, \
                     outputDatasetTemplate=${DATASET_TEMPLATE}, \
                     outputTableNameTemplate=${TABLE_TEMPLATE}, \
                     outputDeadletterTable=${DEADLETTER_TABLE}
```

# Optional Settings

  The template has two optional settings, **custom schemas** and **text transformer UDFs**.

## Custom Schemas
By default, the pipeline is designed to generate DDL in BigQuery if a table for a particular event does not currently exist. The tables generated default to STRING types for all columns but this can be overridden by loading a [BigQuery schema file](https://cloud.google.com/bigquery/docs/schemas) to GCS and providing the path via the **schemaFilePath** paramater on job creation. The pipeline will apply the custom datatype based on field name during table creation.

## Text Transformer UDFs
The template gives you the option to apply a UDF to the pipeline by sloading a script to GCS and providing the path and function name via the **{Language}TextTransformGcsPath** parameter on job creation. Currently JavaScript, and Python are supported. The function should expect to receive and return a JSON string. Example transformations are provided below.

### JavaScript

```
/**
 * A good transform function.
 * @param {string} inJson
 * @return {string} outJson
 */
function transform(inJson) {
  var obj = JSON.parse(inJson);
  obj.someProp = "someValue";
  return JSON.stringify(obj);
}
```

### Python

```
"""
A good transform function.
@param {string} inJson
@return {string} outJson
"""
import json
import sys

def transform(event):
  """ Return a Dict or List of Dict Objects.  Return None to discard
      Input: JSON string
      Output: Python Dictionary
  """
  event['someProp'] = 'someValue'
  event = json.dumps(event)
  return event

# DO NOT EDIT BELOW THIS LINE
# The remaining code is boilerplate required for
# dynamic handling of event batches from the main
# dataflow pipeline. the transform() function should
# be the initial entrypoint for your custom logic.
def _handle_result(result):
  if isinstance(result, list):
    for event in result:
      if event:
        print(json.dumps(event))
  elif result:
    print(json.dumps(result))

if __name__ == '__main__':
  # TODO: How do we handle the case where there are no messages
  file_name = sys.argv[1]
  with open(file_name, "r") as data_file:
    for cnt, raw_data in enumerate(data_file):
      raw_events = "[%s]" % raw_data

      data = json.loads(raw_events)

      if isinstance(data, list):
        for event in data:
          _handle_result(transform(event))
      else:
        event = data
        _handle_result(transform(event))
```
