# DataStream to MongoDB Dataflow Template

The [DataStreamMongoDBToMongoDB](src/main/java/com/google/cloud/teleport/v2/templates/DataStreamMongoDBToMongoDB.java) pipeline
ingests data supplied by DataStream, optionally applies a Javascript or Python UDF if supplied
and writes the data to MongoDB collections.

## Getting Started

### Requirements
* Java 11
* Maven
* DataStream stream is created and sending data to storage
* PubSub Subscription exists or GCS Bucket contains data
* The destination database is created and you have the necessary permissions to write to it

### Building Template
This is a Flex Template meaning that the pipeline code will be containerized and the container will be
used to launch the Dataflow pipeline.

#### Building Container Image
* Set environment variables.

```sh
export PROJECT=<my-project>
export IMAGE_NAME=datastream-mongodb-to-mongodb
export BUCKET_NAME=gs://<bucket-name>
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java11-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export APP_ROOT=/template/${IMAGE_NAME}
export DATAFLOW_JAVA_COMMAND_SPEC=${APP_ROOT}/resources/${IMAGE_NAME}-command-spec.json
export TEMPLATE_IMAGE_SPEC=${BUCKET_NAME}/images/${IMAGE_NAME}-image-spec.json

export TOPIC=projects/${PROJECT}/topics/<topic-name>
export SUBSCRIPTION=projects/${PROJECT}/subscriptions/<subscription-name>

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
    "metadata":{"name":"DataStream to MongoDB",
    "description":"Replicate DataStream Data into MongoDB Collections",
    "parameters":[
        {
            "name":"inputFilePattern",
            "label":"GCS Stream location",
            "helpText":"GCS Stream location",
            "paramType":"TEXT"
        },
        {
            "name":"inputFileFormat",
            "label":"GCS Stream format",
            "helpText":"GCS Stream format",
            "paramType":"TEXT"
        },
        {
            "name":"streamName",
            "label":"Datastream Name",
            "helpText":"Datastream Name",
            "paramType":"TEXT"
        },
        {
            "name":"inputSubscription",
            "label":"PubSub Subscription Name",
            "helpText":"Full subscription reference",
            "paramType":"TEXT"
        },
        {
            "name":"projectId",
            "label":"Project ID",
            "helpText":"The Google Cloud project ID",
            "paramType":"TEXT"
        },
        {
            "name":"databaseName",
            "label":"Target Database Name",
            "helpText":"The target database to write to",
            "paramType":"TEXT"
        },
        {
            "name":"databaseCollection",
            "label":"MongoDB Collection Filter (optional)",
            "helpText":"If specified, only replicate this collection. If not specified, replicate all collections.",
            "paramType":"TEXT",
            "isOptional":true
        },
        {
            "name":"shadowCollectionPrefix",
            "label":"Shadow Collection Prefix",
            "helpText":"The prefix used to name shadow collections. Default: shadow_",
            "paramType":"TEXT",
            "isOptional":true
        },
        {
            "name":"batchSize",
            "label":"Batch Size",
            "helpText":"The batch size for writing to the target database",
            "paramType":"TEXT",
            "isOptional":true
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
            "name":"deadLetterQueueDirectory","label":"Dead Letter Queue Directory",
            "helpText":"The file path used when storing the error queue output",
            "paramType":"TEXT",
            "isOptional":true
        },
        {
            "name":"dlqRetryMinutes","label":"Dead Letter Queue Retry Minutes",
            "helpText":"The number of minutes between dead letter queue retries. Defaults to 10.",
            "paramType":"TEXT",
            "isOptional":true
        },
        {
            "name":"dlqMaxRetryCount","label":"Dead Letter Queue Maximum Retry Count",
            "helpText":"The max number of times temporary errors can be retried through DLQ. Defaults to 500.",
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

### Backfill and CDC Event Processing

This template processes both backfill (initial data load) and CDC (change data capture) events from DataStream in a unified pipeline. All events are processed together in the order they are received, primarily ordered by their timestamps.

The template automatically identifies backfill and CDC events based on metadata fields in the DataStream events:
- The `_metadata_change_type` field if present
- Heuristics based on the presence/absence of CDC-specific fields like `_metadata_log_file` and `_metadata_log_position`

For proper event ordering, the template uses:
- Timestamp field (`_metadata_timestamp`) as the primary ordering mechanism
- Event type (backfill events processed before CDC events with the same timestamp)

This unified approach simplifies the pipeline while still ensuring correct event ordering and processing.

#### Shadow Collections

When processing events, the template creates and uses shadow collections to track which events have been processed. Shadow collections:

- Store ordering information for each document written to the destination database
- Help ensure events are processed in the correct order
- Prevent duplicate processing of events

Each shadow document contains:
- The same document ID as the corresponding data document (`_metadata_id`)
- A timestamp from the source event (`Timestamp`)
- Event source information (`_metadata_source_type`, backfill or CDC)
- Event type information (`_metadata_change_type`, READ, CREATE, UPDATE, DELETE)
- Processing timestamp (`processed_at`)

The template uses only the timestamp for sequence comparison, which simplifies the implementation while still providing effective ordering guarantees. When a new event arrives, its timestamp is compared with the timestamp in the corresponding shadow document (if one exists). If the new event's timestamp is greater than the existing timestamp, the event is processed; otherwise, it is skipped as a duplicate or stale event.

All operations on data and shadow collections are performed within transactions to ensure consistency between them. This guarantees that either both the data document and its corresponding shadow document are updated, or neither is updated.

### Dead Letter Queue

The template includes a robust Dead Letter Queue (DLQ) mechanism to handle errors during processing:

- Failed events are written to a DLQ directory in Cloud Storage
- The DLQ manager supports retrying failed events after a configurable delay
- Maximum retry count can be configured to prevent infinite retries
- DLQ events can be consumed from a PubSub subscription for more immediate retries

This approach ensures that no data is lost due to temporary failures, while also providing visibility into persistent errors.

### Executing Template

The template requires the following parameters:
* inputFilePattern: The GCS location which contains the DataStream's data.
* inputFileFormat: The format of the input files (avro or json).
* streamName: The name of the DataStream stream.
* projectId: The Google Cloud project ID for the destination database.
* databaseName: The destination database to write to (default: (default)).

The template has the following optional parameters:
* inputSubscription: PubSub subscription to read from (ie. projects/<project-id>/subscriptions/<subscription-name>).
* databaseCollection: If specified, only replicate this collection. If not specified, replicate all collections.
* shadowCollectionPrefix: The prefix used to name shadow collections. Default: "shadow_".
* batchSize: The batch size for writing to the destination database. Default: 500.
* deadLetterQueueDirectory: The file path used when storing the error queue output.
* dlqRetryMinutes: The number of minutes between dead letter queue retries. Default: 10.
* dlqMaxRetryCount: The max number of times temporary errors can be retried through DLQ. Default: 500.

Template can be executed using the following API call:
```sh
export JOB_NAME="${IMAGE_NAME}-`date +%Y%m%d-%H%M%S-%N`"
gcloud beta dataflow flex-template run ${JOB_NAME} \
        --project=${PROJECT} --region=us-central1 \
        --template-file-gcs-location=${TEMPLATE_IMAGE_SPEC} \
        --parameters inputFilePattern=gs://<bucket>/datastream/<stream-name>/*,inputFileFormat=json,streamName=<stream-name>,projectId=${PROJECT},databaseName=(default),shadowCollectionPrefix=shadow_
```

### Running with Direct Runner

For local development and testing, you can run the template using the Direct Runner instead of the Dataflow service. This allows you to test your pipeline locally before deploying it to the cloud.

#### Building the Project

First, build the project:

```sh
cd /path/to/DataflowTemplates
mvn clean package -DskipTests -pl v2/datastream-mongodb-to-mongodb -am
```

#### Executing with Direct Runner

Run the template with the Direct Runner using the following command:

```sh
cd /path/to/DataflowTemplates
mvn exec:java \
  -Dexec.mainClass=com.google.cloud.teleport.v2.templates.DataStreamMongoDBToMongoDB \
  -Dexec.args=" \
    --runner=DirectRunner \
    --inputFilePattern=gs://<bucket>/datastream/<stream-name>/* \
    --inputFileFormat=json \
    --streamName=<stream-name> \
    --projectId=<your-project-id> \
    --databaseName=(default) \
    --shadowCollectionPrefix=shadow_ \
    --batchSize=500" \
  -pl v2/datastream-mongodb-to-mongodb
```

#### Additional Configuration for Local Testing

For local testing, you might want to add these parameters to reduce resource usage and speed up testing:

```
--directoryWatchDurationInMinutes=1 \
--fileReadConcurrency=2 \
```

#### Using Local Test Data

If you want to test with local data instead of GCS:

1. Create a local directory with test data files
2. Use a file:// URL in the inputFilePattern parameter
3. Make sure the files match the expected format (JSON or Avro)

#### Important Notes

1. The Direct Runner executes the entire pipeline locally, which may not be suitable for large datasets
2. Authentication will use your local credentials
3. You'll still need access to the destination database specified in the parameters
4. For streaming pipelines, the Direct Runner has limitations compared to the Dataflow service
