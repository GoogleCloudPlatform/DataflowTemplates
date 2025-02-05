# Define variables
PROJECT_ID="data-platform-441421"
REGION="us-central1"
TEMPLATE_FILE="gs://dataflow-work-bucket/images/image-spec.json"
NETWORK="global/networks/data-network"
SUBNETWORK="regions/us-central1/subnetworks/data-subnet"

# Parameters
INPUT_FILE_PATTERN="gs://datastream-tumor-board-bronze/"
GCS_PUBSUB_SUBSCRIPTION="projects/$PROJECT_ID/subscriptions/datastream-tumor-board-bronze-sub"
OUTPUT_STAGING_DATASET_TEMPLATE="log_pai_dev"
OUTPUT_DATASET_TEMPLATE="bronze_pai_dev"
INPUT_FILE_FORMAT="avro"
DEAD_LETTER_QUEUE_DIRECTORY="empty"

# Run the Dataflow Flex Template job
gcloud dataflow flex-template run datastream-to-bigquery-jdk11-4 \
  --project=$PROJECT_ID \
  --region=$REGION \
  --enable-streaming-engine \
  --template-file-gcs-location=$TEMPLATE_FILE \
  --network=$NETWORK \
  --subnetwork=$SUBNETWORK \
  --parameters \
inputFilePattern=$INPUT_FILE_PATTERN,\
gcsPubSubSubscription=$GCS_PUBSUB_SUBSCRIPTION,\
outputStagingDatasetTemplate=$OUTPUT_STAGING_DATASET_TEMPLATE,\
outputDatasetTemplate=$OUTPUT_DATASET_TEMPLATE,\
inputFileFormat=$INPUT_FILE_FORMAT,\
deadLetterQueueDirectory=$DEAD_LETTER_QUEUE_DIRECTORY