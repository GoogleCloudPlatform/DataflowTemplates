while getopts e:s:r:z:m: flag
do
    case "${flag}" in
        e) ENTITY_NAME=${OPTARG};;
        s) SUBSCRIPTION_NAME=${OPTARG};;
        r) REGION=${OPTARG};;
        z) ZONE=${OPTARG};;
        m) WORKER_MACHINE_TYPE=${OPTARG};;
    esac
done

export JOB_NAME=ope_metrics
export DATASET_ID=ope_metrics
export MAX_WORKERS=1
export PROJECT_ID=yin-yang-332008
export BUCKET_NAME=hpi-dataflow-temp-bucket
export PIPELINE_FOLDER=gs://${BUCKET_NAME}/dataflow/pipelines/hpi-pubsub-to-bigquery
export SERVICE_ACCOUNT_EMAIL=simple-data-stream@yin-yang-332008.iam.gserviceaccount.com
export USE_SUBSCRIPTION=true
export RUNNER=DataflowRunner

# Build the template
mvn compile exec:java \
-Dexec.mainClass=com.google.cloud.teleport.templates.PubSubToBigQueryHPI \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--project=${PROJECT_ID} \
--stagingLocation=${PIPELINE_FOLDER}/staging \
--tempLocation=${PIPELINE_FOLDER}/temp \
--templateLocation=${PIPELINE_FOLDER}/template \
--runner=${RUNNER} \
--useSubscription=${USE_SUBSCRIPTION} \
--region=${REGION} \
--tableNameAttr=bq_table \
--outputDeadletterTable=${PROJECT_ID}:${DATASET_ID}.error_records \
--outputTableProject=${PROJECT_ID} \
--outputTableDataset=${DATASET_ID} \
"

# Execute the template
export JOB_NAME=ps-to-bq-$JOB_NAME-`date +"%Y%m%d-%H%M%S%z"`
 
# Execute a pipeline to read from a Subscription.
gcloud dataflow jobs run ${JOB_NAME} \
--enable-streaming-engine \
--gcs-location=${PIPELINE_FOLDER}/template \
--region=${REGION} \
--worker-zone=${ZONE} \
--worker-machine-type=${WORKER_MACHINE_TYPE} \
--num-workers=1 \
--max-workers=${MAX_WORKERS} \
--service-account-email=${SERVICE_ACCOUNT_EMAIL} \
--parameters \
"inputSubscription=projects/${PROJECT_ID}/subscriptions/${SUBSCRIPTION_NAME}"