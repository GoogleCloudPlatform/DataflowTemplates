#!/usr/bin/env bash

export GOOGLE_APPLICATION_CREDENTIALS=~/Downloads/uplifted-plate-241522-f3141d03766a.json
# Set the pipeline vars
PROJECT_ID=uplifted-plate-241522
BUCKET_NAME=poc-bt-tmp
INPUT_TOPIC=gy-test
DATASET_ID=pocds
FAKE_OUTPUT_TABLE=gytest
DL_TABLE=gytest
BT_PROJECT_ID=uplifted-plate-241522
BT_INSTANCE_ID=poc-1
BT_TABLE_ID=bt-poc
PIPELINE_FOLDER=gs://${BUCKET_NAME}/dataflow/pipelines/pubsub-to-bigtable

USE_SUBSCRIPTION=false
# USE_SUBSCRIPTION=true or false depending on whether the pipeline should read
#                   from a Pub/Sub Subscription or a Pub/Sub Topic.

# Set the runner
RUNNER=DataflowRunner

# Build the template
mvn compile exec:java \
-Dexec.mainClass=com.google.cloud.teleport.templates.PubSubToBigTableCB \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--project=${PROJECT_ID} \
--stagingLocation=${PIPELINE_FOLDER}/staging \
--tempLocation=${PIPELINE_FOLDER}/temp \
--templateLocation=${PIPELINE_FOLDER}/template \
--runner=${RUNNER} \
--useSubscription=${USE_SUBSCRIPTION}
"

# # Execute a pipeline to read from a Subscription.
# gcloud dataflow jobs run ${JOB_NAME} \
# --gcs-location=${PIPELINE_FOLDER}/template \
# --zone=us-east1-d \
# --parameters \
# "inputSubscription=projects/${PROJECT_ID}/subscriptions/input-subscription-name,\
# outputTableSpec=${PROJECT_ID}:dataset-id.output-table,\
# outputDeadletterTable=${PROJECT_ID}:dataset-id.deadletter-table"
