PROJECT=data-platform-441421
IMAGE_NAME=datastream-to-bigquery
BUCKET_NAME=gs://dataflow-work-bucket
TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java11-template-launcher-base
BASE_CONTAINER_IMAGE_VERSION=latest
APP_ROOT=/template/${IMAGE_NAME}
DATAFLOW_JAVA_COMMAND_SPEC=${APP_ROOT}/resources/${IMAGE_NAME}-command-spec.json
TEMPLATE_IMAGE_SPEC=${BUCKET_NAME}/images/${IMAGE_NAME}-image-spec.json

#TOPIC=projects/${PROJECT}/topics/test-topic
#SUBSCRIPTION=projects/${PROJECT}/subscriptions/test-topic-sub
#DEADLETTER_TABLE=${PROJECT}:${DATASET_TEMPLATE}.dead_letter

mvn clean package \
-Dimage=${TARGET_GCR_IMAGE} \
-Dbase-container-image=${BASE_CONTAINER_IMAGE} \
-Dbase-container-image.version=${BASE_CONTAINER_IMAGE_VERSION} \
-Dapp-root=${APP_ROOT} \
-Dcommand-spec=${DATAFLOW_JAVA_COMMAND_SPEC} \
-am -pl ${IMAGE_NAME}
