# /bin/bash
export PROJECT=tinyclues-staging
export IMAGE_NAME=tinyclues-staging/off-cat-ind-bq
export BUCKET_NAME=gs://reaction-offer-catalog-indexation_bucket
export TARGET_GCR_IMAGE=eu.gcr.io/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java8-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export TEMPLATE_MODULE=googlecloud-to-elasticsearch
#export APP_ROOT=/root/deployments/mvn/DataflowTemplates/v2/googlecloud-to-elasticsearch
export APP_ROOT=/template/googlecloud-to-elasticsearch

export COMMAND_SPEC=${APP_ROOT}/resources/${TEMPLATE_MODULE}-command-spec.json

gcloud config set project ${PROJECT}

mvn clean package -e \
    -Dmaven.test.skip \
    -Dcheckstyle.skip \
    -Dimage=${TARGET_GCR_IMAGE} \
    -Dbase-container-image=${BASE_CONTAINER_IMAGE} \
    -Dbase-container-image.version=${BASE_CONTAINER_IMAGE_VERSION} \
    -Dapp-root=${APP_ROOT} \
    -Dcommand-spec=${COMMAND_SPEC} \
    -am -pl googlecloud-to-elasticsearch
