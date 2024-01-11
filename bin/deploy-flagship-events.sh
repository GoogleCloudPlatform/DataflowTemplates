#!/usr/bin/env bash
#
# deploy-flagship-events.sh is used to deploy the flagship-events pipeline to a given environment

SCRIPT_ROOT=$(dirname "${BASH_SOURCE[0]}")/..

usage() {
  echo 1>&2 "usage: $(basename $0) [options]"
  echo 1>&2 ""
  echo 1>&2 "    --help    display usage"
  echo 1>&2 "    --env     environment to work with, default is intg"
  echo 1>&2 "    --verify  verify local config matches cluster, do not apply any changes"
  exit 1
}

main() {
  if [[ "${DEBUG}" == "1" ]]; then
    echo "debuging enabled"
    set -x
  fi

  set -o errexit
  set -o pipefail
  trap 'echo "error on line ${LINENO}"' ERR

  ENVIRONMENT=intg

  ARGUMENTS=()
  while (( $# > 0 )); do
    key=$1
    case $key in
      --help| -h )
        usage
        ;;
      --env )
        ENVIRONMENT=$2
        shift
        shift
        ;;
      --verify )
        VERIFY=true
        shift
        ;;
      * )
        ARGUMENTS+=("$1")
        shift
        ;;
    esac
  done
  set -- "${ARGUMENTS[@]}"

  if ! [[ "${ENVIRONMENT}" =~ ^(intg|stge|prod)$ ]]; then
    echo "error: unknown environment '${ENVIRONMENT}'"
    exit 1
  fi

  gcloud config set project "is-events-dataflow-${ENVIRONMENT}"

  mvn clean package -pl v2/flagship-events -am

  gcloud dataflow flex-template build "gs://is-events-dataflow-${ENVIRONMENT}/templates/flagship-events.json" \
    --image-gcr-path "us-west1-docker.pkg.dev/is-events-dataflow-${ENVIRONMENT}/default/dataflow/flagship-events:latest" \
    --sdk-language "JAVA" \
    --flex-template-base-image JAVA11 \
    --metadata-file "v2/flagship-events/metadata.json" \
    --jar "v2/flagship-events/target/flagship-events-1.0-SNAPSHOT.jar" \
    --env FLEX_TEMPLATE_JAVA_MAIN_CLASS="com.keap.dataflow.flagshipevents.FlagshipEventsPubsubToBigQuery"

  mvn clean package -PtemplatesStage  \
    -DskipTests \
    -DprojectId="is-events-dataflow-${ENVIRONMENT}" \
    -DbucketName="templates" \
    -DstagePrefix="templates" \
    -DtemplateName="flagship-events" \
    -pl v2/flagship-events \
    -am

  gcloud dataflow flex-template run "flagshipevents-`date +%Y%m%d-%H%M%S`" \
    --template-file-gcs-location "gs://is-events-dataflow-${ENVIRONMENT}/templates/flagship-events.json" \
    --parameters env="${ENVIRONMENT}"

}

main "$@"
