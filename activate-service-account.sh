export SERVICE_ACCOUNT_EMAIL=meagar-test-779@cloud-bigtable-dev.google.com.iam.gserviceaccount.com
export SERVICE_ACCOUNT_KEY_FILE=/usr/local/google/home/meagar/src/github.com/meagar/DataflowTemplates/key.json

gcloud auth activate-service-account $SERVICE_ACCOUNT_EMAIL --key-file=$SERVICE_ACCOUNT_KEY_FILE

export PROJECT=google.com:cloud-bigtable-dev
export INSTANCE=meagar-test
export TABLE=meagar-test

