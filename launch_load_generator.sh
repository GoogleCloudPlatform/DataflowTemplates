#!/bin/bash
set -e
cd /usr/local/google/home/pratick/IdeaProjects/dataflow-poc-shadow-table/v2/spanner-to-sourcedb

echo "Building and launching Load Generator with 100 workers..."
mvn compile exec:java \
  -Dexec.mainClass=com.google.cloud.teleport.v2.templates.SpannerConcurrencyLoadGenerator \
  -Dexec.args="--runner=DataflowRunner \
  --project=span-cloud-migrations-testing \
  --projectId=span-cloud-migrations-testing \
  --region=us-central1 \
  --instanceId=pratick-load-test \
  --databaseId=poc-source \
  --jobName=extreme-concurrency-loadtest-102 \
  --numWorkers=100 \
  --maxNumWorkers=100" \
  -Dmaven.test.skip=true \
  -Denforcer.skip=true \
  -Dspotless.check.skip=true \
  -Dcheckstyle.skip=true
