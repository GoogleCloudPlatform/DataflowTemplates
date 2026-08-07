#!/bin/bash
set -e
cd /usr/local/google/home/pratick/IdeaProjects/dataflow-poc-shadow-table

echo "Building Spanner_to_SourceDb Template..."
mvn clean package -PtemplatesStage -Dmaven.test.skip=true -Denforcer.skip=true -pl v2/spanner-to-sourcedb \
  -DprojectId="span-cloud-migrations-testing" \
  -DbucketName="gs://span-cloud-migrations-testing-temp" \
  -DstagePrefix="templates-loadtest" \
  -DtemplateName="Spanner_to_SourceDb" \
  -Dspotless.check.skip=true \
  -Dcheckstyle.skip=true

echo "Launching Dataflow Job..."
gcloud dataflow flex-template run loadtest-spanner-to-sourcedb-3 \
  --template-file-gcs-location=gs://span-cloud-migrations-testing-temp/templates-loadtest/flex/Spanner_to_SourceDb \
  --region=us-central1 \
  --project=span-cloud-migrations-testing \
  --num-workers=100 \
  --max-workers=100 \
  --parameters="changeStreamName=UsersChangeStream,instanceId=pratick-load-test,databaseId=poc-source,spannerProjectId=span-cloud-migrations-testing,metadataInstance=pratick-load-test,metadataDatabase=poc-source,sourceType=mysql,sessionFilePath=gs://span-cloud-migrations-testing-temp/session.json,sourceShardsFilePath=gs://span-cloud-migrations-testing-temp/shard.json,shadowTablePrefix=shadow_,workerMachineType=n1-standard-4,deadLetterQueueDirectory=gs://span-cloud-migrations-testing-temp/dlq,maxShardConnections=10000" \
  --enable-streaming-engine \
  --additional-pipeline-options="numberOfWorkerHarnessThreads=100,autoscalingAlgorithm=NONE"

echo "Dataflow Job Submitted!"
