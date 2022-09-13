#  Integration Test: Big Query and Actions only

## Requirements
* Java 11
* Maven
* Text file exists

## Running Flex Template

Run the Apache Beam pipeline locally for development.

* Set environment variables that will be used in the build process.
 ```sh
 export JAVA_HOME=`/usr/libexec/java_home -v 8`
 export PROJECT=neo4j-se-team-201905
 export GS_WORKING_DIR=gs://neo4j-se-temp/dataflow-working
 export APP_NAME=gcpToNeo4j
 export JOB_NAME="test-bq-actions-`date +%Y%m%d-%H%M%S`"
 export REGION=us-central1
 export MACHINE_TYPE=n2-highmem-8
 ```
* Running this below from the /v2/googlecloud-to-neo4j directory
 ```sh
 mvn compile exec:java \
   -Dexec.mainClass=com.google.cloud.teleport.v2.neo4j.templates.GoogleCloudToNeo4j \
   -Dexec.args="\
     --usePublicIps=true \
     --stagingLocation=$GS_WORKING_DIR/staging/ \
     --tempLocation=$GS_WORKING_DIR/temp/ \
     --jobName=$JOB_NAME \
     --appName=$APP_NAME \
     --region=$REGION \
     --workerMachineType=$MACHINE_TYPE \
     --maxNumWorkers=2 \
     --jobSpecUri=gs://neo4j-se-dataflow/job-specs/testing/bigquery/bq-northwind-actions-jobspec.json \
     --neo4jConnectionUri=gs://neo4j-se-dataflow/job-specs/testing/connection/auradb-free-connection.json"
 ```

