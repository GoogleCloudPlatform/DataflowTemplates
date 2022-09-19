# Integration Test: BQ

## Requirements

* Java 11
* Maven
* Text file exists

## Running Flex Template

Run the Apache Beam pipeline using the GCP sdk.

 ```sh
export TEMPLATE_GCS_LOCATION="gs://neo4j-dataflow/flex-templates/images/googlecloud-to-neo4j-image-spec.json"
export REGION=us-central1
 
gcloud dataflow flex-template run "test-actions-only-cli-`date +%Y%m%d-%H%M%S`" \
    --template-file-gcs-location="$TEMPLATE_GCS_LOCATION" \
    --region "$REGION" \
    --parameters jobSpecUri="gs://neo4j-dataflow/job-specs/testing/other/only-actions-jobspec.json" \
    --parameters neo4jConnectionUri="gs://neo4j-dataflow/job-specs/testing/connection/auradb-free-connection.json"
 ```

REST version looks like this:

 ```sh
curl -X POST "https://dataflow.googleapis.com/v1b3/projects/neo4j-se-team-201905/locations/us-central1/flexTemplates:launch" \
-H "Content-Type: application/json" \
-H "Authorization: Bearer $(gcloud auth print-access-token)" \
-d '{
   "launch_parameter": {
      "jobName": "test-bq-rest-'$(date +%Y%m%d-%H%M%S)'",
      "parameters": {
         "jobSpecUri": "gs://neo4j-dataflow/job-specs/testing/bq-northwind-jobspec.json",
         "neo4jConnectionUri": "gs://neo4j-dataflow/job-specs/testing/connection/auradb-free-connection.json"
      },
   "containerSpecGcsPath": "gs://neo4j-dataflow/flex-templates/images/googlecloud-to-neo4j-image-spec.json"
   }
}'
 ```