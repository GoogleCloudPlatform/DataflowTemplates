#!/bin/bash
#
# Copyright (C) 2018 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#

# Change directory to the location of the pom.xml
cd "$(dirname ${BASH_SOURCE[0]})"/..

# Variables
PROJECT_ID=data-analytics-pocs
PIPELINE_NAME=text-io-to-bigquery-local
PIPELINE_FOLDER=TextIOToBigQuery

# Execute the pipeline
mvn compile exec:java \
    -Dexec.mainClass=com.google.cloud.teleport.templates.TextIOToBigQuery \
    -Dexec.cleanupDaemonThreads=false \
    -Dexec.args=" \
        --project=${PROJECT_ID} \
        --runner=DirectRunner \
        --stagingLocation=gs://${PROJECT_ID}/dataflow/pipelines/${PIPELINE_FOLDER}/staging \
        --tempLocation=gs://${PROJECT_ID}/dataflow/pipelines/${PIPELINE_FOLDER}/temp \
        --JSONPath=gs://the-coolest-temp-dir/template/template.json \
        --javascriptTextTransformGcsPath=gs://the-coolest-temp-dir/udf/sample_UDF.js \
        --input=gs://the-coolest-temp-dir/sample_data/sample_data.txt \
        --outputTable=data-analytics-pocs:teleport.textiotobq_localschema_helper_test_v3 \
        --javascriptTextTransformFunctionName=transform"
