#!/bin/bash

#
# Copyright 2017 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Change directory to the location of the pom.xml
cd "$(dirname ${BASH_SOURCE[0]})"/..

# Set the pipeline vars
PROJECT_ID=data-analytics-pocs
PIPELINE_CLASS=Decompressor
PIPELINE_FOLDER=gs://${PROJECT_ID}/dataflow/pipelines/decompressor

# Set the runner
RUNNER=DataflowRunner

# Build the template
mvn compile exec:java \
     -Dexec.mainClass=com.google.cloud.teleport.templates.${PIPELINE_CLASS} \
     -Dexec.cleanupDaemonThreads=false \
     -Dexec.args=" \
     --project=${PROJECT_ID} \
     --stagingLocation=${PIPELINE_FOLDER}/staging \
     --tempLocation=${PIPELINE_FOLDER}/temp \
     --templateLocation=${PIPELINE_FOLDER}/template \
     --runner=${RUNNER} \
     --numWorkers=25"

# Execute the template
JOB_NAME=decompressor-$USER-$RANDOM

gcloud dataflow jobs run ${JOB_NAME} \
    --gcs-location=${PIPELINE_FOLDER}/template \
    --zone=us-east1-d \
    --parameters inputFilePattern=gs://${PROJECT_ID}/dataflow/testing/decompressor/compressed/*,outputDirectory=gs://${PROJECT_ID}/dataflow/testing/decompressor/decompressed,outputFailureFile=gs://${PROJECT_ID}/dataflow/testing/decompressor/decompressed/failed-${JOB_NAME}.csv