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
PIPELINE_CLASS=PubsubToAvro
PIPELINE_FOLDER=gs://${PROJECT_ID}/dataflow/pipelines/pubsub-to-avro

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
     --windowDuration=1m"

# Execute the template
JOB_NAME=pubsub-to-avro-$USER-$RANDOM

gcloud dataflow jobs run ${JOB_NAME} \
    --gcs-location=${PIPELINE_FOLDER}/template \
    --zone=us-east1-d \
    --parameters \
"inputTopic=projects/${PROJECT_ID}/topics/windowed-files,\
outputDirectory=gs://${PROJECT_ID}/temp/YYYY-MM-DD/HH/,\
outputFilenamePrefix=windowed-file-,\
outputFilenameSuffix=.avro"