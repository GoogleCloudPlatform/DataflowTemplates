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
PIPELINE_NAME=text-io-to-bigquery
PIPELINE_FOLDER=TextIOToBigQuery

# Execute the pipeline
mvn compile exec:java \
    -Dexec.mainClass=com.google.cloud.teleport.templates.TextIOToBigQuery \
    -Dexec.cleanupDaemonThreads=false \
    -Dexec.args=" \
        --project=${PROJECT_ID} \
        --runner=DataflowRunner \
        --stagingLocation=gs://${PROJECT_ID}/dataflow/pipelines/${PIPELINE_FOLDER}/staging \
        --tempLocation=gs://${PROJECT_ID}/dataflow/pipelines/${PIPELINE_FOLDER}/temp \
        --templateLocation=gs://${PROJECT_ID}/dataflow/pipelines/${PIPELINE_FOLDER}/template"
