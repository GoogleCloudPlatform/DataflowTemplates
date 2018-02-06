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

# Execute the pipeline
gcloud dataflow jobs run text-io-to-bigquery \
    --gcs-location gs://data-analytics-pocs/dataflow/pipelines/TextIOToBigQuery/template \
    --parameters   javascriptTextTransformFunctionName=transform,JSONPath=gs://data-analytics-pocs/dataflow/pipelines/TextIOToBigQuery/resources/template.json,javascriptTextTransformGcsPath=gs://data-analytics-pocs/dataflow/pipelines/TextIOToBigQuery/resources/sample_UDF.js,input=gs://data-analytics-pocs/dataflow/pipelines/TextIOToBigQuery/resources/sample_data.txt,outputTable=data-analytics-pocs:teleport.textiotobq_v4


