#
# Copyright (C) 2019 Google Inc.
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

FROM gcr.io/dataflow-templates-base/python2-template-launcher-base

RUN mkdir -p /template/wordcount

COPY wordcount.py /template/wordcount

COPY spec/python_command_spec.json /template/wordcount

ENV DATAFLOW_PYTHON_COMMAND_SPEC /template/wordcount/python_command_spec.json

RUN pip install avro==1.8.2 pyarrow==0.11.1 apache-beam[gcp]==2.16.0
