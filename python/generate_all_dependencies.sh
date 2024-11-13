# Copyright 2024 Google Inc. All Rights Reserved.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

SCRIPTPATH=$(dirname "$0")

sh $SCRIPTPATH/generate_dependencies.sh $SCRIPTPATH/../python/src/main/python/streaming-llm/base_requirements.txt $SCRIPTPATH/../python/src/main/python/streaming-llm/requirements.txt
# Generate a base set of dependencies to use for any templates without special dependencies
mkdir -p $SCRIPTPATH/__build__/
sh $SCRIPTPATH/generate_dependencies.sh $SCRIPTPATH/default_base_python_requirements.txt $SCRIPTPATH/__build__/default_python_requirements.txt
sh $SCRIPTPATH/generate_dependencies.sh $SCRIPTPATH/default_base_yaml_requirements.txt $SCRIPTPATH/__build__/default_yaml_requirements.txt

cp $SCRIPTPATH/__build__/default_python_requirements.txt $SCRIPTPATH/../python/src/main/python/word-count-python/requirements.txt

cp $SCRIPTPATH/__build__/default_yaml_requirements.txt $SCRIPTPATH/../python/src/main/python/yaml-template/requirements.txt
cp $SCRIPTPATH/__build__/default_yaml_requirements.txt $SCRIPTPATH/../python/src/main/python/job-builder-server/requirements.txt
cp $SCRIPTPATH/__build__/default_yaml_requirements.txt $SCRIPTPATH/../v2/googlecloud-to-elasticsearch/src/main/resources/requirements.txt
cp $SCRIPTPATH/__build__/default_yaml_requirements.txt $SCRIPTPATH/../v2/googlecloud-to-googlecloud/src/main/resources/requirements.txt
cp $SCRIPTPATH/__build__/default_yaml_requirements.txt $SCRIPTPATH/../v2/googlecloud-to-splunk/src/main/resources/requirements.txt
cp $SCRIPTPATH/__build__/default_yaml_requirements.txt $SCRIPTPATH/../v2/pubsub-binary-to-bigquery/src/main/resources/requirements.txt
cp $SCRIPTPATH/__build__/default_yaml_requirements.txt $SCRIPTPATH/../v2/pubsub-to-mongodb/src/main/resources/requirements.txt
