#!/usr/bin/env bash
# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

echo 'Running spotless to verify that the code sticks to the Google style guide.'

FINAL_STATUS=0

# Attempts to get the changes based on a regex that filters from `git status`.
function get_change_count {
  echo $(git status | grep -e $1 | wc -l)
}

if [[ $(get_change_count '[^/]src/') -gt 0 ]]; then
  mvn spotless:check
  readonly CLASSIC_STATUS=$?
  if [[ $CLASSIC_STATUS -ne 0 ]]; then echo 'Error in Classic Templates. Run `mvn spotless:apply` from root to fix'; fi
  FINAL_STATUS=$(($FINAL_STATUS | $CLASSIC_STATUS))
else
  echo 'No changes detected in Classic Templates. Skipping spotless check.'
fi

if [[ $(get_change_count '[^/]v2/') -gt 0 ]]; then
  mvn spotless:check -f v2/pom.xml
  readonly FLEX_STATUS=$?
  if [[ $FLEX_STATUS -ne 0 ]]; then echo 'Error in Flex Templates. Run `mvn spotless:apply -f v2/pom.xml` to fix'; fi
  FINAL_STATUS=$(($FINAL_STATUS | $FLEX_STATUS))
else
  echo 'No changes detected in Flex Templates. Skipping spotless check.'
fi

echo 'Check complete.'
exit $FINAL_STATUS