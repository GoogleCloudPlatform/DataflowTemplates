"""
Copyright (C) 2020 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
"""

"""
A good transform function.
@param {string} inJson
@return {string} outJson
"""
import json
import sys

def transform(event):
  """ Return a Dict or List of Dict Objects.  Return None to discard
      Input: JSON string
      Output: Python Dictionary
  """
  event['someProp'] = 'someValue'
  event = json.dumps(event)
  return event

# DO NOT EDIT BELOW THIS LINE
# The remaining code is boilerplate required for
# dynamic handling of event batches from the main
# dataflow pipeline. the transform() function should
# be the initial entrypoint for your custom logic.
def _handle_result(result):
  if isinstance(result, list):
    for event in result:
      if event:
        print(json.dumps(event))
  elif result:
    print(json.dumps(result))

if __name__ == '__main__':
  # TODO: How do we handle the case where there are no messages
  file_name = sys.argv[1]
  with open(file_name, "r") as data_file:
    for cnt, raw_data in enumerate(data_file):
      raw_events = "[%s]" % raw_data

      data = json.loads(raw_events)

      if isinstance(data, list):
        for event in data:
          _handle_result(transform(event))
      else:
        event = data
        _handle_result(transform(event))
