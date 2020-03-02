"""
Copyright 2020 Google Inc. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import json
import sys

def transform(event):
  """ Return a Dict or List of Dict Objects.  Return None to discard """
  # Ensure Table Dest Exists

  return clean_transformed_event(event)

def clean_transformed_event(event):
  # TODO: Review logic to decide if cleaning all complex data is required
  for col in event:
    if type(event[col]) in [dict, list]:
      event[col] = json.dumps(event[col])

  return json.dumps(event)

def _handle_result(result):
  if isinstance(result, list):
    for event in result:
      if event:
        print(event)
  elif result:
    print(result)

if __name__ == '__main__':
  # TODO: How do we want to handle the case where there are no messages
  file_name = sys.argv[1]
  with open(file_name, "r") as data_file:
    for cnt, raw_data in enumerate(data_file):
      raw_events = "[%s]" % raw_data

      # TODO: What if line is not valid JSON, how are errors pushed to DLQ
      data = json.loads(raw_events)

      if isinstance(data, list):
        for event in data:
          _handle_result(transform(event))
      else:
        event = data
        _handle_result(transform(event))
  exit()
