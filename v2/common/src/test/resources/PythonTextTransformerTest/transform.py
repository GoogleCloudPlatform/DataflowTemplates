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
import copy
import json
import sys
import traceback

def transform(event):
  """ Return a Dict or List of Dict Objects.  Return None to discard """
  event['new_key'] = 'new_value'
  # event = event
  return event

def _handle_result(input_data):
  event_id = copy.deepcopy(input_data['id'])
  event = copy.deepcopy(input_data['event'])
  try:
    transformed_event = transform(event)
    if isinstance(transformed_event, list):
      for row in transformed_event:
        payload = json.dumps({'status': 'SUCCESS',
                              'id': event_id,
                              'event': row,
                              'error_message': None})
        print(payload)
    else:
      payload = json.dumps({'status': 'SUCCESS',
                            'id': event_id,
                            'event': transformed_event,
                            'error_message': None})
      print(payload)
  except Exception as e:
    stack_trace = traceback.format_exc()
    payload = json.dumps({'status': 'FAILED',
                          'id': event_id,
                          'event': event,
                          'error_message': stack_trace})
    print(payload)

if __name__ == '__main__':
  # TODO: How do we handle the case where there are no messages
  file_name = sys.argv[1]
  data = []
  with open(file_name, "r") as data_file:
    for line in data_file:
      data.append(json.loads(line))

  if isinstance(data, list):
    for event in data:
      _handle_result(event)
  else:
    event = data
    _handle_result(event)
  exit()
