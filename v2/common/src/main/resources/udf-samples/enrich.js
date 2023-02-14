/*
 * Copyright (C) 2023 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

/**
 * Sample UDF function to add fields to incoming events. Use this to add more
 * contextual information.
 *
 * @param {string} inJson input JSON event (stringified)
 * @return {string} outJson output JSON event (stringified)
 */
 function process(inJson) {
  const data = JSON.parse(inJson);

  // Add new field to track data source
  data.source = "pos";

  return JSON.stringify(data);
}