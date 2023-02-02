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
 * Sample UDF function to filter out some incoming events. Use this to drop
 * unneeded or sensitive events.
 * @param {string} inJson input JSON (stringified)
 * @return {string} outJson output JSON (stringified)
 */
 function process(inJson) {
  const data = JSON.parse(inJson);

  // Drop events with certain field values
  if (data.severity == "DEBUG") {
    return null;
  }

  return JSON.stringify(data);
}