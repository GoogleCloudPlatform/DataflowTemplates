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
 * Sample UDF function to transform an incoming CSV line into a JSON object with
 * specific schema. Use this to transform lines of CSV files into a structured
 * JSON before writing to destinations like a BigQuery table.
 * Compatible Dataflow templates:
 * - Text Files on Cloud Storage to BigQuery
 * @param {string} line input CSV line string
 * @return {string} outJson output JSON (stringified)
 */
 function process(line) {
  const values = line.split(',');

  // Create new obj and set each field according to destination's schema
  const obj = {
    location: values[0],
    name: values[1],
    age: values[2],
    color: values[3],
    coffee: values[4]
  };

  return JSON.stringify(obj);
}