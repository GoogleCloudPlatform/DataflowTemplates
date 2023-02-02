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
 * Sample UDF function to transform fields of incoming events. Use this for
 * lightweight ETL like normalizing certain event fields to a given format,
 * or simple field redaction.
 *
 * @param {string} inJson input JSON (stringified)
 * @return {string} outJson output JSON (stringified)
 */
 function process(inJson) {
  const data = JSON.parse(inJson);

  // Normalize existing field values
  data.source = (data.source && data.source.toLowerCase()) || "unknown";

  // Redact existing field values
  if (data.sensitiveField) {
    data.sensitiveField = "REDACTED";
  }

  // Remove existing fields
  if (data.redundantField) {
    delete(data.redundantField);
  }

  return JSON.stringify(data);
}