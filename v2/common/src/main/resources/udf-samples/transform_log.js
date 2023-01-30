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
 * Sample UDF function to transform fields of a Cloud Logging log entry
 * received from Pub/Sub.
 * For JSON schema of the wrapper Pub/Sub message, see:
 * https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage
 * For JSON schema of the payload log entry, see:
 * https://cloud.google.com/logging/docs/reference/v2/rest/v2/LogEntry
 *
 * @param {string} inJson input JSON (stringified)
 * @return {string} outJson output JSON (stringified)
 */
 function process(inJson) {
  const obj = JSON.parse(inJson);
  const includePubsubMessage = obj.data && obj.attributes;
  const data = includePubsubMessage ? obj.data : obj;

  // For syslog or application logs, return only the source raw logs
  if (data.textPayload) {
    return data.textPayload; // Return string value, and skip JSON.stringify
  }

  return JSON.stringify(obj);
}