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
 * Sample UDF function to add fields to a Cloud Logging log entry
 * received from Pub/Sub. Use this to add more contextual information.
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

  // Example 1: Add new field to track originating Pub/Sub subscription
  data.inputSubscription = "audit_logs_subscription";

  // Example 2: Add new field to track app ID based on caller using a lookup fn
  if (data.protoPayload &&
    data.protoPayload.authenticationInfo &&
    data.protoPayload.authenticationInfo.principalEmail) {
    const caller = data.protoPayload.authenticationInfo.principalEmail;
    data.callingAppId = callerToAppIdLookup(caller);
  }

  return JSON.stringify(obj);
}

/**
 * Sample lookup function to map event's caller with corresponding user
 * application IDs
 * @param {string} caller
 * @return {string} appId
 */
function callerToAppIdLookup(caller) {
  let appId;
  switch(caller) {
    case "SA_10001@PROJECT_ID.iam.gserviceaccount.com":
      appId = "ERP";
      break;
    case "SA_10002@PROJECT_ID.iam.gserviceaccount.com":
      appId = "MobileApp";
      break;
    case "service-org-ORG_ID@security-center-api.iam.gserviceaccount.com":
      appId = "OrgSCC";
      break;
    default:
      appId = "Other";
      break;
  }
  return appId;
}