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
 * Sample UDF function to route some incoming events to deadletter (DL) queue.
 * Use this to drop unrecognized or failed events to DL queue for further
 * inspection and to ensure data quality downstream.
 * Depending on Dataflow job implementation, DL queue could be a Pub/Sub topic,
 * a BigQuery table, or a Cloud Storage bucket.
 * Note: This assumes the Dataflow job supports and is configured with DL queue.
 * Examples of such Dataflow templates include:
 * - Pub/Sub topic/subscription to BigQuery (Bigquery table DL)
 * - Pub/Sub to Splunk (Pub/Sub topic DL)
 * - Pub/Sub to JDBC (Pub/Sub topic DL)
 * - Pub/Sub to MongoDB template (Bigquery table DL)
 * - Datastream to Cloud Spanner (Cloud Storage bucket DL)
 * - Datastream to BigQuery (Cloud Storage bucket DL)
 * - Cloud Spanner change streams to BigQuery (Cloud Storage bucket DL)
 * - Cloud Storage to BigQuery (BigQuery table DL)
 * - Apache Kafka to BigQuery (BigQuery table)
 *
 * @param {string} inJson input JSON (stringified)
 * @return {string} outJson output JSON (stringified)
 */
 function process(inJson) {
  const data = JSON.parse(inJson);

  // Example of 'Catch All' clause to route unrecognized events to DL topic
  if (! data.hasOwnProperty('severity')) {
    throw new Error("Unrecognized event. eventId='" + data.Id + "'");
  }

  return JSON.stringify(data);
}