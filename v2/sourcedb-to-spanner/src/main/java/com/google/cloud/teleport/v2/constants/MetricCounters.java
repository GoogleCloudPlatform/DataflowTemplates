/*
 * Copyright (C) 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.constants;

// Counters for bulk template.
public class MetricCounters {

  // Counter for errors in the transformer.
  public static final String TRANSFORMER_ERRORS = "transformer_errors";

  // Counter for events filtered out in the transformer based on custom transformation response.
  public static final String FILTERED_EVENTS = "filtered_events";

  // Counter for errors encountered by the reader when trying to map JDBC ResultSet to a SourceRow.
  public static final String READER_MAPPING_ERRORS = "reader_mapping_errors";

  // Counter for errors encountered by the reader while discovering schema. This counts all sorts of
  // errors including SQLTransientConnectionException, SQLNonTransientConnectionException,
  // SQLExceptions etc.
  public static final String READER_SCHEMA_DISCOVERY_ERRORS = "reader_schema_discovery_errors";

  // Counter for number of mutations that failed writing to Spanner
  public static final String FAILED_MUTATION_ERRORS = "failed_mutation_errors";

  // Counter for the number of tables completed.
  public static final String TABLES_COMPLETED = "tables_completed";
}
