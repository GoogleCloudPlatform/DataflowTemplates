/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.templates.datastream;

import com.google.common.collect.ImmutableSet;
import java.util.Set;

/** Constants used in Datastream templates. */
public final class DatastreamConstants {

  private DatastreamConstants() {
    // Utility class; prevent instantiation
  }

  // Common event metadata fields
  public static final String EVENT_SOURCE_METADATA = "_metadata_source";
  public static final String EVENT_CHANGE_TYPE_KEY = "_metadata_change_type";
  public static final String TIMESTAMP_SECONDS = "_metadata_timestamp_seconds";
  public static final String TIMESTAMP_NANOS = "_metadata_timestamp_nanos";
  public static final String RETRY_COUNT = "_metadata_retry_count";
  public static final String CHANGE_EVENT = "changeEvent";
  // DLQ related event field
  public static final String IS_DLQ_RECONSUMED = "isDlqReconsumed";

  // MongoDB specific fields
  public static final String MONGODB_DOCUMENT_ID = "_id";

  // Source metadata fields
  public static final String COLLECTION = "collection";

  // Event types
  public static final String DELETE_EVENT = "DELETE";
  public static final String UPDATE_EVENT = "UPDATE";
  public static final String READ_EVENT = "READ";
  public static final String EMPTY_EVENT = "";

  // Read method metadata
  public static final String EVENT_READ_METHOD_KEY = "_metadata_read_method";
  public static final String READ_METHOD_BACKFILL = "backfill";
  public static final String READ_METHOD_CDC = "cdc";

  // Default shadow collection prefix
  public static final String DEFAULT_SHADOW_COLLECTION_PREFIX = "shadow_";

  /* Max DoFns per dataflow worker in a streaming pipeline. */
  public static final int MAX_DOFN_PER_WORKER = 500;

  /** Metadata fields ignored during BSON document conversion. */
  public static final Set<String> MAPPER_IGNORE_FIELDS =
      ImmutableSet.of(
          "_metadata_stream",
          "_metadata_schema",
          "_metadata_table",
          "_metadata_source",
          "_metadata_ssn",
          "_metadata_rs_id",
          "_metadata_tx_id",
          "_metadata_uuid",
          "_metadata_dlq_reconsumed",
          "_metadata_error",
          "_metadata_retry_count",
          "_metadata_timestamp",
          "_metadata_read_timestamp",
          "_metadata_read_method",
          "_metadata_deleted",
          "_metadata_primary_keys",
          "_metadata_log_file",
          "_metadata_log_position",
          "_metadata_dataflow_timestamp",
          "data",
          "_metadata_timestamp_seconds",
          "_metadata_timestamp_nanos");
}
