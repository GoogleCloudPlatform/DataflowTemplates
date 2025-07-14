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

/** Constants used in Datastream templates. */
public class DatastreamConstants {

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
  public static final String EMPTY_EVENT = "";

  // Default shadow collection prefix
  public static final String DEFAULT_SHADOW_COLLECTION_PREFIX = "shadow_";

  /* Max DoFns per dataflow worker in a streaming pipeline. */
  public static final int MAX_DOFN_PER_WORKER = 500;
}
