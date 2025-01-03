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
package com.google.cloud.teleport.v2.templates.constants;

import org.apache.beam.sdk.values.TupleTag;

/** A single class to store all constants. */
public class Constants {

  // Transaction tag prefix used in the forward migration job.
  public static final String FWD_MIGRATION_TRANSACTION_TAG_PREFIX = "txBy=";

  // Filtration Mode - none
  public static final String FILTRATION_MODE_NONE = "none";

  // Filtration Mode - forward_migration
  public static final String FILTRATION_MODE_FORWARD_MIGRATION = "forward_migration";

  // Sharding Mode - single_shard
  public static final String SHARDING_MODE_SINGLE_SHARD = "single_shard";

  // Sharding Mode - multi_shard
  public static final String SHARDING_MODE_MULTI_SHARD = "multi_shard";

  // Run Mode - regular
  public static final String RUN_MODE_REGULAR = "regular";

  // Run mode - resume
  public static final String RUN_MODE_RESUME = "resume";

  // Commit timestamp column name in shadow table
  public static final String PROCESSED_COMMIT_TS_COLUMN_NAME = "processed_commit_ts";

  // Record sequence  column name in shadow table
  public static final String RECORD_SEQ_COLUMN_NAME = "record_seq";

  // The tag for events failed with non-retryable errors
  public static final TupleTag<String> PERMANENT_ERROR_TAG = new TupleTag<String>() {};

  // The Tag for retryable Failed writes
  public static final TupleTag<String> RETRYABLE_ERROR_TAG = new TupleTag<String>() {};

  // output tag for successful write
  public static final TupleTag<String> SUCCESS_TAG = new TupleTag<String>() {};

  // The Tag for skipped records
  public static final TupleTag<String> SKIPPED_TAG = new TupleTag<String>() {};

  // The Tag for records filtered via custom transformation.
  public static final TupleTag<String> FILTERED_TAG = new TupleTag<String>() {};

  // Message written to the file for skipped records
  public static final String SKIPPED_TAG_MESSAGE = "Skipped record from reverse replication";

  // Message written to the file for no shard found records
  public static final String SHARD_NOT_PRESENT_ERROR_MESSAGE = "No shard identified for the record";

  // Default parallelism for the Dataflow workers
  public static final int DEFAULT_WORKER_HARNESS_THREAD_COUNT = 500;

  // Default shard id in case of single shard migration with logical shard id unspecified
  public static final String DEFAULT_SHARD_ID = "single_shard";

  public static final String SOURCE_MYSQL = "mysql";

  public static final String SOURCE_CASSANDRA = "cassandra";

  // Message written to the file for filtered records
  public static final String FILTERED_TAG_MESSAGE =
      "Filtered record from custom transformation in reverse replication";
}
