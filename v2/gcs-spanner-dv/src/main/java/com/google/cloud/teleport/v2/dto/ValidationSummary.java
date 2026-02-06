/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.dto;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.joda.time.Instant;

/** Represents a row in the ValidationSummary table in BigQuery. */
@AutoValue
@DefaultSchema(AutoValueSchema.class)
public abstract class ValidationSummary {

  public static final String RUN_ID_COLUMN_NAME = "run_id";
  public static final String SOURCE_DATABASE_COLUMN_NAME = "source_database";
  public static final String DESTINATION_DATABASE_COLUMN_NAME = "destination_database";
  public static final String STATUS_COLUMN_NAME = "status";
  public static final String TOTAL_TABLES_VALIDATED_COLUMN_NAME = "total_tables_validated";
  public static final String TABLES_WITH_MISMATCHES_COLUMN_NAME = "tables_with_mismatches";
  public static final String TOTAL_ROWS_MATCHED_COLUMN_NAME = "total_rows_matched";
  public static final String TOTAL_ROWS_MISMATCHED_COLUMN_NAME = "total_rows_mismatched";
  public static final String START_TIMESTAMP_COLUMN_NAME = "start_timestamp";
  public static final String END_TIMESTAMP_COLUMN_NAME = "end_timestamp";

  public abstract String getRunId();

  @Nullable
  public abstract String getSourceDatabase();

  @Nullable
  public abstract String getDestinationDatabase();

  public abstract String getStatus();

  public abstract Long getTotalTablesValidated();

  @Nullable
  public abstract String getTablesWithMismatches();

  public abstract Long getTotalRowsMatched();

  public abstract Long getTotalRowsMismatched();

  public abstract Instant getStartTimestamp();

  public abstract Instant getEndTimestamp();

  public static Builder builder() {
    return new AutoValue_ValidationSummary.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setRunId(String runId);

    public abstract Builder setSourceDatabase(String sourceDatabase);

    public abstract Builder setDestinationDatabase(String destinationDatabase);

    public abstract Builder setStatus(String status);

    public abstract Builder setTotalTablesValidated(Long totalTablesValidated);

    public abstract Builder setTablesWithMismatches(String tablesWithMismatches);

    public abstract Builder setTotalRowsMatched(Long totalRowsMatched);

    public abstract Builder setTotalRowsMismatched(Long totalRowsMismatched);

    public abstract Builder setStartTimestamp(Instant startTimestamp);

    public abstract Builder setEndTimestamp(Instant endTimestamp);

    public abstract ValidationSummary build();
  }
}
