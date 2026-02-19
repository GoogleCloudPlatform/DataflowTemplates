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

/** Represents a row in the TableValidationStats table in BigQuery. */
@AutoValue
@DefaultSchema(AutoValueSchema.class)
public abstract class TableValidationStats {
  public static final String RUN_ID_COLUMN_NAME = "run_id";
  public static final String SCHEMA_NAME = "schema_name";
  public static final String TABLE_NAME_COLUMN_NAME = "table_name";
  public static final String STATUS_COLUMN_NAME = "status";
  public static final String SOURCE_ROW_COUNT_COLUMN_NAME = "source_row_count";
  public static final String DESTINATION_ROW_COUNT_COLUMN_NAME = "destination_row_count";
  public static final String MATCHED_ROW_COUNT_COLUMN_NAME = "matched_row_count";
  public static final String MISMATCH_ROW_COUNT_COLUMN_NAME = "mismatch_row_count";
  public static final String START_TIMESTAMP_COLUMN_NAME = "start_timestamp";
  public static final String END_TIMESTAMP_COLUMN_NAME = "end_timestamp";

  public abstract String getRunId();

  @Nullable
  public abstract String getSchemaName();

  public abstract String getTableName();

  public abstract String getStatus();

  public abstract Long getSourceRowCount();

  public abstract Long getDestinationRowCount();

  public abstract Long getMatchedRowCount();

  public abstract Long getMismatchRowCount();

  public abstract Instant getStartTimestamp();

  public abstract Instant getEndTimestamp();

  public static Builder builder() {
    return new AutoValue_TableValidationStats.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setRunId(String runId);

    public abstract Builder setSchemaName(String schemaName);

    public abstract Builder setTableName(String tableName);

    public abstract Builder setStatus(String status);

    public abstract Builder setSourceRowCount(Long sourceRowCount);

    public abstract Builder setDestinationRowCount(Long destinationRowCount);

    public abstract Builder setMatchedRowCount(Long matchedRowCount);

    public abstract Builder setMismatchRowCount(Long mismatchRowCount);

    public abstract Builder setStartTimestamp(Instant startTimestamp);

    public abstract Builder setEndTimestamp(Instant endTimestamp);

    public abstract TableValidationStats build();
  }
}
