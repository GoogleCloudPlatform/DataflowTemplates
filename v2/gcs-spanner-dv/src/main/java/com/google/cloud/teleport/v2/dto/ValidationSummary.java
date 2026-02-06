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
