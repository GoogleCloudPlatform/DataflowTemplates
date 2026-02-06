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

/** Represents a row in the MismatchedRecords table in BigQuery. */
@AutoValue
@DefaultSchema(AutoValueSchema.class)
public abstract class MismatchedRecord {

  public abstract String getRunId();

  public abstract String getTableName();

  public abstract String getMismatchType();

  public abstract String getRecordKey();

  public abstract String getSource();

  public abstract String getHash();

  @Nullable
  public abstract String getMismatchedColumns();

  public static Builder builder() {
    return new AutoValue_MismatchedRecord.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setRunId(String runId);

    public abstract Builder setTableName(String tableName);

    public abstract Builder setMismatchType(String mismatchType);

    public abstract Builder setRecordKey(String recordKey);

    public abstract Builder setSource(String source);

    public abstract Builder setHash(String hash);

    public abstract Builder setMismatchedColumns(String mismatchedColumns);

    public abstract MismatchedRecord build();
  }
}
