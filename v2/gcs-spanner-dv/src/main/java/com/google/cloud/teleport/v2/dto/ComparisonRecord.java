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
import java.util.List;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

@AutoValue
@DefaultSchema(AutoValueSchema.class)
public abstract class ComparisonRecord {

  public abstract String getTableName();

  public abstract List<Column> getPrimaryKeyColumns();

  public abstract String getHash();

  public static Builder builder() {
    return new AutoValue_ComparisonRecord.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setTableName(String tableName);

    public abstract Builder setPrimaryKeyColumns(List<Column> primaryKeyColumns);

    public abstract Builder setHash(String hash);

    public abstract ComparisonRecord build();
  }
}
