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
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

@AutoValue
@DefaultSchema(AutoValueSchema.class)
public abstract class SpannerTableReadConfiguration {

  public abstract String getTableName();

  @Nullable
  public abstract List<String> getColumnsToInclude();

  @Nullable
  public abstract List<String> getColumnsToExclude();

  @Nullable
  public abstract String getCustomQuery();

  public static Builder builder() {
    return new AutoValue_SpannerTableReadConfiguration.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setTableName(String tableName);

    public abstract Builder setColumnsToInclude(List<String> columnsToInclude);

    public abstract Builder setColumnsToExclude(List<String> columnsToExclude);

    public abstract Builder setCustomQuery(String customQuery);

    public abstract SpannerTableReadConfiguration build();
  }
}
