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
package com.google.cloud.teleport.v2.spanner.sourceddl;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import javax.annotation.Nullable;

/** Represents a column in a source database table. */
@AutoValue
public abstract class SourceColumn implements Serializable {

  private static final long serialVersionUID = 1L;

  public abstract String name();

  public abstract String type();

  public abstract boolean isNullable();

  public abstract boolean isPrimaryKey();

  @Nullable
  public abstract Long size();

  @Nullable
  public abstract Integer precision();

  @Nullable
  public abstract Integer scale();

  public abstract ImmutableList<String> columnOptions();

  public abstract SourceDatabaseType sourceType();

  public abstract boolean isGenerated();

  public static Builder builder(SourceDatabaseType sourceType) {
    return new AutoValue_SourceColumn.Builder()
        .sourceType(sourceType)
        .isNullable(true)
        .isPrimaryKey(false)
        .isGenerated(false)
        .columnOptions(ImmutableList.of());
  }

  public abstract Builder toBuilder();

  /** A builder for {@link SourceColumn}. */
  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder name(String name);

    public abstract Builder type(String type);

    public abstract Builder isNullable(boolean isNullable);

    public abstract Builder isPrimaryKey(boolean isPrimaryKey);

    public abstract Builder isGenerated(boolean isGenerated);

    public abstract Builder size(Long size);

    public abstract Builder precision(Integer precision);

    public abstract Builder scale(Integer scale);

    public abstract Builder columnOptions(ImmutableList<String> columnOptions);

    public abstract Builder sourceType(SourceDatabaseType sourceType);

    public abstract SourceColumn build();
  }
}
