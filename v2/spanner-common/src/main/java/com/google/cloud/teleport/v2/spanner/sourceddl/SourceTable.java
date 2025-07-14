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

/** Represents a table in a source database. */
@AutoValue
public abstract class SourceTable implements Serializable {

  private static final long serialVersionUID = 1L;

  public abstract String name();

  @Nullable
  public abstract String schema();

  public abstract ImmutableList<SourceColumn> columns();

  public abstract ImmutableList<String> primaryKeyColumns();

  public abstract SourceDatabaseType sourceType();

  @Nullable
  public abstract String comment();

  public static Builder builder(SourceDatabaseType sourceType) {
    return new AutoValue_SourceTable.Builder()
        .sourceType(sourceType)
        .columns(ImmutableList.of())
        .primaryKeyColumns(ImmutableList.of());
  }

  public abstract Builder toBuilder();

  /** A builder for {@link SourceTable}. */
  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder name(String name);

    public abstract Builder schema(String schema);

    public abstract Builder columns(ImmutableList<SourceColumn> columns);

    public abstract Builder primaryKeyColumns(ImmutableList<String> primaryKeyColumns);

    public abstract Builder sourceType(SourceDatabaseType sourceType);

    public abstract Builder comment(String comment);

    public abstract SourceTable build();
  }

  public SourceColumn column(String name) {
    for (SourceColumn c : columns()) {
      if (c.name().equals(name)) {
        return c;
      }
    }
    return null;
  }
}
