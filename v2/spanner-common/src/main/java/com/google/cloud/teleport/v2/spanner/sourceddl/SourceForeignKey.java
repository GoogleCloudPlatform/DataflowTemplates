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
package com.google.cloud.teleport.v2.spanner.sourceddl;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import java.io.Serializable;

/** Represents a foreign key in a source database. */
@AutoValue
public abstract class SourceForeignKey implements Serializable {

  public abstract String name();

  public abstract String referencedTable();

  public abstract ImmutableList<String> keyColumns();

  public abstract ImmutableList<String> referencedColumns();

  public abstract String tableName();

  public static Builder builder() {
    return new AutoValue_SourceForeignKey.Builder()
        .keyColumns(ImmutableList.of())
        .referencedColumns(ImmutableList.of());
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder name(String name);

    public abstract Builder tableName(String tableName);

    public abstract Builder referencedTable(String referencedTable);

    public abstract Builder keyColumns(ImmutableList<String> keyColumns);

    public abstract Builder referencedColumns(ImmutableList<String> referencedColumns);

    public abstract SourceForeignKey build();
  }
}
