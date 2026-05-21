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
package com.google.cloud.teleport.v2.templates.model;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import java.io.Serializable;

/** Represents a foreign key in the data generator schema. */
@AutoValue
public abstract class DataGeneratorForeignKey implements Serializable {

  /** The name of the foreign key constraint. */
  public abstract String name();

  /** The table this foreign key references. */
  public abstract String referencedTable();

  /** The columns in this table that make up the foreign key. */
  public abstract ImmutableList<String> keyColumns();

  /** The columns in the referenced table that are referenced. */
  public abstract ImmutableList<String> referencedColumns();

  public static Builder builder() {
    return new AutoValue_DataGeneratorForeignKey.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder name(String name);

    public abstract Builder referencedTable(String referencedTable);

    public abstract Builder keyColumns(ImmutableList<String> keyColumns);

    public abstract Builder referencedColumns(ImmutableList<String> referencedColumns);

    public abstract DataGeneratorForeignKey build();
  }
}
