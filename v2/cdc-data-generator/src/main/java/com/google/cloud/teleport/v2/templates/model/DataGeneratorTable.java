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
import javax.annotation.Nullable;

/** Represents a table in the data generator schema. */
@AutoValue
public abstract class DataGeneratorTable implements Serializable {

  /** The name of the table. */
  public abstract String name();

  /** The columns of the table. */
  // TODO(khajanchi): Consider using a map of columns instead of list
  public abstract ImmutableList<DataGeneratorColumn> columns();

  /** The primary key column names. */
  public abstract ImmutableList<String> primaryKeys();

  /** The name of the table this table is interleaved in, if any (Spanner specific). */
  @Nullable
  public abstract String interleavedInTable();

  /** Foreign keys defined on this table. */
  public abstract ImmutableList<DataGeneratorForeignKey> foreignKeys();

  /** Unique keys/indexes defined on this table. */
  public abstract ImmutableList<DataGeneratorUniqueKey> uniqueKeys();

  /** Whether this table is a root table (not interleaved/child). */
  public abstract boolean isRoot();

  /** The QPS for inserts. */
  public abstract int insertQps();

  /** The QPS for updates. */
  public abstract int updateQps();

  /** The QPS for deletes. */
  public abstract int deleteQps();

  /** The number of records to generate for this table for each record of the parent table. */
  public abstract double recordsPerTick();

  /** The name of the parent table that drives generation for this table (if any). */
  @Nullable
  public abstract String generatorParent();

  /** The names of the tables that are children of this table in the generation hierarchy. */
  @Nullable
  public abstract ImmutableList<String> childTables();

  public abstract Builder toBuilder();

  public static Builder builder() {
    return new AutoValue_DataGeneratorTable.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder name(String name);

    public abstract Builder columns(ImmutableList<DataGeneratorColumn> columns);

    public abstract Builder primaryKeys(ImmutableList<String> primaryKeys);

    public abstract Builder interleavedInTable(@Nullable String interleavedInTable);

    public abstract Builder foreignKeys(ImmutableList<DataGeneratorForeignKey> foreignKeys);

    public abstract Builder uniqueKeys(ImmutableList<DataGeneratorUniqueKey> uniqueKeys);

    public abstract Builder isRoot(boolean isRoot);

    public abstract Builder insertQps(int insertQps);

    public abstract Builder updateQps(int updateQps);

    public abstract Builder deleteQps(int deleteQps);

    public abstract Builder recordsPerTick(double recordsPerTick);

    public abstract Builder generatorParent(@Nullable String generatorParent);

    public abstract Builder childTables(ImmutableList<String> childTables);

    public abstract DataGeneratorTable build();
  }
}
