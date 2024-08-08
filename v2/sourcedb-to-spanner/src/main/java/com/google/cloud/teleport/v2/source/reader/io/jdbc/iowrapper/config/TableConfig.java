/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.PartitionColumn;
import com.google.common.collect.ImmutableList;
import javax.annotation.Nullable;

/** Table Configuration. */
@AutoValue
public abstract class TableConfig {

  /** Name of the table. */
  public abstract String tableName();

  /**
   * Max number of read partitions. If not-null uses the user supplied maxPartitions, instead of
   * auto-inference. defaults to null.
   */
  @Nullable
  public abstract Integer maxPartitions();

  /** Partition Column. As of now only a single partition column is supported */
  public abstract ImmutableList<PartitionColumn> partitionColumns();

  /** Approximate count of the rows in the table. */
  public abstract Long approxRowCount();

  public static Builder builder(String tableName) {
    return new AutoValue_TableConfig.Builder().setTableName(tableName).setMaxPartitions(null);
  }

  @AutoValue.Builder
  public abstract static class Builder {

    abstract Builder setTableName(String value);

    public abstract Builder setMaxPartitions(Integer value);

    abstract ImmutableList.Builder<PartitionColumn> partitionColumnsBuilder();

    public abstract Builder setApproxRowCount(Long value);

    public Builder withPartitionColum(PartitionColumn column) {
      this.partitionColumnsBuilder().add(column);
      return this;
    }

    abstract TableConfig autoBuild();

    public TableConfig build() {
      TableConfig tableConfig = this.autoBuild();
      return tableConfig;
    }
  }
}
