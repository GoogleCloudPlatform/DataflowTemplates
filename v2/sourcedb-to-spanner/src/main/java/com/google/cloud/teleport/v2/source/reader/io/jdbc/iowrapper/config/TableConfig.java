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
import com.google.common.base.Preconditions;
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

  /**
   * Configures the size of data read in db, per db read call. Defaults to beam's DEFAULT_FETCH_SIZE
   * of 50_000. For manually fine-tuning this, take into account the read ahead buffer pool settings
   * (innodb_read_ahead_threshold) and the worker memory.
   */
  @Nullable
  public abstract Integer maxFetchSize();

  /** Partition Column. As of now only a single partition column is supported */
  public abstract ImmutableList<String> partitionColumns();

  public static Builder builder(String tableName) {
    return new AutoValue_TableConfig.Builder()
        .setTableName(tableName)
        .setMaxPartitions(null)
        .setMaxFetchSize(null);
  }

  @AutoValue.Builder
  public abstract static class Builder {

    abstract Builder setTableName(String value);

    public abstract Builder setMaxPartitions(Integer value);

    public abstract Builder setMaxFetchSize(Integer value);

    abstract ImmutableList.Builder<String> partitionColumnsBuilder();

    public Builder withPartitionColum(String column) {
      this.partitionColumnsBuilder().add(column);
      return this;
    }

    abstract TableConfig autoBuild();

    public TableConfig build() {
      TableConfig tableConfig = this.autoBuild();
      Preconditions.checkState(
          tableConfig.partitionColumns().size() == 1,
          "A single partition column is required. Currently Partition Columns are not auto inferred and composite partition columns are not supported.");
      return tableConfig;
    }
  }
}
