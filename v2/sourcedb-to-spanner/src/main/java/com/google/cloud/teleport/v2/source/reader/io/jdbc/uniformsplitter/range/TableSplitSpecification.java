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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range;

import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Defines the specification for splitting a table, including its identifier and the columns to be
 * used for partitioning.
 */
@AutoValue
public abstract class TableSplitSpecification implements Serializable {

  private static final int MAX_PARTITION_INFERENCE_SCALE_FACTOR = 20;

  public static final long SPLITTER_MAX_RELATIVE_DEVIATION = 1;

  /**
   * Returns the identifier for the table.
   *
   * @return the table identifier.
   */
  public abstract TableIdentifier tableIdentifier();

  /**
   * Returns the list of columns to be used for partitioning.
   *
   * @return the list of partition columns.
   */
  public abstract ImmutableList<PartitionColumn> partitionColumns();

  /**
   * Approximate count of rows in the table. This is used to auto infer {@link
   * com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableSplitSpecification#maxPartitionsHint()}
   * and hence the count for initial split of ranges.
   */
  public abstract Long approxRowCount();

  /**
   * Hint for Maximum number of partitions of the source key space. If not set, it is auto inferred.
   * Note that if autoAdjustMaxPartitions is set to true, the aggregated count of all ranges will
   * auto-adjust this value by replacing the approximate count in above expression.
   */
  public abstract Long maxPartitionsHint();

  /** Number of stages for the splitting process for this table. */
  public abstract Long splitStagesCount();

  /** Hint for number of initial split of ranges. */
  public abstract Long initialSplitHeight();

  /**
   * An optional, initial range to start with. This can help anywhere from unit testing, to micro
   * batching (read only columns after a timestamp), to splitting the whole split detection and read
   * of a really large table.
   */
  @Nullable
  public abstract Range initialRange();

  /**
   * Auto Inference of max partitions similar to <a
   * href=https://github.com/apache/beam/blob/b50ad0fe8fc168eaded62efb08f19cf2aea341e2/sdks/java/io/jdbc/src/main/java/org/apache/beam/sdk/io/jdbc/JdbcIO.java#L1398>JDBCIO#readWithPartitions</a>
   * We scale the square root of partitions to 20 instead of 10 as in the initial split we begin the
   * closest power of 2 on the larger side.
   *
   * @param count approximate count of rows to migrate.
   * @return inferred max partitions
   */
  public static long inferMaxPartitions(long count) {
    return Math.max(
        1, Math.round(Math.floor(Math.sqrt(count) / MAX_PARTITION_INFERENCE_SCALE_FACTOR)));
  }

  /** Gives log to the base 2 of a given value rounded up. */
  public static long logToBaseTwo(long value) {
    return value == 0L ? 0L : 64L - Long.numberOfLeadingZeros(value - 1);
  }

  /**
   * Creates a builder for {@link TableSplitSpecification}.
   *
   * @return a new builder instance.
   */
  public static Builder builder() {
    return new AutoValue_TableSplitSpecification.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setTableIdentifier(TableIdentifier value);

    abstract TableIdentifier tableIdentifier();

    public abstract Builder setPartitionColumns(ImmutableList<PartitionColumn> value);

    public abstract Builder setApproxRowCount(Long value);

    public abstract Builder setMaxPartitionsHint(Long value);

    abstract Optional<Long> maxPartitionsHint();

    public abstract Builder setSplitStagesCount(Long value);

    abstract Optional<Long> splitStagesCount();

    public abstract Builder setInitialSplitHeight(Long value);

    abstract Optional<Long> initialSplitHeight();

    public abstract Builder setInitialRange(@Nullable Range value);

    @Nullable
    abstract Range initialRange();

    abstract ImmutableList<PartitionColumn> partitionColumns();

    abstract Long approxRowCount();

    abstract TableSplitSpecification autoBuild();

    public TableSplitSpecification build() {
      long maxPartitionsHint;
      if (maxPartitionsHint().isPresent()) {
        maxPartitionsHint = maxPartitionsHint().get();
      } else {
        maxPartitionsHint = inferMaxPartitions(approxRowCount());
        setMaxPartitionsHint(maxPartitionsHint);
      }

      if (!initialSplitHeight().isPresent()) {
        setInitialSplitHeight(logToBaseTwo(maxPartitionsHint * SPLITTER_MAX_RELATIVE_DEVIATION));
      }

      if (!splitStagesCount().isPresent()) {
        setSplitStagesCount(
            logToBaseTwo(maxPartitionsHint)
                + partitionColumns().size()
                + 1 /* For initial counts */);
      }

      if (initialRange() != null) {
        Range curRange = initialRange();
        // It's impossible to build a range where child ranges have mismatched table wrt parent.
        // Therefore, we only need to verify the table identifier of the root range.
        Preconditions.checkState(
            curRange.tableIdentifier().equals(tableIdentifier()),
            "Initial range table identifier %s does not match table split specification identifier %s",
            curRange.tableIdentifier(),
            tableIdentifier());
        for (PartitionColumn col : partitionColumns()) {
          Preconditions.checkState(
              curRange.colName().equals(col.columnName()),
              "Initial range column path %s does not match partition columns %s",
              initialRange(),
              partitionColumns());
          if (curRange.hasChildRange()) {
            curRange = curRange.childRange();
          } else {
            break;
          }
        }
      }

      return autoBuild();
    }
  }
}
