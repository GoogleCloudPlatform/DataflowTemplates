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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.columnboundary;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.UniformSplitterDBAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.PartitionColumn;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.Range;
import java.io.Serializable;
import javax.annotation.Nullable;

/**
 * Intermediate data structure generated during PTransforms to indicate a column for which a
 * boundary (min, max) is requested.
 */
@AutoValue
public abstract class ColumnForBoundaryQuery implements Serializable {

  /**
   * @return column details.
   */
  public abstract PartitionColumn partitionColumn();

  /**
   * Parent range. There are two kinds of boundary queries that the splitting logic needs to make:
   *
   * <ol>
   *   <li>{@code SELECT MIN(firstCol), MAX(firstCol) from table} for the first column.
   *   <li>{@code SELECT MIN(col), MAX(col) from table WHERE ...} for subsequent columns.
   * </ol>
   *
   * <p>For the first column, set the parentRange as null. For subsequent columns setting the parent
   * range helps the {@link UniformSplitterDBAdapter} and {@link
   * ColumnForBoundaryQueryPreparedStatementSetter} generate a correct query and parameters.
   *
   * @return Parent Range. Null indicates first column for splitting. Defaults to Null.
   */
  @Nullable
  public abstract Range parentRange();

  public static Builder builder() {
    return new AutoValue_ColumnForBoundaryQuery.Builder().setParentRange(null);
  }

  /**
   * @return Name of the column for which a boundary query is needed.
   */
  public String columnName() {
    return partitionColumn().columnName();
  }

  /**
   * @return Class of the column for which a boundary query is needed.
   */
  public Class columnClass() {
    return partitionColumn().columnClass();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setPartitionColumn(PartitionColumn value);

    public abstract PartitionColumn.Builder partitionColumnBuilder();

    public Builder setColumnName(String value) {
      this.partitionColumnBuilder().setColumnName(value);
      return this;
    }

    public Builder setColumnClass(Class value) {
      this.partitionColumnBuilder().setColumnClass(value);
      return this;
    }

    public abstract Builder setParentRange(Range value);

    public abstract ColumnForBoundaryQuery build();
  }
}
