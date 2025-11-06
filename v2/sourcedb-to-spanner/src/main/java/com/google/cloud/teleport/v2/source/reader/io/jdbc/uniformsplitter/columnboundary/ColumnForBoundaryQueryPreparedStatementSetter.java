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

import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.mysql.MysqlDialectAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.PartitionColumn;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.Range;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableIdentifier;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableSplitSpecification;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.sql.PreparedStatement;
import java.util.function.Function;
import org.apache.beam.sdk.io.jdbc.JdbcIO.PreparedStatementSetter;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

/**
 * Implement {@link PreparedStatementSetter} to set {@link PreparedStatement} parameters for getting
 * boundary of a column.
 */
public class ColumnForBoundaryQueryPreparedStatementSetter
    implements PreparedStatementSetter<ColumnForBoundaryQuery> {

  private final ImmutableMap<TableIdentifier, TableSplitSpecification> tableSplitSpecificationMap;

  /**
   * Construct {@link ColumnForBoundaryQueryPreparedStatementSetter}.
   *
   * @param tableSplitSpecifications list of table split specifications.
   */
  public ColumnForBoundaryQueryPreparedStatementSetter(
      ImmutableList<TableSplitSpecification> tableSplitSpecifications) {
    this.tableSplitSpecificationMap =
        tableSplitSpecifications.stream()
            .collect(
                ImmutableMap.toImmutableMap(
                    TableSplitSpecification::tableIdentifier, Function.identity()));
  }

  /**
   * Set statement parameters.
   *
   * @param element details of the column for which boundary is to be found.
   * @param preparedStatement the prepared statement.
   * @throws Exception coming form jdbc. Since this is in run-time, beam will retry the exception.
   * @see MysqlDialectAdapter#getBoundaryQuery(String, ImmutableList, String)
   */
  @Override
  public void setParameters(
      ColumnForBoundaryQuery element,
      @UnknownKeyFor @NonNull @Initialized PreparedStatement preparedStatement)
      throws @UnknownKeyFor @NonNull @Initialized Exception {
    ImmutableMap.Builder<String, Range> parentRangeMap = ImmutableMap.builder();
    Range currentRange = element.parentRange();
    while (currentRange != null) {
      parentRangeMap.put(currentRange.colName(), currentRange);
      currentRange = currentRange.childRange();
    }
    ImmutableMap<String, Range> parentRanges = parentRangeMap.build();

    int parameterIdx = 1;
    for (PartitionColumn partitionColumn :
        tableSplitSpecificationMap.get(element.tableIdentifier()).partitionColumns()) {

      if (partitionColumn.columnName().equals(element.columnName())) {
        // For the target column, we want to scan all values, so we disable the predicate.
        preparedStatement.setObject(parameterIdx++, false); // Disabled
        preparedStatement.setObject(parameterIdx++, null);
        preparedStatement.setObject(parameterIdx++, null);
        preparedStatement.setObject(parameterIdx++, false);
        preparedStatement.setObject(parameterIdx++, null);
      } else if (parentRanges.containsKey(partitionColumn.columnName())) {
        Range rangeForColumn = parentRanges.get(partitionColumn.columnName());
        preparedStatement.setObject(parameterIdx++, true); // Enabled
        preparedStatement.setObject(parameterIdx++, rangeForColumn.start());
        preparedStatement.setObject(parameterIdx++, rangeForColumn.end());
        preparedStatement.setObject(parameterIdx++, rangeForColumn.isLast());
        preparedStatement.setObject(parameterIdx++, rangeForColumn.end());
      } else {
        // For other partition columns outside the parent path, they are effectively disabled.
        preparedStatement.setObject(parameterIdx++, false); // Disabled
        preparedStatement.setObject(parameterIdx++, null);
        preparedStatement.setObject(parameterIdx++, null);
        preparedStatement.setObject(parameterIdx++, false);
        preparedStatement.setObject(parameterIdx++, null);
      }
    }
  }
}
