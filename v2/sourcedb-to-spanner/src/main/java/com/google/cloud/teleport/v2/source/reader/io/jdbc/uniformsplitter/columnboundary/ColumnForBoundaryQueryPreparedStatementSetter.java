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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implement {@link PreparedStatementSetter} to set {@link PreparedStatement} parameters for getting
 * boundary of a column.
 */
public class ColumnForBoundaryQueryPreparedStatementSetter
    implements PreparedStatementSetter<ColumnForBoundaryQuery> {

  private static final Logger logger =
      LoggerFactory.getLogger(ColumnForBoundaryQueryPreparedStatementSetter.class);

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

  private static String getCallerInfo() {
    for (StackTraceElement element : Thread.currentThread().getStackTrace()) {
      if (element.getClassName().endsWith("Test")) {
        return "["
            + element.getClassName().substring(element.getClassName().lastIndexOf('.') + 1)
            + "."
            + element.getMethodName()
            + ":"
            + element.getLineNumber()
            + "]";
      }
    }
    return "[Worker/Pipeline]";
  }

  private static String bytesToHex(byte[] bytes) {
    if (bytes == null) {
      return "null";
    }
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) {
      sb.append(String.format("%02X", b));
    }
    return sb.toString();
  }

  private static java.util.UUID bytesToUuid(byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    java.nio.ByteBuffer bb = java.nio.ByteBuffer.wrap(bytes);
    return new java.util.UUID(bb.getLong(), bb.getLong());
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
        Object start = rangeForColumn.start();
        // Convert raw byte[] back to native java.util.UUID before setting parameter.
        // Binding a raw byte[] directly causes PostgreSQL JDBC to send BYTEA, triggering
        // SQL type mismatch errors (ERROR: operator does not exist: uuid >= bytea).
        if (start instanceof byte[] bytes
            && "uuid".equalsIgnoreCase(partitionColumn.columnTypeName())) {
          java.util.UUID uuid = bytesToUuid(bytes);
          logger.info(
              "[UUID Partitioning / Stage 5: Boundary Query] "
                  + getCallerInfo()
                  + " Table: "
                  + element.tableIdentifier().tableName()
                  + " | Parameter Start: Converted 16-byte Hex "
                  + bytesToHex(bytes)
                  + " -> Native JDBC java.util.UUID: "
                  + uuid);
          start = uuid;
        }
        Object end = rangeForColumn.end();
        if (end instanceof byte[] bytes
            && "uuid".equalsIgnoreCase(partitionColumn.columnTypeName())) {
          java.util.UUID uuid = bytesToUuid(bytes);
          logger.info(
              "[UUID Partitioning / Stage 5: Boundary Query] "
                  + getCallerInfo()
                  + " Table: "
                  + element.tableIdentifier().tableName()
                  + " | Parameter End: Converted 16-byte Hex "
                  + bytesToHex(bytes)
                  + " -> Native JDBC java.util.UUID: "
                  + uuid);
          end = uuid;
        }
        preparedStatement.setObject(parameterIdx++, true); // Enabled
        preparedStatement.setObject(parameterIdx++, start);
        preparedStatement.setObject(parameterIdx++, end);
        preparedStatement.setObject(parameterIdx++, rangeForColumn.isLast());
        preparedStatement.setObject(parameterIdx++, end);
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
