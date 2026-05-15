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

import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.mysql.MysqlDialectAdapter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.sql.PreparedStatement;
import org.apache.beam.sdk.io.jdbc.JdbcIO.PreparedStatementSetter;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implement {@link PreparedStatementSetter} to set {@link PreparedStatement} parameters for getting
 * a count of, and reading a {@link Range}.
 */
public class RangePreparedStatementSetter implements PreparedStatementSetter<Range> {

  private final ImmutableMap<TableIdentifier, Long> numColumnsMap;
  private final ImmutableMap<TableIdentifier, ImmutableList<PartitionColumn>> partitionColumnsMap;
  private static final Logger logger = LoggerFactory.getLogger(PreparedStatementSetter.class);

  public RangePreparedStatementSetter(
      ImmutableList<TableSplitSpecification> tableSplitSpecifications) {
    this.numColumnsMap =
        tableSplitSpecifications.stream()
            .collect(
                ImmutableMap.toImmutableMap(
                    specification -> specification.tableIdentifier(), // Key Mapper
                    specification -> (long) specification.partitionColumns().size() // Value Mapper
                    ));
    this.partitionColumnsMap =
        tableSplitSpecifications.stream()
            .collect(
                ImmutableMap.toImmutableMap(
                    specification -> specification.tableIdentifier(),
                    specification -> specification.partitionColumns()));
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
   * Set range parameters for the {@link PreparedStatement}.
   *
   * @param element Range for which parameters are set.
   * @param preparedStatement the prepared statement.
   * @param startParameterIdx initial parameter index in case of chaining.
   * @throws Exception coming form jdbc. Since this is in run-time, beam will retry the exception.
   * @see MysqlDialectAdapter#getReadQuery(String, ImmutableList)
   * @see MysqlDialectAdapter#getCountQuery(String, ImmutableList, long)
   */
  private void setRangeParameters(
      Range element,
      @UnknownKeyFor @NonNull @Initialized PreparedStatement preparedStatement,
      int startParameterIdx)
      throws @UnknownKeyFor @NonNull @Initialized Exception {

    long rangeColumns = element.height() + 1;
    if (!numColumnsMap.containsKey(element.tableIdentifier())) {
      logger.error(
          "Got Range {} for unknown tableIdentifier. Known Identifiers are {} and {}",
          element,
          numColumnsMap);
      throw new RuntimeException("Invalid Range");
    }
    long numColumns = numColumnsMap.get(element.tableIdentifier());
    ImmutableList<PartitionColumn> partitionColumns =
        partitionColumnsMap.get(element.tableIdentifier());
    long extraColumns = numColumns - rangeColumns;
    Range range = element;
    for (long i = 0; i < rangeColumns; i++) {
      PartitionColumn pc = partitionColumns.get((int) i);
      Object start = range.start();
      if (start instanceof byte[] bytes && "uuid".equalsIgnoreCase(pc.columnTypeName())) {
        java.util.UUID uuid = bytesToUuid(bytes);
        logger.info(
            "[UUID Partitioning / Stage 5: Query Binding] "
                + getCallerInfo()
                + " Table: "
                + element.tableIdentifier().tableName()
                + " | Parameter Start: Converted 16-byte Hex "
                + bytesToHex(bytes)
                + " -> Native JDBC java.util.UUID: "
                + uuid);
        start = uuid;
      }
      Object end = range.end();
      if (end instanceof byte[] bytes && "uuid".equalsIgnoreCase(pc.columnTypeName())) {
        java.util.UUID uuid = bytesToUuid(bytes);
        logger.info(
            "[UUID Partitioning / Stage 5: Query Binding] "
                + getCallerInfo()
                + " Table: "
                + element.tableIdentifier().tableName()
                + " | Parameter End: Converted 16-byte Hex "
                + bytesToHex(bytes)
                + " -> Native JDBC java.util.UUID: "
                + uuid);
        end = uuid;
      }
      preparedStatement.setObject(startParameterIdx++, true /* include column */);
      preparedStatement.setObject(startParameterIdx++, start);
      preparedStatement.setObject(startParameterIdx++, end);
      preparedStatement.setObject(startParameterIdx++, range.isLast());
      preparedStatement.setObject(startParameterIdx++, end);
      range = range.childRange();
    }
    for (long i = 0; i < extraColumns; i++) {
      preparedStatement.setObject(startParameterIdx++, false /* include column */);
      preparedStatement.setObject(startParameterIdx++, null);
      preparedStatement.setObject(startParameterIdx++, null);
      preparedStatement.setObject(startParameterIdx++, false);
      preparedStatement.setObject(startParameterIdx++, null);
    }
  }

  @Override
  public void setParameters(
      Range element, @UnknownKeyFor @NonNull @Initialized PreparedStatement preparedStatement)
      throws @UnknownKeyFor @NonNull @Initialized Exception {
    com.google.common.base.Preconditions.checkNotNull(element, "Range element cannot be null");
    setRangeParameters(element, preparedStatement, 1);
  }
}
