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
package com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter;

import com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.range.BoundaryExtractorFactory;
import com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.range.PartitionColumn;
import com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.stringmapper.CollationOrderRow;
import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.util.List;
import javax.annotation.Nullable;

/** Helper Interface to help uniform splitter adapt to the source database. */
public interface UniformSplitterDBAdapter extends Serializable {

  /**
   * Get query for the prepared statement to read columns within a range.
   *
   * @param tableName name of the table to read
   * @param partitionColumns partition columns.
   * @return Query Statement.
   */
  String getReadQuery(String tableName, ImmutableList<String> partitionColumns);

  /**
   * Get query for the prepared statement to count a given range.
   *
   * @param tableName name of the table to read.
   * @param partitionColumns partition columns.
   * @param timeoutMillis timeout of the count query in milliseconds. Set to 0 to disable timeout.
   * @return Query Statement.
   */
  String getCountQuery(
      String tableName, ImmutableList<String> partitionColumns, long timeoutMillis);

  /**
   * Get query for the prepared statement to get min and max of a given column, optionally in the
   * context of a parent range.
   *
   * @param tableName name of the table to read.
   * @param partitionColumns partition columns.
   */
  String getBoundaryQuery(String tableName, ImmutableList<String> partitionColumns, String colName);

  /**
   * Check if a given {@link SQLException} is a timeout. The implementation needs to check for
   * dialect specific {@link SQLException#getSQLState() SqlState} and {@link
   * SQLException#getErrorCode() ErrorCode} to check if the exception indicates a server side
   * timeout. The client side timeout would be already checked for by handling {@link
   * java.sql.SQLTimeoutException}, so the implementation does not need to check for the same.
   *
   * @param exception
   * @return
   */
  boolean checkForTimeout(SQLException exception);

  /**
   * Get a query that returns order of collation. The query must return all the characters in the
   * character set with the columns listed in {@link CollationsOrderQueryColumns}.
   *
   * @param dbCharset character set used by the database for which collation ordering has to be
   *     found.
   * @param dbCollation collation set used by the database for which collation ordering has to be
   *     found.
   * @param padSpace pad space used by the database for which collation ordering has to be found.
   * @return Query to get the order of collation.
   */
  String getCollationsOrderQuery(String dbCharset, String dbCollation, boolean padSpace);

  default Duration extractBoundaryDuration(ResultSet rs, int index) throws SQLException {
    return BoundaryExtractorFactory.parseTimeStringToDuration(rs.getString(index));
  }

  default List<CollationOrderRow> processCollationResultSet(
      ResultSet rs,
      com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.stringmapper.CollationReference
          collationReference)
      throws SQLException {
    List<CollationOrderRow> list = new java.util.ArrayList<>();
    while (rs.next()) {
      String charsetChar =
          rs.getString(CollationOrderRow.CollationsOrderQueryColumns.CHARSET_CHAR_COL);
      if (charsetChar == null || charsetChar.isEmpty()) {
        continue;
      }
      list.add(CollationOrderRow.fromRS(rs));
    }
    return list;
  }

  default LocalTime extractBoundaryLocalTime(ResultSet rs, int index) throws SQLException {
    return BoundaryExtractorFactory.parseTimeStringToLocalTime(rs.getString(index));
  }

  default OffsetTime extractBoundaryOffsetTime(ResultSet rs, int index) throws SQLException {
    return BoundaryExtractorFactory.parseTimeStringToOffsetTime(rs.getString(index));
  }

  /**
   * Reads a binary boundary (the MIN or MAX of a {@code byte[]} partition column) from a boundary
   * query result. The returned bytes must sort, as unsigned big-endian bytes, the same way the
   * database sorts the column, since split points are computed on them in Java.
   *
   * <p>Override for types whose driver representation or sort order differs from raw bytes (for
   * example Postgres {@code uuid} or SQL Server {@code uniqueidentifier}).
   *
   * @param rs result set positioned on the boundary row.
   * @param index column index of the boundary.
   * @param partitionColumn the partition column.
   * @return boundary bytes, or null.
   */
  default byte[] extractBinaryBoundary(ResultSet rs, int index, PartitionColumn partitionColumn)
      throws SQLException {
    return rs.getBytes(index);
  }

  /**
   * Converts a boundary value to the object bound to a prepared statement parameter. The default
   * binds the value unchanged.
   *
   * <p>Override for values the JDBC driver doesn't accept as is (for example bytes of a Postgres
   * {@code uuid}, which must be bound as a {@link java.util.UUID}). Must be the inverse of {@link
   * #extractBinaryBoundary} for binary columns.
   *
   * @param value boundary value (start or end of a range), or null.
   * @param partitionColumn the partition column.
   * @return the object to bind.
   */
  @Nullable
  default Object toQueryParameter(@Nullable Object value, PartitionColumn partitionColumn) {
    return value;
  }
}
