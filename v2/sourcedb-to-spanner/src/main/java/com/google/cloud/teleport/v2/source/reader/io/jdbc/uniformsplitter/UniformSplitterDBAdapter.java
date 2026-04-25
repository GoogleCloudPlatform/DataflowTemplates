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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.BoundaryExtractorFactory;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationOrderRow.CollationsOrderQueryColumns;
import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;

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
   * Describes the shape of the result set returned by {@link #getCollationsOrderQuery}.
   *
   * <ul>
   *   <li>{@link #WEIGHT_BYTES} – the query returns raw {@code WEIGHT_STRING} sort-key bytes for
   *       each character (columns {@code weight_non_trailing}, {@code weight_trailing}, {@code
   *       is_empty}, {@code is_space}). Java performs all grouping, ranking and
   *       equivalent-character resolution. Used by the MySQL adapter.
   *   <li>{@link #WITH_RANKS} – the query returns pre-computed dense ranks ({@code codepoint_rank},
   *       {@code codepoint_rank_pad_space}) together with {@code is_empty} and {@code is_space}.
   *       Java resolves equivalent characters from the rank groups. Used by the PostgreSQL adapter.
   * </ul>
   */
  enum CollationQueryResultType {
    WEIGHT_BYTES,
    WITH_RANKS
  }

  /**
   * Returns the type of result produced by {@link #getCollationsOrderQuery}. Defaults to {@link
   * CollationQueryResultType#WITH_RANKS}.
   */
  default CollationQueryResultType collationQueryResultType() {
    return CollationQueryResultType.WITH_RANKS;
  }

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
}
