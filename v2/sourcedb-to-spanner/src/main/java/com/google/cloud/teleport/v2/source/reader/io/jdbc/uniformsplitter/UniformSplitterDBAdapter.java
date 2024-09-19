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

import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationOrderRow.CollationsOrderQueryColumns;
import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.sql.SQLException;

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
   * @return Query to get the order of collation.
   */
  String getCollationsOrderQuery(String dbCharset, String dbCollation);
}
