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
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.RangePreparedStatementSetter;
import com.google.common.collect.ImmutableList;
import java.sql.PreparedStatement;
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

  /** List of partition columns. */
  private ImmutableList<String> partitionCols;

  /**
   * Construct {@link ColumnForBoundaryQuery}.
   *
   * @param partitionCols partition columns.
   */
  public ColumnForBoundaryQueryPreparedStatementSetter(ImmutableList<String> partitionCols) {
    this.partitionCols = partitionCols;
  }

  /**
   * Set statement parameters.
   *
   * @param element details of the colum for which boudnary is to be found.
   * @param preparedStatement the prepared statement.
   * @throws Exception coming form jdbc. Since this is in run-time, beam will retry the exception.
   * @see MysqlDialectAdapter#getBoundaryQuery(String, ImmutableList, String)
   */
  @Override
  public void setParameters(
      ColumnForBoundaryQuery element,
      @UnknownKeyFor @NonNull @Initialized PreparedStatement preparedStatement)
      throws @UnknownKeyFor @NonNull @Initialized Exception {
    int parameterIdx = 1;
    new RangePreparedStatementSetter(partitionCols.size())
        .setRangeParameters(element.parentRange(), preparedStatement, parameterIdx);
  }
}
