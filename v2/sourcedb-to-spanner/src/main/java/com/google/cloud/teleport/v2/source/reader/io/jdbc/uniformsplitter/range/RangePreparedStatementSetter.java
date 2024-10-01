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
import java.sql.PreparedStatement;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.jdbc.JdbcIO.PreparedStatementSetter;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

/**
 * Implement {@link PreparedStatementSetter} to set {@link PreparedStatement} parameters for getting
 * a count of, and reading a {@link Range}.
 */
public class RangePreparedStatementSetter implements PreparedStatementSetter<Range> {

  private long numColumns;

  public RangePreparedStatementSetter(long numColumns) {
    this.numColumns = numColumns;
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
  public void setRangeParameters(
      @Nullable Range element,
      @UnknownKeyFor @NonNull @Initialized PreparedStatement preparedStatement,
      int startParameterIdx)
      throws @UnknownKeyFor @NonNull @Initialized Exception {

    long rangeColumns = (element != null) ? element.height() + 1 : 0;
    long extraColumns = numColumns - rangeColumns;
    Range range = element;
    for (long i = 0; i < rangeColumns; i++) {
      preparedStatement.setObject(startParameterIdx++, true /* include column */);
      preparedStatement.setObject(startParameterIdx++, range.start());
      preparedStatement.setObject(startParameterIdx++, range.end());
      preparedStatement.setObject(startParameterIdx++, range.isLast());
      preparedStatement.setObject(startParameterIdx++, range.end());
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
    setRangeParameters(element, preparedStatement, 1);
  }
}
