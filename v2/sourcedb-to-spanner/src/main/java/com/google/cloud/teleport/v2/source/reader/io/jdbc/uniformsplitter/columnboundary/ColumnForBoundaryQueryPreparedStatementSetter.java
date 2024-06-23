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

import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.RangePreparedStatementSetter;
import com.google.common.collect.ImmutableList;
import java.sql.PreparedStatement;
import org.apache.beam.sdk.io.jdbc.JdbcIO.PreparedStatementSetter;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

public class ColumnForBoundaryQueryPreparedStatementSetter
    implements PreparedStatementSetter<ColumnForBoundaryQuery> {

  ImmutableList<String> partitionCols;

  public ColumnForBoundaryQueryPreparedStatementSetter(ImmutableList<String> partitionCols) {
    this.partitionCols = partitionCols;
  }

  @Override
  public void setParameters(
      ColumnForBoundaryQuery element,
      @UnknownKeyFor @NonNull @Initialized PreparedStatement preparedStatement)
      throws @UnknownKeyFor @NonNull @Initialized Exception {
    int parameterIdx = 1;
    // We do 2 aggregations min and max.
    int numAggregation = 2;
    for (int i = 0; i < numAggregation; i++) {
      for (String col : partitionCols) {
        if (col.equals(element.columnName())) {
          preparedStatement.setBoolean(parameterIdx++, true);
        } else {
          preparedStatement.setBoolean(parameterIdx++, false);
        }
      }
    }
    new RangePreparedStatementSetter(partitionCols.size())
        .setRangeParameters(element.parentRange(), preparedStatement, parameterIdx);
  }
}
