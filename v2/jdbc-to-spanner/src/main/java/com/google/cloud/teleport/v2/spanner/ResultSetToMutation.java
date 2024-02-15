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
package com.google.cloud.teleport.v2.spanner;

import com.google.cloud.spanner.Mutation;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Collections;
import java.util.Set;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link JdbcIO.RowMapper} implementation to convert Jdbc ResultSet rows to Cloud Spanner Mutation.
 */
public class ResultSetToMutation implements JdbcIO.RowMapper<Mutation> {

  private static final Logger LOG = LoggerFactory.getLogger(ResultSetToMutation.class);

  /** Factory method for {@link ResultSetToMutation}. */
  public static JdbcIO.RowMapper<Mutation> create(String table, Set<String> columnsToIgnore) {
    return new ResultSetToMutation(table, columnsToIgnore);
  }

  private String table;
  private Set<String> columnsToIgnore;

  private ResultSetToMutation(String table, Set<String> columnsToIgnore) {
    this.table = table;
    this.columnsToIgnore = columnsToIgnore == null ? Collections.emptySet() : columnsToIgnore;
  }

  @Override
  public Mutation mapRow(ResultSet resultSet) throws Exception {
    ResultSetMetaData metaData = resultSet.getMetaData();
    Mutation.WriteBuilder mutation = Mutation.newInsertOrUpdateBuilder(table);
    for (int i = 1; i <= metaData.getColumnCount(); i++) {
      Object columnVal = resultSet.getObject(i);
      if (columnVal == null) {
        continue;
      }
      String columnName = metaData.getColumnName(i);
      if (columnsToIgnore.contains(columnName)) {
        continue;
      }
      int columnType = metaData.getColumnType(i);
      switch (columnType) {
        case java.sql.Types.VARCHAR:
        case java.sql.Types.CHAR:
        case java.sql.Types.LONGVARCHAR:
          mutation.set(columnName).to(String.valueOf(columnVal));
          break;
        case java.sql.Types.BIGINT:
        case java.sql.Types.INTEGER:
        case java.sql.Types.SMALLINT:
        case java.sql.Types.TINYINT:
          mutation.set(columnName).to(Long.valueOf(columnVal.toString()));
          break;
        case java.sql.Types.BOOLEAN:
        case java.sql.Types.BIT:
          mutation.set(columnName).to(resultSet.getBoolean(i));
          break;
        case java.sql.Types.DOUBLE:
        case java.sql.Types.FLOAT:
        case java.sql.Types.REAL:
          mutation.set(columnName).to(((Number) columnVal).doubleValue());
          break;
        case java.sql.Types.TIMESTAMP:
        case java.sql.Types.TIME:
          com.google.cloud.Timestamp ts = com.google.cloud.Timestamp.of(resultSet.getTimestamp(i));
          mutation.set(columnName).to(ts);
          break;
        default:
          throw new IllegalArgumentException(
              "Not supported: "
                  + columnName
                  + ","
                  + columnType
                  + ":"
                  + metaData.getColumnTypeName(i));
      }
    }

    return mutation.build();
  }
}
