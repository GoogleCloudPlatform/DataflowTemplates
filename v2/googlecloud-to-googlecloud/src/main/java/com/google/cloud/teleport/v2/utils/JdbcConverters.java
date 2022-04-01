/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.utils;

import com.google.api.services.bigquery.model.TableRow;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import org.apache.beam.sdk.io.jdbc.JdbcIO;

/** Common code for Teleport DataplexJdbcIngestion. */
public class JdbcConverters {

  /** Factory method for {@link ResultSetToTableRow}. */
  public static JdbcIO.RowMapper<TableRow> getResultSetToTableRow() {
    return new ResultSetToTableRow();
  }

  /**
   * {@link JdbcIO.RowMapper} implementation to convert Jdbc ResultSet rows to UTF-8 encoded JSONs.
   */
  private static class ResultSetToTableRow implements JdbcIO.RowMapper<TableRow> {

    static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
    static DateTimeFormatter datetimeFormatter =
        DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss.SSSSSS");
    static SimpleDateFormat timestampFormatter =
        new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSSSSSXXX");

    @Override
    public TableRow mapRow(ResultSet resultSet) throws Exception {

      ResultSetMetaData metaData = resultSet.getMetaData();

      TableRow outputTableRow = new TableRow();

      for (int i = 1; i <= metaData.getColumnCount(); i++) {
        if (resultSet.getObject(i) == null) {
          outputTableRow.set(metaData.getColumnName(i), resultSet.getObject(i));
          continue;
        }

        /*
         * DATE:      EPOCH MILLISECONDS -> yyyy-MM-dd
         * DATETIME:  EPOCH MILLISECONDS -> yyyy-MM-dd hh:mm:ss.SSSSSS
         * TIMESTAMP: EPOCH MILLISECONDS -> yyyy-MM-dd hh:mm:ss.SSSSSSXXX
         *
         * MySQL drivers have ColumnTypeName in all caps and postgres in small case
         */
        switch (metaData.getColumnTypeName(i).toLowerCase()) {
          case "date":
            outputTableRow.set(
                metaData.getColumnName(i), dateFormatter.format(resultSet.getDate(i)));
            break;
          case "datetime":
            outputTableRow.set(
                metaData.getColumnName(i),
                datetimeFormatter.format((TemporalAccessor) resultSet.getObject(i)));
            break;
          case "timestamp":
            outputTableRow.set(
                metaData.getColumnName(i), timestampFormatter.format(resultSet.getTimestamp(i)));
            break;
          default:
            outputTableRow.set(metaData.getColumnName(i), resultSet.getObject(i));
        }
      }

      return outputTableRow;
    }
  }
}
