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
import java.sql.Array;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Common code for Jdbc templates. */
public class JdbcConverters {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcConverters.class);

  /** Factory method for {@link ResultSetToTableRow}. */
  public static JdbcIO.RowMapper<TableRow> getResultSetToTableRow(boolean useColumnAlias) {
    return new ResultSetToTableRow(useColumnAlias);
  }

  /**
   * {@link JdbcIO.RowMapper} implementation to convert Jdbc ResultSet rows to UTF-8 encoded JSONs.
   */
  private static class ResultSetToTableRow implements JdbcIO.RowMapper<TableRow> {

    private static final ZoneId DEFAULT_TIME_ZONE_ID = ZoneId.systemDefault();

    private static final DateTimeFormatter DATE_FORMATTER =
        DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter DATETIME_FORMATTER =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSXXX");

    private Boolean useColumnAlias;

    public ResultSetToTableRow(Boolean useColumnAlias) {
      this.useColumnAlias = useColumnAlias;
    }

    @Override
    public TableRow mapRow(ResultSet resultSet) throws Exception {
      ResultSetMetaData metaData = resultSet.getMetaData();

      TableRow outputTableRow = new TableRow();

      for (int i = 1; i <= metaData.getColumnCount(); i++) {
        if (resultSet.getObject(i) == null) {
          outputTableRow.set(getColumnRef(metaData, i), resultSet.getObject(i));
          continue;
        }

        // Arrays have to be handled differently, as circular reference can stack overflow on
        // Postgres
        if (metaData.getColumnClassName(i) != null
            && metaData.getColumnClassName(i).equals("java.sql.Array")) {
          Array array = resultSet.getArray(i);
          List<Object> textList = Arrays.asList((Object[]) array.getArray());
          outputTableRow.set(getColumnRef(metaData, i), textList);
        } else {

          /*
           * DATE:      EPOCH MILLISECONDS -> yyyy-MM-dd
           * DATETIME:  EPOCH MICROSECONDS -> yyyy-MM-dd HH:mm:ss.SSSSSS
           * TIMESTAMP: EPOCH MICROSECONDS -> yyyy-MM-dd HH:mm:ss.SSSSSSXXX
           *
           * MySQL drivers have ColumnTypeName in all caps and postgres in small case
           */
          switch (metaData.getColumnTypeName(i).toLowerCase()) {
            case "date":
              outputTableRow.set(
                  getColumnRef(metaData, i),
                  DATE_FORMATTER.format(resultSet.getDate(i).toLocalDate()));
              break;
            case "datetime":
              Object timeObject = resultSet.getObject(i);

              if (timeObject instanceof TemporalAccessor) {
                outputTableRow.set(
                    getColumnRef(metaData, i),
                    DATETIME_FORMATTER.format((TemporalAccessor) timeObject));
              } else {
                Timestamp ts = resultSet.getTimestamp(i);
                // getTimestamp() returns timestamps in the default (JVM) time zone by default:
                OffsetDateTime odt = ts.toInstant().atZone(DEFAULT_TIME_ZONE_ID).toOffsetDateTime();
                outputTableRow.set(getColumnRef(metaData, i), TIMESTAMP_FORMATTER.format(odt));
              }
              break;
            case "timestamp":
              Timestamp ts = resultSet.getTimestamp(i);
              // getTimestamp() returns timestamps in the default (JVM) time zone by default:
              OffsetDateTime odt = ts.toInstant().atZone(DEFAULT_TIME_ZONE_ID).toOffsetDateTime();
              outputTableRow.set(getColumnRef(metaData, i), TIMESTAMP_FORMATTER.format(odt));
              break;
            case "clob":
              Clob clobObject = resultSet.getClob(i);
              if (clobObject.length() > Integer.MAX_VALUE) {
                LOG.warn(
                    "The Clob value size {} in column {} exceeds 2GB and will be truncated.",
                    clobObject.length(),
                    getColumnRef(metaData, i));
              }
              outputTableRow.set(
                  getColumnRef(metaData, i), clobObject.getSubString(1, (int) clobObject.length()));
              break;
            default:
              outputTableRow.set(getColumnRef(metaData, i), resultSet.getObject(i));
          }
        }
      }

      return outputTableRow;
    }

    protected String getColumnRef(ResultSetMetaData metaData, int index) throws SQLException {
      if (useColumnAlias != null && useColumnAlias) {
        String columnLabel = metaData.getColumnLabel(index);
        if (columnLabel != null && !columnLabel.isEmpty()) {
          return columnLabel;
        }
      }

      return metaData.getColumnName(index);
    }
  }
}
