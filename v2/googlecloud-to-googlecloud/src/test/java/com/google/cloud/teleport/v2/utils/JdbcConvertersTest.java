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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import com.google.api.services.bigquery.model.TableRow;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test class for {@link JdbcConverters}. */
@RunWith(MockitoJUnitRunner.class)
public class JdbcConvertersTest {

  private static final String NAME_KEY = "name";
  private static final String NAME_VALUE = "John";
  private static final String AGE_KEY = "age";
  private static final int AGE_VALUE = 24;
  private static final String TIMESTAMP = "2020-10-15T00:37:23.000Z";

  static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
  static DateTimeFormatter datetimeFormatter =
      DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss.SSSSSS");
  static SimpleDateFormat timestampFormatter =
      new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSSSSSXXX");

  private static TableRow expectedTableRow;

  @Mock private ResultSet resultSet;

  @Mock private ResultSetMetaData resultSetMetaData;

  private static final Logger LOG = LoggerFactory.getLogger(JdbcConvertersTest.class);

  @Test
  public void testRowMapper() throws Exception {
    Mockito.when(resultSet.getObject(1)).thenReturn(NAME_VALUE);
    Mockito.when(resultSet.getObject(2)).thenReturn(AGE_VALUE);
    Mockito.when(resultSet.getMetaData()).thenReturn(resultSetMetaData);

    Mockito.when(resultSetMetaData.getColumnCount()).thenReturn(2);

    Mockito.when(resultSetMetaData.getColumnName(1)).thenReturn(NAME_KEY);
    Mockito.when(resultSetMetaData.getColumnTypeName(1)).thenReturn("string");

    Mockito.when(resultSetMetaData.getColumnName(2)).thenReturn(AGE_KEY);
    Mockito.when(resultSetMetaData.getColumnTypeName(2)).thenReturn("integer");

    expectedTableRow = new TableRow();
    expectedTableRow.set(NAME_KEY, NAME_VALUE);
    expectedTableRow.set(AGE_KEY, AGE_VALUE);

    JdbcIO.RowMapper<TableRow> resultSetConverters = JdbcConverters.getResultSetToTableRow();
    TableRow actualTableRow = resultSetConverters.mapRow(resultSet);

    assertThat(expectedTableRow.size(), equalTo(actualTableRow.size()));
    assertThat(actualTableRow, equalTo(expectedTableRow));
  }

  @Test
  public void testTemporalFields() throws Exception {
    LocalDateTime datetimeObj = LocalDateTime.parse(TIMESTAMP.split("Z")[0]);
    Date dateObj = Date.valueOf(TIMESTAMP.split("T")[0]);
    Timestamp timestampObj = Timestamp.from(Instant.parse(TIMESTAMP));

    Mockito.when(resultSet.getObject(1)).thenReturn(datetimeObj);
    Mockito.when(resultSet.getObject(2)).thenReturn(dateObj);
    Mockito.when(resultSet.getDate(2)).thenReturn(dateObj);
    Mockito.when(resultSet.getObject(3)).thenReturn(timestampObj);
    Mockito.when(resultSet.getTimestamp(3)).thenReturn(timestampObj);
    Mockito.when(resultSet.getMetaData()).thenReturn(resultSetMetaData);

    Mockito.when(resultSetMetaData.getColumnCount()).thenReturn(3);

    Mockito.when(resultSetMetaData.getColumnName(1)).thenReturn("datetime_column");
    Mockito.when(resultSetMetaData.getColumnTypeName(1)).thenReturn("datetime");

    Mockito.when(resultSetMetaData.getColumnName(2)).thenReturn("date_column");
    Mockito.when(resultSetMetaData.getColumnTypeName(2)).thenReturn("date");

    Mockito.when(resultSetMetaData.getColumnName(3)).thenReturn("timestamp_column");
    Mockito.when(resultSetMetaData.getColumnTypeName(3)).thenReturn("timestamp");

    expectedTableRow = new TableRow();
    expectedTableRow.set("datetime_column", datetimeFormatter.format(datetimeObj));
    expectedTableRow.set("date_column", dateFormatter.format(dateObj));
    expectedTableRow.set("timestamp_column", timestampFormatter.format(timestampObj));

    JdbcIO.RowMapper<TableRow> resultSetConverters = JdbcConverters.getResultSetToTableRow();
    TableRow actualTableRow = resultSetConverters.mapRow(resultSet);

    assertThat(expectedTableRow.size(), equalTo(actualTableRow.size()));
    assertThat(actualTableRow, equalTo(expectedTableRow));
  }

  @Test
  public void testNullFields() throws Exception {
    Mockito.when(resultSet.getObject(1)).thenReturn(null);
    Mockito.when(resultSet.getObject(2)).thenReturn(null);
    Mockito.when(resultSet.getMetaData()).thenReturn(resultSetMetaData);

    Mockito.when(resultSetMetaData.getColumnCount()).thenReturn(2);

    Mockito.when(resultSetMetaData.getColumnName(1)).thenReturn(NAME_KEY);
    Mockito.when(resultSetMetaData.getColumnTypeName(1)).thenReturn("string");

    Mockito.when(resultSetMetaData.getColumnName(2)).thenReturn("date_column");
    Mockito.when(resultSetMetaData.getColumnTypeName(2)).thenReturn("date");

    expectedTableRow = new TableRow();
    expectedTableRow.set(NAME_KEY, null);
    expectedTableRow.set("date_column", null);

    JdbcIO.RowMapper<TableRow> resultSetConverters = JdbcConverters.getResultSetToTableRow();
    TableRow actualTableRow = resultSetConverters.mapRow(resultSet);

    assertThat(expectedTableRow.size(), equalTo(actualTableRow.size()));
    assertThat(actualTableRow, equalTo(expectedTableRow));
  }
}
