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
import static org.mockito.Mockito.when;

import com.google.api.services.bigquery.model.TableRow;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.TimeZone;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link JdbcConverters}. */
@RunWith(MockitoJUnitRunner.class)
public class JdbcConvertersTest {

  private static final String NAME_KEY = "name";
  private static final String NAME_VALUE = "John";
  private static final String AGE_KEY = "age";
  private static final int AGE_VALUE = 24;
  private static final String DESCRIPTION_KEY = "description";

  private static TableRow expectedTableRow;

  @Mock private ResultSet resultSet;
  @Mock private ResultSetMetaData resultSetMetaData;
  @Mock private Clob clobData;

  @Test
  public void testRowMapper() throws Exception {
    when(resultSet.getObject(1)).thenReturn(NAME_VALUE);
    when(resultSet.getObject(2)).thenReturn(AGE_VALUE);
    when(resultSet.getObject(3)).thenReturn(clobData);
    when(resultSet.getClob(3)).thenReturn(clobData);

    when(clobData.length()).thenReturn((long) 20);
    when(clobData.getSubString(1, 20)).thenReturn("This is a long text.");

    when(resultSet.getMetaData()).thenReturn(resultSetMetaData);

    when(resultSetMetaData.getColumnCount()).thenReturn(3);

    when(resultSetMetaData.getColumnName(1)).thenReturn(NAME_KEY);
    when(resultSetMetaData.getColumnTypeName(1)).thenReturn("string");

    when(resultSetMetaData.getColumnName(2)).thenReturn(AGE_KEY);
    when(resultSetMetaData.getColumnTypeName(2)).thenReturn("integer");

    when(resultSetMetaData.getColumnName(3)).thenReturn(DESCRIPTION_KEY);
    when(resultSetMetaData.getColumnTypeName(3)).thenReturn("clob");

    expectedTableRow = new TableRow();
    expectedTableRow.set(NAME_KEY, NAME_VALUE);
    expectedTableRow.set(AGE_KEY, AGE_VALUE);
    expectedTableRow.set(DESCRIPTION_KEY, "This is a long text.");

    JdbcIO.RowMapper<TableRow> resultSetConverters = JdbcConverters.getResultSetToTableRow(false);
    TableRow actualTableRow = resultSetConverters.mapRow(resultSet);

    assertThat(expectedTableRow.size(), equalTo(actualTableRow.size()));
    assertThat(actualTableRow, equalTo(expectedTableRow));
  }

  @Test
  public void testRowMapperWithAlias() throws Exception {
    when(resultSet.getObject(1)).thenReturn(NAME_VALUE);
    when(resultSet.getObject(2)).thenReturn(AGE_VALUE);
    when(resultSet.getObject(3)).thenReturn(clobData);
    when(resultSet.getClob(3)).thenReturn(clobData);

    when(clobData.length()).thenReturn((long) 20);
    when(clobData.getSubString(1, 20)).thenReturn("This is a long text.");

    when(resultSet.getMetaData()).thenReturn(resultSetMetaData);

    when(resultSetMetaData.getColumnCount()).thenReturn(3);

    when(resultSetMetaData.getColumnLabel(1)).thenReturn(NAME_KEY);
    when(resultSetMetaData.getColumnTypeName(1)).thenReturn("string");

    when(resultSetMetaData.getColumnLabel(2)).thenReturn(AGE_KEY);
    when(resultSetMetaData.getColumnTypeName(2)).thenReturn("integer");

    when(resultSetMetaData.getColumnLabel(3)).thenReturn(DESCRIPTION_KEY);
    when(resultSetMetaData.getColumnTypeName(3)).thenReturn("clob");

    expectedTableRow = new TableRow();
    expectedTableRow.set(NAME_KEY, NAME_VALUE);
    expectedTableRow.set(AGE_KEY, AGE_VALUE);
    expectedTableRow.set(DESCRIPTION_KEY, "This is a long text.");

    JdbcIO.RowMapper<TableRow> resultSetConverters = JdbcConverters.getResultSetToTableRow(true);
    TableRow actualTableRow = resultSetConverters.mapRow(resultSet);

    assertThat(expectedTableRow.size(), equalTo(actualTableRow.size()));
    assertThat(actualTableRow, equalTo(expectedTableRow));
  }

  @Test
  public void testTemporalFields() throws Exception {
    // Must use TimeZone.getDefault() time zone here because JdbcConverters will convert timestamps
    // to string in the *default* time zone, which we don't know (may be different depending on the
    // time zone of the machine that runs the test).
    ZonedDateTime zdt =
        LocalDateTime.parse("2023-01-02T13:14:15.666777888")
            .atZone(TimeZone.getDefault().toZoneId());

    Date dateObj = new Date(zdt.toInstant().toEpochMilli());
    LocalDateTime datetimeObj = zdt.toLocalDateTime();
    Timestamp timestampObj = Timestamp.from(zdt.toInstant());

    when(resultSet.getDate(1)).thenReturn(dateObj);
    when(resultSet.getObject(1)).thenReturn(dateObj);
    when(resultSet.getObject(2)).thenReturn(datetimeObj);
    when(resultSet.getObject(3)).thenReturn(timestampObj);
    when(resultSet.getTimestamp(3)).thenReturn(timestampObj);
    when(resultSet.getMetaData()).thenReturn(resultSetMetaData);

    when(resultSetMetaData.getColumnCount()).thenReturn(3);
    when(resultSetMetaData.getColumnName(1)).thenReturn("date_column");
    when(resultSetMetaData.getColumnTypeName(1)).thenReturn("date");
    when(resultSetMetaData.getColumnName(2)).thenReturn("datetime_column");
    when(resultSetMetaData.getColumnTypeName(2)).thenReturn("datetime");
    when(resultSetMetaData.getColumnName(3)).thenReturn("timestamp_column");
    when(resultSetMetaData.getColumnTypeName(3)).thenReturn("timestamp");

    expectedTableRow = new TableRow();
    expectedTableRow.set("date_column", "2023-01-02");
    expectedTableRow.set("datetime_column", "2023-01-02 13:14:15.666777");
    expectedTableRow.set(
        "timestamp_column", "2023-01-02 13:14:15.666777" + zdt.getOffset().getId());

    JdbcIO.RowMapper<TableRow> resultSetConverters = JdbcConverters.getResultSetToTableRow(false);
    TableRow actualTableRow = resultSetConverters.mapRow(resultSet);

    assertThat(expectedTableRow.size(), equalTo(actualTableRow.size()));
    assertThat(actualTableRow, equalTo(expectedTableRow));
  }

  @Test
  public void testMSSQLDateTime() throws Exception {
    // Must use TimeZone.getDefault() time zone here because JdbcConverters will convert timestamps
    // to string in the *default* time zone, which we don't know (may be different depending on the
    // time zone of the machine that runs the test).
    ZonedDateTime zdt =
        LocalDateTime.parse("2023-01-02T13:14:15.666777888")
            .atZone(TimeZone.getDefault().toZoneId());

    Timestamp timestampObj = Timestamp.from(zdt.toInstant());

    when(resultSet.getObject(1)).thenReturn(timestampObj);
    when(resultSet.getTimestamp(1)).thenReturn(timestampObj);
    when(resultSet.getMetaData()).thenReturn(resultSetMetaData);

    when(resultSetMetaData.getColumnCount()).thenReturn(1);
    when(resultSetMetaData.getColumnName(1)).thenReturn("datetime_column");
    when(resultSetMetaData.getColumnTypeName(1)).thenReturn("datetime");

    expectedTableRow = new TableRow();
    expectedTableRow.set("datetime_column", "2023-01-02 13:14:15.666777" + zdt.getOffset().getId());

    JdbcIO.RowMapper<TableRow> resultSetConverters = JdbcConverters.getResultSetToTableRow(false);
    TableRow actualTableRow = resultSetConverters.mapRow(resultSet);

    assertThat(expectedTableRow.size(), equalTo(actualTableRow.size()));
    assertThat(actualTableRow, equalTo(expectedTableRow));
  }

  @Test
  public void testNullFields() throws Exception {
    when(resultSet.getObject(1)).thenReturn(null);
    when(resultSet.getObject(2)).thenReturn(null);
    when(resultSet.getMetaData()).thenReturn(resultSetMetaData);

    when(resultSetMetaData.getColumnCount()).thenReturn(2);

    when(resultSetMetaData.getColumnName(1)).thenReturn(NAME_KEY);
    when(resultSetMetaData.getColumnName(2)).thenReturn("date_column");

    expectedTableRow = new TableRow();
    expectedTableRow.set(NAME_KEY, null);
    expectedTableRow.set("date_column", null);

    JdbcIO.RowMapper<TableRow> resultSetConverters = JdbcConverters.getResultSetToTableRow(false);
    TableRow actualTableRow = resultSetConverters.mapRow(resultSet);

    assertThat(expectedTableRow.size(), equalTo(actualTableRow.size()));
    assertThat(actualTableRow, equalTo(expectedTableRow));
  }
}
