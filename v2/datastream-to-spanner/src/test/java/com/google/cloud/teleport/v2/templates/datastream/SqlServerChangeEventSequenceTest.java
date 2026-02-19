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
package com.google.cloud.teleport.v2.templates.datastream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import org.junit.Test;

/** Unit tests for testing change event comparison logic in SQLServer database. */
public final class SqlServerChangeEventSequenceTest {

  private final long previousEventTimestamp = 1615159727L;

  private final long eventTimestamp = 1615159728L;

  @Test
  public void canOrderBasedOnTimestamp() {
    SqlServerChangeEventSequence oldEvent =
        new SqlServerChangeEventSequence(previousEventTimestamp, "00000016:00000123:0001");
    SqlServerChangeEventSequence newEvent =
        new SqlServerChangeEventSequence(eventTimestamp, "00000016:00000123:0001");

    assertTrue(oldEvent.compareTo(newEvent) < 0);
    assertTrue(newEvent.compareTo(oldEvent) > 0);
  }

  @Test
  public void canOrderBasedOnLsn() {
    SqlServerChangeEventSequence oldEvent =
        new SqlServerChangeEventSequence(eventTimestamp, "00000016:00000123:0001");
    SqlServerChangeEventSequence newEvent =
        new SqlServerChangeEventSequence(eventTimestamp, "00000016:00000123:0002");

    assertTrue(oldEvent.compareTo(newEvent) < 0);
    assertTrue(newEvent.compareTo(oldEvent) > 0);
  }

  @Test
  public void equalLsn() {
    SqlServerChangeEventSequence oldEvent =
        new SqlServerChangeEventSequence(eventTimestamp, "00000016:00000123:0001");
    SqlServerChangeEventSequence newEvent =
        new SqlServerChangeEventSequence(eventTimestamp, "00000016:00000123:0001");

    assertTrue(oldEvent.compareTo(newEvent) == 0);
  }

  @Test
  public void canOrderDumpEventAndCDCEventAtSameTimestamp() {
    SqlServerChangeEventSequence dumpEvent = new SqlServerChangeEventSequence(eventTimestamp, "");
    SqlServerChangeEventSequence cdcEvent =
        new SqlServerChangeEventSequence(eventTimestamp, "00000016:00000123:0001");

    // Empty LSN (dump event) should come before CDC event with same timestamp
    // "00000016..." > ""
    assertTrue(dumpEvent.compareTo(cdcEvent) < 0);
    assertTrue(cdcEvent.compareTo(dumpEvent) > 0);
  }

  @Test
  public void testCreateFromShadowTableWithUseSqlStatements_SqlServer() throws Exception {
    // Arrange
    TransactionContext transactionContext = mock(TransactionContext.class);
    Ddl shadowTableDdl =
        Ddl.builder()
            .createTable("shadow_table_sqlserver")
            .column("id")
            .int64()
            .endColumn()
            .column("shadow_timestamp")
            .int64()
            .endColumn()
            .column("lsn")
            .string()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .build();
    boolean useSqlStatements = true;

    ChangeEventContext mockContext = mock(ChangeEventContext.class);
    when(mockContext.getShadowTable()).thenReturn("shadow_table_sqlserver");
    when(mockContext.getPrimaryKey()).thenReturn(Key.of(1L));
    when(mockContext.getSafeShadowColumn(DatastreamConstants.SQLSERVER_TIMESTAMP_KEY))
        .thenReturn("shadow_timestamp");
    when(mockContext.getSafeShadowColumn(DatastreamConstants.SQLSERVER_LSN_KEY)).thenReturn("lsn");

    // Mock the behavior of the transaction context
    Struct mockRow = mock(Struct.class);
    when(mockRow.getLong("shadow_timestamp")).thenReturn(1615159728L);
    when(mockRow.getString("lsn")).thenReturn("00000016:00000123:0001");

    ResultSet mockResultSet = mock(ResultSet.class);
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getCurrentRowAsStruct()).thenReturn(mockRow);
    when(transactionContext.executeQuery(any(Statement.class))).thenReturn(mockResultSet);

    // Act
    SqlServerChangeEventSequence result =
        SqlServerChangeEventSequence.createFromShadowTable(
            transactionContext, mockContext, shadowTableDdl, useSqlStatements);

    // Assert
    assertNotNull(result);
    assertEquals((Object) 1615159728L, result.getTimestamp());
    assertEquals("00000016:00000123:0001", result.getLsn());
  }
}
