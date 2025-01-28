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

/** Unit tests for testing change event comparison logic in Oracle database. */
public final class OracleChangeEventSequenceTest {

  private final long previousEventTimestamp = 1615159727L;

  private final long eventTimestamp = 1615159728L;

  @Test
  public void canOrderBasedOnTimestamp() {
    OracleChangeEventSequence oldEvent = new OracleChangeEventSequence(previousEventTimestamp, 2L);
    OracleChangeEventSequence newEvent = new OracleChangeEventSequence(eventTimestamp, 2L);

    assertTrue(oldEvent.compareTo(newEvent) < 0);
    assertTrue(newEvent.compareTo(oldEvent) > 0);
  }

  @Test
  public void canOrderBasedOnScn() {
    OracleChangeEventSequence oldEvent = new OracleChangeEventSequence(eventTimestamp, 2L);
    OracleChangeEventSequence newEvent = new OracleChangeEventSequence(eventTimestamp, 3L);

    assertTrue(oldEvent.compareTo(newEvent) < 0);
    assertTrue(newEvent.compareTo(oldEvent) > 0);
  }

  @Test
  public void canOrderDumpEventAndCDCEventAtSameTimestamp() {
    OracleChangeEventSequence dumpEvent = new OracleChangeEventSequence(eventTimestamp, -1L);
    OracleChangeEventSequence cdcEvent = new OracleChangeEventSequence(eventTimestamp, 3L);

    assertTrue(dumpEvent.compareTo(cdcEvent) < 0);
    assertTrue(cdcEvent.compareTo(dumpEvent) > 0);
  }

  @Test
  public void testCreateFromShadowTableWithUseSqlStatements_Oracle() throws Exception {
    // Arrange
    TransactionContext transactionContext = mock(TransactionContext.class);
    String shadowTable = "shadow_table_oracle";
    Ddl shadowTableDdl =
        Ddl.builder()
            .createTable("shadow_table_oracle")
            .column("id")
            .int64()
            .endColumn()
            .column("timestamp")
            .int64()
            .endColumn()
            .column("scn")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .build();
    Key primaryKey = Key.of(1L);
    boolean useSqlStatements = true;

    // Mock the behavior of the transaction context
    Struct mockRow = mock(Struct.class);
    when(mockRow.getLong("id")).thenReturn(1L);
    when(mockRow.getLong("timestamp")).thenReturn(1615159728L);
    when(mockRow.getLong("scn")).thenReturn(100L);

    ResultSet mockResultSet = mock(ResultSet.class);
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getCurrentRowAsStruct()).thenReturn(mockRow);
    when(transactionContext.executeQuery(any(Statement.class))).thenReturn(mockResultSet);

    // Act
    OracleChangeEventSequence result =
        OracleChangeEventSequence.createFromShadowTable(
            transactionContext, shadowTable, shadowTableDdl, primaryKey, useSqlStatements);

    // Assert
    assertNotNull(result);
    assertEquals((Object) 1615159728L, result.getTimestamp());
    assertEquals((Object) 100L, result.getSCN());
  }
}
