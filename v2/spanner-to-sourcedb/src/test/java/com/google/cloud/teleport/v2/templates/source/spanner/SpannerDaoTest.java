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
package com.google.cloud.teleport.v2.templates.source.spanner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.templates.constants.Constants;
import com.google.cloud.teleport.v2.templates.dbutils.SpannerDao;
import com.google.cloud.teleport.v2.templates.utils.ShadowTableRecord;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public final class SpannerDaoTest {

  @Rule public final MockitoRule mocktio = MockitoJUnit.rule();
  @Mock private SpannerAccessor mockSpannerAccessor;

  @Mock private DatabaseClient mockDatabaseClient;

  @Mock private ReadOnlyTransaction mockReadOnlyTransaction;

  @Mock private TransactionContext mockReadWriteTransaction;

  @Before
  public void doBeforeEachTest() throws Exception {
    when(mockSpannerAccessor.getDatabaseClient()).thenReturn(mockDatabaseClient);
    when(mockDatabaseClient.singleUse()).thenReturn(mockReadOnlyTransaction);
    doNothing().when(mockSpannerAccessor).close();
  }

  @Test
  public void testUpdateShadowTable() {
    SpannerDao spannerDao = new SpannerDao(mockSpannerAccessor);
    Mutation mutation = Mutation.newInsertBuilder("T").set("C1").to("x").set("C2").to("y").build();

    spannerDao.updateShadowTable(mutation, mockReadWriteTransaction);
    verify(mockReadWriteTransaction).buffer(eq(mutation));
  }

  @Test
  public void testReadShadowTableRecordWithExclusiveLock() {
    SpannerDao spannerDao = new SpannerDao(mockSpannerAccessor);
    Key primaryKey = Key.of("x");
    Ddl shadowTableDdl =
        Ddl.builder()
            .createTable("shadow_T")
            .column("id")
            .string()
            .endColumn()
            .column(Constants.PROCESSED_COMMIT_TS_COLUMN_NAME)
            .timestamp()
            .endColumn()
            .column(Constants.RECORD_SEQ_COLUMN_NAME)
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .build();

    ResultSet mockResultSet = mock(ResultSet.class);
    when(mockReadWriteTransaction.executeQuery(any(Statement.class))).thenReturn(mockResultSet);

    // Test case 1: No record found
    when(mockResultSet.next()).thenReturn(false);
    ShadowTableRecord result =
        spannerDao.readShadowTableRecordWithExclusiveLock(
            "shadow_T", primaryKey, shadowTableDdl, mockReadWriteTransaction);
    assertNull(result);

    // Test case 2: Record found
    when(mockResultSet.next()).thenReturn(true);
    Struct mockStruct = mock(Struct.class);
    when(mockResultSet.getCurrentRowAsStruct()).thenReturn(mockStruct);
    Timestamp now = Timestamp.now();
    when(mockStruct.getTimestamp(eq(Constants.PROCESSED_COMMIT_TS_COLUMN_NAME))).thenReturn(now);
    when(mockStruct.getLong(eq(Constants.RECORD_SEQ_COLUMN_NAME))).thenReturn(1L);

    result =
        spannerDao.readShadowTableRecordWithExclusiveLock(
            "shadow_T", primaryKey, shadowTableDdl, mockReadWriteTransaction);
    assertEquals(now, result.getProcessedCommitTimestamp());
    assertEquals(1L, result.getRecordSequence());
  }

  @Test
  public void testGetDatabaseClient() {
    SpannerDao spannerDao = new SpannerDao(mockSpannerAccessor);
    DatabaseClient mockClient = mock(DatabaseClient.class);
    when(mockSpannerAccessor.getDatabaseClient()).thenReturn(mockClient);
    assertEquals(mockClient, spannerDao.getDatabaseClient());
  }

  @Test
  public void testClose() {
    SpannerDao spannerDao = new SpannerDao(mockSpannerAccessor);
    spannerDao.close();
    verify(mockSpannerAccessor, times(1)).close();
  }

  @Test
  public void testConstructor_WithSpannerConfig() {
    try (MockedStatic<SpannerAccessor> mockedSpannerAccessor = mockStatic(SpannerAccessor.class)) {
      SpannerAccessor mockAccessor = mock(SpannerAccessor.class);
      mockedSpannerAccessor.when(() -> SpannerAccessor.getOrCreate(any())).thenReturn(mockAccessor);

      SpannerConfig config = SpannerConfig.create();
      SpannerDao spannerDao = new SpannerDao(config);
      assertNotNull(spannerDao);
    }
  }

  @Test
  public void testConstructor_WithIds() {
    try (MockedStatic<SpannerAccessor> mockedSpannerAccessor = mockStatic(SpannerAccessor.class)) {
      SpannerAccessor mockAccessor = mock(SpannerAccessor.class);
      mockedSpannerAccessor.when(() -> SpannerAccessor.getOrCreate(any())).thenReturn(mockAccessor);

      SpannerDao spannerDao = new SpannerDao("project", "instance", "db");
      assertNotNull(spannerDao);
    }
  }
}
