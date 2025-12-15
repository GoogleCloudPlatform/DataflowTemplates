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
package com.google.cloud.teleport.v2.templates.dbutils.dao;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Options.ReadOption;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.teleport.v2.templates.constants.Constants;
import com.google.cloud.teleport.v2.templates.dbutils.dao.spanner.SpannerDao;
import com.google.cloud.teleport.v2.templates.utils.ShadowTableRecord;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.mockito.Mock;
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
  public void testGetShadowTableRecordReturnsNull() {
    SpannerDao spannerDao = new SpannerDao(mockSpannerAccessor);
    // Mock readRow
    when(mockReadOnlyTransaction.readRow(eq("tableName"), any(Key.class), any(Iterable.class)))
        .thenReturn(null);
    assertThat(spannerDao.getShadowTableRecord("tableName", null)).isNull();
  }

  @Test
  public void testGetShadowTableRecordReturnsRecord() {
    com.google.cloud.spanner.ResultSet resultSet = mock(ResultSet.class);

    Struct row =
        Struct.newBuilder()
            .set(Constants.PROCESSED_COMMIT_TS_COLUMN_NAME)
            .to(Timestamp.parseTimestamp("2023-05-18T12:01:13.088397258Z"))
            .set(Constants.RECORD_SEQ_COLUMN_NAME)
            .to(1)
            .build();

    SpannerDao spannerDao = new SpannerDao(mockSpannerAccessor);
    when(mockReadOnlyTransaction.read(
            eq("junk"), any(KeySet.class), any(Iterable.class), any(ReadOption.class)))
        .thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(row);
    ShadowTableRecord response = spannerDao.getShadowTableRecord("junk", Key.of(1));
    spannerDao.close();

    ShadowTableRecord expectedResponse =
        new ShadowTableRecord(Timestamp.parseTimestamp("2023-05-18T12:01:13.088397258Z"), 1);
    assertThat(response.getProcessedCommitTimestamp())
        .isEqualTo(expectedResponse.getProcessedCommitTimestamp());
    assertThat(response.getRecordSequence()).isEqualTo(expectedResponse.getRecordSequence());
  }

  @Test
  public void testGetShadowTableRecordNoRecord() {
    com.google.cloud.spanner.ResultSet resultSet = mock(ResultSet.class);
    SpannerDao spannerDao = new SpannerDao(mockSpannerAccessor);

    // No pk
    ShadowTableRecord nullResponse = spannerDao.getShadowTableRecord("junk", null);
    assertThat(nullResponse).isNull();

    // Row not found
    when(mockReadOnlyTransaction.read(
            eq("junk"), any(KeySet.class), any(Iterable.class), any(ReadOption.class)))
        .thenReturn(resultSet);
    when(resultSet.next()).thenReturn(false);
    ShadowTableRecord response = spannerDao.getShadowTableRecord("junk", Key.of(1));
    spannerDao.close();
    assertThat(response).isNull();
  }

  @Test(expected = RuntimeException.class)
  public void testGetShadowTableRecordException() {
    SpannerDao spannerDao = new SpannerDao(mockSpannerAccessor);
    doThrow(new RuntimeException("generic exception"))
        .when(
            mockReadOnlyTransaction.read(
                eq("error"), any(KeySet.class), any(Iterable.class), any(ReadOption.class)));
    ShadowTableRecord response = spannerDao.getShadowTableRecord("error", null);
  }

  @Test
  public void testUpdateShadowTable() {
    SpannerDao spannerDao = new SpannerDao(mockSpannerAccessor);
    Mutation mutation = Mutation.newInsertBuilder("T").set("C1").to("x").set("C2").to("y").build();

    spannerDao.updateShadowTable(mutation, mockReadWriteTransaction);
    verify(mockReadWriteTransaction).buffer(eq(mutation));
  }
}
