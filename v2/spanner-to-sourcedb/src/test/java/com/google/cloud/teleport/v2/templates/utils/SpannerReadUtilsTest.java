/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.templates.utils;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDb.Options;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class SpannerReadUtilsTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public final MockitoRule mocktio = MockitoJUnit.rule();

  @Mock private SpannerAccessor spannerAccessor;

  @Mock private DatabaseClient mockDatabaseClient;

  @Mock private ReadOnlyTransaction mockReadOnlyTransaction;

  @Mock private DoFn.ProcessContext processContext;

  @Mock private Options mockOptions;

  @Mock private PCollectionView<Ddl> mockDdlView;

  Struct mockRow = mock(Struct.class);

  private static final String SESSION_FILE_PATH =
      "src/test/resources/AssignShardIdTestSession.json";
  private static final String ALL_TYPES_SESSION_FILE_PATH =
      "src/test/resources/AssignShardIdTestAllDataTypesSession.json";

  @Before
  public void setUp() {
    mockSpannerReadRow();
    when(processContext.getPipelineOptions()).thenReturn(mockOptions);
    ShardingLogicImplFetcher.reset();
  }

  private void mockSpannerReadRow() {
    when(spannerAccessor.getDatabaseClient()).thenReturn(mockDatabaseClient);

    when(mockDatabaseClient.singleUse(any(TimestampBound.class)))
        .thenReturn(mockReadOnlyTransaction);

    when(mockRow.getValue("accountId")).thenReturn(Value.string("Id1"));
    when(mockRow.getValue("accountName")).thenReturn(Value.string("xyz"));
    when(mockRow.getValue("migration_shard_id")).thenReturn(Value.string("shard1"));
    when(mockRow.isNull("string_col_null")).thenReturn(true);
    when(mockRow.getValue("accountNumber")).thenReturn(Value.int64(1));
    when(mockRow.isNull("int_64_col_null")).thenReturn(true);
    when(mockRow.getValue("bytesCol"))
        .thenReturn(Value.bytes(ByteArray.copyFrom("GOOGLE".getBytes())));
    when(mockRow.isNull("bytes_col_null")).thenReturn(true);
    when(mockRow.getDouble("float_64_col")).thenReturn(0.5);
    when(mockRow.getValue("float_64_col")).thenReturn(Value.float64(0.5));
    when(mockRow.getDouble("float_64_col_nan")).thenReturn(Double.NaN);
    when(mockRow.getValue("float_64_col_nan")).thenReturn(Value.float64(Double.NaN));
    when(mockRow.getDouble("float_64_col_infinity")).thenReturn(Double.POSITIVE_INFINITY);
    when(mockRow.getValue("float_64_col_infinity"))
        .thenReturn(Value.float64(Double.POSITIVE_INFINITY));
    when(mockRow.getDouble("float_64_col_neg_infinity")).thenReturn(Double.NEGATIVE_INFINITY);
    when(mockRow.getValue("float_64_col_neg_infinity"))
        .thenReturn(Value.float64(Double.NEGATIVE_INFINITY));
    when(mockRow.isNull("float_64_col_null")).thenReturn(true);
    when(mockRow.getValue("float_32_col")).thenReturn(Value.float32(0.5f));
    when(mockRow.getFloat("float_32_col")).thenReturn(0.5f);
    when(mockRow.getFloat("float_32_col_nan")).thenReturn(Float.NaN);
    when(mockRow.getValue("float_32_col_nan")).thenReturn(Value.float32(Float.NaN));
    when(mockRow.getFloat("float_32_col_infinity")).thenReturn(Float.POSITIVE_INFINITY);
    when(mockRow.getValue("float_32_col_infinity"))
        .thenReturn(Value.float32(Float.POSITIVE_INFINITY));
    when(mockRow.getFloat("float_32_col_neg_infinity")).thenReturn(Float.NEGATIVE_INFINITY);
    when(mockRow.getValue("float_32_col_neg_infinity"))
        .thenReturn(Value.float32(Float.NEGATIVE_INFINITY));
    when(mockRow.isNull("float_32_col_null")).thenReturn(true);
    when(mockRow.getBoolean("bool_col")).thenReturn(true);
    when(mockRow.getValue("bool_col")).thenReturn(Value.bool(true));
    when(mockRow.isNull("bool_col_null")).thenReturn(true);
    when(mockRow.isNull("timestamp_col_null")).thenReturn(true);

    // Mock readRow
    when(mockReadOnlyTransaction.readRow(eq("tableName"), any(Key.class), any(Iterable.class)))
        .thenReturn(mockRow);

    doNothing().when(spannerAccessor).close();
  }

  @Test
  public void testGetRowAsMap() throws Exception {
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(SESSION_FILE_PATH);
    List<String> columns =
        List.of("accountId", "accountName", "migration_shard_id", "accountNumber");
    Map<String, Object> actual = SpannerReadUtils.getRowAsMap(mockRow, columns, "tableName", ddl);
    Map<String, Object> expected = new HashMap<>();
    expected.put("accountId", "Id1");
    expected.put("accountName", "xyz");
    expected.put("migration_shard_id", "shard1");
    expected.put("accountNumber", 1L);
    assertEquals(actual, expected);
  }

  @Test(expected = Exception.class)
  public void cannotGetRowAsMap() throws Exception {
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(SESSION_FILE_PATH);
    List<String> columns =
        List.of("accountId", "accountName", "migration_shard_id", "accountNumber", "missingColumn");

    SpannerReadUtils.getRowAsMap(mockRow, columns, "tableName", ddl);
  }
}
