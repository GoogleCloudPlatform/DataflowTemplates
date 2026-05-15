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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.JdbcSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.postgresql.PostgreSQLDialectAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.postgresql.PostgreSQLDialectAdapter.PostgreSQLVersion;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.JdbcIoWrapper;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceColumnIndexInfo;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceColumnIndexInfo.IndexType;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Arrays;
import javax.sql.DataSource;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Rigorous unit tests demonstrating the precise technical constraints and bugs when handling
 * 128-bit binary UUIDs via byte arrays in Java.
 */
@RunWith(MockitoJUnitRunner.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class BinaryUUIDBehaviorTest {

  /**
   * Demonstrates Point 2 from the architectural critique: When a positive 128-bit integer has its
   * most significant bit set to 1 (e.g. UUID starting with 8-f), BigInteger.toByteArray() prepends
   * an extra 0x00 sign byte to preserve positive signum in two's complement, resulting in a 17-byte
   * array instead of 16.
   */
  @Test
  public void testBigIntegerToByteArraySignBit() {
    // Create a 16-byte raw binary array with leading bit = 1 (e.g. 0xFF across all 16 bytes)
    byte[] raw16Bytes = new byte[16];
    Arrays.fill(raw16Bytes, (byte) 0xFF);
    assertThat(raw16Bytes.length).isEqualTo(16);

    // Convert to positive BigInteger (signum 1)
    BigInteger unsigned128BitNum = new BigInteger(1, raw16Bytes);

    // Convert back to byte array
    byte[] serializedBytes = unsigned128BitNum.toByteArray();

    // Assert that BigInteger returned 17 bytes due to leading 0x00 sign byte
    assertThat(serializedBytes.length).isEqualTo(17);
    assertThat(serializedBytes[0]).isEqualTo((byte) 0x00);
  }

  /**
   * Demonstrates Point 4 from the architectural critique: For Java arrays (byte[]),
   * Objects.equal(a, b) evaluates reference equality (a == b) rather than value equality. Two
   * distinct byte[] instances with identical contents return false.
   */
  @Test
  public void testByteArrayObjectsEqual() {
    byte[] arrayA = new byte[] {0x11, 0x22, 0x33, 0x44};
    byte[] arrayB = new byte[] {0x11, 0x22, 0x33, 0x44};

    // Objects.equal evaluates to false
    assertThat(Objects.equal(arrayA, arrayB)).isFalse();
  }

  /**
   * Demonstrates the successful resolution of Point 4 across the data lifecycle: Proves that
   * Boundary.equals(), Boundary.hashCode(), Range.isMergable(), and Range.mergeRange() correctly
   * evaluate byte[] contents rather than reference equality.
   */
  @Test
  public void testBoundaryAndRangeByteEqualityAndMergability() {
    byte[] startA =
        new byte[] {
          0x00,
          0x11,
          0x22,
          0x33,
          0x44,
          0x55,
          0x66,
          0x77,
          (byte) 0x88,
          (byte) 0x99,
          (byte) 0xAA,
          (byte) 0xBB,
          (byte) 0xCC,
          (byte) 0xDD,
          (byte) 0xEE,
          (byte) 0xFF
        };
    byte[] startB = startA.clone(); // Distinct object instance, identical contents
    byte[] midA =
        new byte[] {
          0x7F,
          0x11,
          0x22,
          0x33,
          0x44,
          0x55,
          0x66,
          0x77,
          (byte) 0x88,
          (byte) 0x99,
          (byte) 0xAA,
          (byte) 0xBB,
          (byte) 0xCC,
          (byte) 0xDD,
          (byte) 0xEE,
          (byte) 0xFF
        };
    byte[] midB = midA.clone();
    byte[] endA =
        new byte[] {
          (byte) 0xFF,
          0x11,
          0x22,
          0x33,
          0x44,
          0x55,
          0x66,
          0x77,
          (byte) 0x88,
          (byte) 0x99,
          (byte) 0xAA,
          (byte) 0xBB,
          (byte) 0xCC,
          (byte) 0xDD,
          (byte) 0xEE,
          (byte) 0xFF
        };
    byte[] endB = endA.clone();

    TableIdentifier tableId =
        TableIdentifier.builder().setDataSourceId("test_ds").setTableName("test_table").build();
    PartitionColumn col =
        PartitionColumn.builder()
            .setColumnName("id")
            .setColumnClass(byte[].class)
            .setColumnTypeName("uuid")
            .build();

    Boundary<byte[]> boundaryA =
        Boundary.<byte[]>builder()
            .setTableIdentifier(tableId)
            .setPartitionColumn(col)
            .setStart(startA)
            .setEnd(endA)
            .setBoundarySplitter(BoundarySplitterFactory.create(byte[].class))
            .build();

    Boundary<byte[]> boundaryB =
        Boundary.<byte[]>builder()
            .setTableIdentifier(tableId)
            .setPartitionColumn(col)
            .setStart(startB)
            .setEnd(endB)
            .setBoundarySplitter(BoundarySplitterFactory.create(byte[].class))
            .build();

    // Prove Boundary equals() evaluates byte[] contents correctly
    assertThat(boundaryA).isEqualTo(boundaryB);
    assertThat(boundaryA.hashCode()).isEqualTo(boundaryB.hashCode());

    // Construct left and right ranges to test mergability across distinct array instances
    Range leftRange =
        Range.builder()
            .setTableIdentifier(tableId)
            .setBoundarySplitter(BoundarySplitterFactory.create(byte[].class))
            .setColName("id")
            .setColClass(byte[].class)
            .setStart(startA)
            .setEnd(midA)
            .setCount(100L)
            .setIsFirst(true)
            .setIsLast(false)
            .build();

    Range rightRange =
        Range.builder()
            .setTableIdentifier(tableId)
            .setBoundarySplitter(BoundarySplitterFactory.create(byte[].class))
            .setColName("id")
            .setColClass(byte[].class)
            .setStart(midB)
            .setEnd(endB)
            .setCount(100L)
            .setIsFirst(false)
            .setIsLast(true)
            .build();

    // Prove Range isMergable() and mergeRange() evaluate byte[] value equality correctly
    assertThat(leftRange.isMergable(rightRange)).isTrue();
    Range merged = leftRange.mergeRange(rightRange, null);
    assertThat(merged.count()).isEqualTo(200L);
    assertThat(java.util.Arrays.equals((byte[]) merged.start(), startA)).isTrue();
    assertThat(java.util.Arrays.equals((byte[]) merged.end(), endA)).isTrue();
  }

  private static byte[] parseHexBytes(String hexString) {
    String cleanString = hexString.replace("-", "");
    byte[] bytes = new byte[cleanString.length() / 2];
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = (byte) Integer.parseInt(cleanString.substring(i * 2, i * 2 + 2), 16);
    }
    return bytes;
  }

  /**
   * Exhaustively verifies the extremes of the 128-bit UUID space (Stage 4): 1. BigInteger Byte
   * Array Quirks (17-byte sign expansion, <16-byte leading zero truncation, absolute zero) 2. Null
   * Boundary Handling (open start, open end, both null) 3. Mathematical Correctness (perfect halves
   * midpoint math, adjacent identifiers rounding) 4. Range Inversion (reversed inputs swapping
   * bounds)
   */
  @Test
  public void testExhaustiveUUIDSplittingEdgeCases() {
    BoundarySplitter<byte[]> splitter = BoundarySplitterFactory.create(byte[].class);
    PartitionColumn col =
        PartitionColumn.builder()
            .setColumnName("id")
            .setColumnClass(byte[].class)
            .setColumnTypeName("uuid")
            .build();

    // 1. BigInteger Byte Array Quirks (The Danger Zone)
    // Sign-Byte Extension (17-byte array)
    byte[] start1 = parseHexBytes("80000000-0000-0000-0000-000000000000");
    byte[] end1 = parseHexBytes("ffffffff-ffff-ffff-ffff-ffffffffffff");
    byte[] expectedMid1 = parseHexBytes("bfffffff-ffff-ffff-ffff-ffffffffffff");
    byte[] mid1 = splitter.getSplitPoint(start1, end1, col, null, null);
    assertThat(mid1).isEqualTo(expectedMid1);

    // Leading Zeros Truncation (Under 16 bytes)
    byte[] start2 = parseHexBytes("00000000-0000-0000-0000-000000000000");
    byte[] end2 = parseHexBytes("00000000-0000-0000-0000-000000000020");
    byte[] expectedMid2 = parseHexBytes("00000000-0000-0000-0000-000000000010");
    byte[] mid2 = splitter.getSplitPoint(start2, end2, col, null, null);
    assertThat(mid2).isEqualTo(expectedMid2);

    // Absolute Zero UUID
    byte[] startZero = parseHexBytes("00000000-0000-0000-0000-000000000000");
    byte[] midZero = splitter.getSplitPoint(startZero, startZero, col, null, null);
    assertThat(midZero).isEqualTo(startZero);

    // 2. Null Boundary Handling
    assertThat(splitter.getSplitPoint(null, null, col, null, null)).isNull();

    byte[] openStartMid = splitter.getSplitPoint(null, startZero, col, null, null);
    assertThat(openStartMid).isEqualTo(startZero);

    byte[] openEndMid = splitter.getSplitPoint(end1, null, col, null, null);
    assertThat(openEndMid).isEqualTo(end1);

    // 3. Mathematical Correctness & Rounding
    // Perfect Halves
    byte[] expectedPerfectHalf = parseHexBytes("7fffffff-ffff-ffff-ffff-ffffffffffff");
    byte[] midPerfectHalf = splitter.getSplitPoint(startZero, end1, col, null, null);
    assertThat(midPerfectHalf).isEqualTo(expectedPerfectHalf);

    // Adjacent Identifiers
    byte[] startAdjacent = parseHexBytes("00000000-0000-0000-0000-000000000001");
    byte[] endAdjacent = parseHexBytes("00000000-0000-0000-0000-000000000002");
    byte[] midAdjacent = splitter.getSplitPoint(startAdjacent, endAdjacent, col, null, null);
    assertThat(midAdjacent).isEqualTo(startAdjacent);

    // 4. Range Inversion (Reversed Inputs)
    byte[] midInverted = splitter.getSplitPoint(end1, startZero, col, null, null);
    assertThat(midInverted).isEqualTo(expectedPerfectHalf);
  }

  @Test
  public void testStage1PostgreSQLSchemaDiscovery() throws Exception {
    DataSource mockDataSource = mock(DataSource.class);
    Connection mockConnection = mock(Connection.class);
    PreparedStatement mockStatement = mock(PreparedStatement.class);
    ResultSet mockResultSet = mock(ResultSet.class);

    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(any())).thenReturn(mockStatement);
    when(mockStatement.executeQuery()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true).thenReturn(false);

    when(mockResultSet.getString("table_name")).thenReturn("test_uuid_table");
    when(mockResultSet.getString("type_category")).thenReturn("U");
    when(mockResultSet.getString("type_name")).thenReturn("uuid");
    when(mockResultSet.getString("column_name")).thenReturn("uuid_col");
    when(mockResultSet.getString("index_name")).thenReturn("uuid_idx");
    when(mockResultSet.getBoolean("is_unique")).thenReturn(true);
    when(mockResultSet.getBoolean("is_primary")).thenReturn(true);
    when(mockResultSet.getLong("cardinality")).thenReturn(1000L);
    when(mockResultSet.getLong("ordinal_position")).thenReturn(1L);

    PostgreSQLDialectAdapter adapter = new PostgreSQLDialectAdapter(PostgreSQLVersion.DEFAULT);
    JdbcSchemaReference ref =
        JdbcSchemaReference.builder().setDbName("test_db").setNamespace("test_schema").build();
    ImmutableMap<String, ImmutableList<SourceColumnIndexInfo>> indexes =
        adapter.discoverTableIndexes(mockDataSource, ref, ImmutableList.of("test_uuid_table"));

    assertThat(indexes).containsKey("test_uuid_table");
    SourceColumnIndexInfo indexInfo = indexes.get("test_uuid_table").get(0);
    assertThat(indexInfo.columnName()).isEqualTo("uuid_col");
    assertThat(indexInfo.indexType()).isEqualTo(IndexType.BINARY);
    assertThat(indexInfo.columnTypeName()).isEqualTo("uuid");
  }

  @Test
  public void testStage2ConfigInference() throws Exception {
    SourceColumnIndexInfo indexInfo =
        SourceColumnIndexInfo.builder()
            .setColumnName("uuid_col")
            .setIndexName("uuid_idx")
            .setIsUnique(true)
            .setIsPrimary(true)
            .setCardinality(1000L)
            .setOrdinalPosition(1L)
            .setIndexType(IndexType.BINARY)
            .setColumnTypeName("uuid")
            .build();

    Method m =
        JdbcIoWrapper.class.getDeclaredMethod(
            "partitionColumnFromIndexInfo", SourceColumnIndexInfo.class);
    m.setAccessible(true);
    PartitionColumn pc = (PartitionColumn) m.invoke(null, indexInfo);

    assertThat(pc.columnName()).isEqualTo("\"uuid_col\"");
    assertThat(pc.columnClass()).isEqualTo(byte[].class);
    assertThat(pc.columnTypeName()).isEqualTo("uuid");
  }

  @Test
  public void testStage3BoundaryExtraction() throws Exception {
    ResultSet mockResultSet = mock(ResultSet.class);
    ResultSetMetaData mockMetaData = mock(ResultSetMetaData.class);

    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getMetaData()).thenReturn(mockMetaData);
    when(mockMetaData.getColumnTypeName(1)).thenReturn("uuid");
    when(mockMetaData.getColumnTypeName(2)).thenReturn("uuid");

    java.util.UUID minUuid = new java.util.UUID(0L, 0L);
    java.util.UUID maxUuid = new java.util.UUID(-1L, -1L);

    when(mockResultSet.getObject(1)).thenReturn(minUuid);
    when(mockResultSet.getObject(1, java.util.UUID.class)).thenReturn(minUuid);
    when(mockResultSet.getObject(2)).thenReturn(maxUuid);
    when(mockResultSet.getObject(2, java.util.UUID.class)).thenReturn(maxUuid);

    TableIdentifier tableId =
        TableIdentifier.builder()
            .setDataSourceId("test_ds")
            .setTableName("test_uuid_table")
            .build();
    PartitionColumn col =
        PartitionColumn.builder()
            .setColumnName("uuid_col")
            .setColumnClass(byte[].class)
            .setColumnTypeName("uuid")
            .build();

    Boundary<byte[]> boundary =
        BoundaryExtractorFactory.create(byte[].class)
            .getBoundary(col, mockResultSet, null, tableId);

    byte[] expectedZero = new byte[16];
    byte[] expectedMax = new byte[16];
    Arrays.fill(expectedMax, (byte) 0xFF);

    assertThat(Arrays.equals(boundary.start(), expectedZero)).isTrue();
    assertThat(Arrays.equals(boundary.end(), expectedMax)).isTrue();
  }

  @Test
  public void testStage5ParameterBinding() throws Exception {
    byte[] startBytes = new byte[16];
    byte[] endBytes = new byte[16];
    Arrays.fill(endBytes, (byte) 0xFF);

    TableIdentifier tableId =
        TableIdentifier.builder()
            .setDataSourceId("test_ds")
            .setTableName("test_uuid_table")
            .build();
    PartitionColumn col =
        PartitionColumn.builder()
            .setColumnName("uuid_col")
            .setColumnClass(byte[].class)
            .setColumnTypeName("uuid")
            .build();

    Boundary<byte[]> boundary =
        Boundary.<byte[]>builder()
            .setTableIdentifier(tableId)
            .setPartitionColumn(col)
            .setStart(startBytes)
            .setEnd(endBytes)
            .setBoundarySplitter(BoundarySplitterFactory.create(byte[].class))
            .build();

    Range range =
        Range.builder()
            .setTableIdentifier(tableId)
            .setBoundarySplitter(BoundarySplitterFactory.create(byte[].class))
            .setColName("uuid_col")
            .setColClass(byte[].class)
            .setStart(startBytes)
            .setEnd(endBytes)
            .setCount(1000L)
            .setIsFirst(true)
            .setIsLast(true)
            .build();

    TableSplitSpecification splitSpec =
        TableSplitSpecification.builder()
            .setTableIdentifier(tableId)
            .setPartitionColumns(ImmutableList.of(col))
            .setApproxRowCount(1000L)
            .build();

    PreparedStatement mockStatement = mock(PreparedStatement.class);
    RangePreparedStatementSetter setter =
        new RangePreparedStatementSetter(ImmutableList.of(splitSpec));
    setter.setParameters(range, mockStatement);

    verify(mockStatement).setObject(2, new java.util.UUID(0L, 0L));
    verify(mockStatement).setObject(3, new java.util.UUID(-1L, -1L));
  }

  @Test
  public void testStage6QueryExecution() throws Exception {
    PostgreSQLDialectAdapter adapter = new PostgreSQLDialectAdapter(PostgreSQLVersion.DEFAULT);
    String readQuery = adapter.getReadQuery("test_uuid_table", ImmutableList.of("\"uuid_col\""));

    assertThat(readQuery)
        .contains(
            "SELECT * FROM test_uuid_table WHERE ((? = FALSE) OR (\"uuid_col\" >= ? AND (\"uuid_col\" < ? OR (? = TRUE AND \"uuid_col\" = ?))))");
  }
}
