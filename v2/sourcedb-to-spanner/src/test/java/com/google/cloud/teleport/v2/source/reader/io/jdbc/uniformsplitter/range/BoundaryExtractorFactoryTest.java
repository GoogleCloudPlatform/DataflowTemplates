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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range;

import static com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.BoundaryExtractorFactory.BYTE_ARRAY_CLASS;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationMapper;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationReference;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.Map;
import org.apache.beam.sdk.io.jdbc.JdbcIO.PoolableDataSourceProvider;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link BoundaryExtractorFactory}. */
@RunWith(MockitoJUnitRunner.class)
public class BoundaryExtractorFactoryTest {

  @Mock ResultSet mockResultSet;

  @Test
  public void testFromLongs() throws SQLException {
    BoundaryExtractor<Long> extractor = BoundaryExtractorFactory.create(Long.class);
    PartitionColumn partitionColumn =
        PartitionColumn.builder().setColumnName("col1").setColumnClass(Long.class).build();
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getLong(1)).thenReturn(0L);
    when(mockResultSet.getLong(2)).thenReturn(42L);
    Boundary<Long> boundary = extractor.getBoundary(partitionColumn, mockResultSet, null);

    assertThat(boundary.start()).isEqualTo(0L);
    assertThat(boundary.end()).isEqualTo(42L);
    assertThat(boundary.split(null).getLeft().end()).isEqualTo(21L);
    // Mismatched Type
    assertThrows(
        IllegalArgumentException.class,
        () ->
            extractor.getBoundary(
                PartitionColumn.builder()
                    .setColumnName("col1")
                    .setColumnClass(Integer.class)
                    .build(),
                mockResultSet,
                null));
  }

  @Test
  public void testFromIntegers() throws SQLException {
    PartitionColumn partitionColumn =
        PartitionColumn.builder().setColumnName("col1").setColumnClass(Integer.class).build();
    BoundaryExtractor<Integer> extractor = BoundaryExtractorFactory.create(Integer.class);
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getInt(1)).thenReturn(0);
    when(mockResultSet.getInt(2)).thenReturn(42);
    Boundary<Integer> boundary = extractor.getBoundary(partitionColumn, mockResultSet, null);

    assertThat(boundary.start()).isEqualTo(0);
    assertThat(boundary.end()).isEqualTo(42);
    assertThat(boundary.split(null).getLeft().end()).isEqualTo(21);
    // Mismatched Type
    assertThrows(
        IllegalArgumentException.class,
        () ->
            extractor.getBoundary(
                PartitionColumn.builder().setColumnName("col1").setColumnClass(Long.class).build(),
                mockResultSet,
                null));
  }

  @Test
  public void testFromBigDecimalsWholeNumbers() throws SQLException {
    final BigInteger unsignedBigIntMax = new BigInteger("18446744073709551615");
    PartitionColumn partitionColumn =
        PartitionColumn.builder()
            .setColumnName("col1")
            .setColumnClass(BigDecimal.class)
            .setNumericScale(0)
            .build();
    BoundaryExtractor<BigDecimal> extractor = BoundaryExtractorFactory.create(BigDecimal.class);
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getBigDecimal(1))
        .thenReturn(new BigDecimal(BigInteger.ZERO))
        .thenReturn(null);
    // BigInt Unsigned Max in MySQL
    when(mockResultSet.getBigDecimal(2))
        .thenReturn(new BigDecimal(unsignedBigIntMax))
        .thenReturn(null);
    Boundary<BigDecimal> boundaryMinMax =
        extractor.getBoundary(partitionColumn, mockResultSet, null);
    Boundary<BigDecimal> boundaryNull = extractor.getBoundary(partitionColumn, mockResultSet, null);

    assertThat(boundaryMinMax.start()).isEqualTo(new BigDecimal(BigInteger.ZERO));
    assertThat(boundaryMinMax.end()).isEqualTo(new BigDecimal(unsignedBigIntMax));
    assertThat(boundaryMinMax.split(null).getLeft().end())
        .isEqualTo(new BigDecimal(unsignedBigIntMax.divide(BigInteger.TWO)));
    assertThat(boundaryNull.start()).isNull();
    assertThat(boundaryNull.end()).isNull();
    assertThat(boundaryNull.isSplittable(null)).isFalse();
    // Mismatched Type
    assertThrows(
        IllegalArgumentException.class,
        () ->
            extractor.getBoundary(
                PartitionColumn.builder().setColumnName("col1").setColumnClass(long.class).build(),
                mockResultSet,
                null));
  }

  @Test
  public void testFromBigDecimalsRealNumbers() throws SQLException {
    PartitionColumn partitionColumn =
        PartitionColumn.builder()
            .setColumnName("col1")
            .setColumnClass(BigDecimal.class)
            .setNumericScale(2)
            .build();
    System.out.println(partitionColumn.numericScale());
    BoundaryExtractor<BigDecimal> extractor = BoundaryExtractorFactory.create(BigDecimal.class);

    // Check splittable if there is a mid point within the scale
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getBigDecimal(1)).thenReturn(new BigDecimal("1.1"));
    when(mockResultSet.getBigDecimal(2)).thenReturn(new BigDecimal("1.2"));
    Boundary<BigDecimal> boundary1 = extractor.getBoundary(partitionColumn, mockResultSet, null);
    assertThat(boundary1.start()).isEqualTo(new BigDecimal("1.1"));
    assertThat(boundary1.end()).isEqualTo(new BigDecimal("1.2"));
    assertThat(boundary1.split(null).getLeft().end()).isEqualTo(new BigDecimal("1.15"));
    assertThat(boundary1.split(null).getRight().start()).isEqualTo(new BigDecimal("1.15"));
    assertThat(boundary1.isSplittable(null)).isTrue();

    // Check bot splittable if there is no mid point within the scale
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getBigDecimal(1)).thenReturn(new BigDecimal("1.01"));
    when(mockResultSet.getBigDecimal(2)).thenReturn(new BigDecimal("1.02"));
    Boundary<BigDecimal> boundary2 = extractor.getBoundary(partitionColumn, mockResultSet, null);
    assertThat(boundary2.isSplittable(null)).isFalse();

    // Mismatched Type
    assertThrows(
        IllegalArgumentException.class,
        () ->
            extractor.getBoundary(
                PartitionColumn.builder().setColumnName("col1").setColumnClass(Long.class).build(),
                mockResultSet,
                null));
  }

  @Test
  public void testFromBigDecimalsEmptyTable() throws SQLException {
    PartitionColumn partitionColumn =
        PartitionColumn.builder()
            .setColumnName("col1")
            .setColumnClass(BigDecimal.class)
            .setNumericScale(0)
            .build();
    BoundaryExtractor<BigDecimal> extractor = BoundaryExtractorFactory.create(BigDecimal.class);
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getBigDecimal(1)).thenReturn(null);
    // BigInt Unsigned Max in MySQL
    when(mockResultSet.getBigDecimal(2)).thenReturn(null);
    Boundary<BigDecimal> boundary = extractor.getBoundary(partitionColumn, mockResultSet, null);

    assertThat(boundary.start()).isNull();
    assertThat(boundary.end()).isNull();
    assertThat(boundary.split(null).getLeft().end()).isNull();
  }

  @Test
  public void testFromStrings() throws SQLException {
    PartitionColumn partitionColumn =
        PartitionColumn.builder()
            .setColumnName("col1")
            .setColumnClass(String.class)
            .setStringCollation(
                CollationReference.builder()
                    .setDbCharacterSet("latin1")
                    .setDbCollation("latin1_swedish_ci")
                    .setPadSpace(true)
                    .build())
            .setStringMaxLength(255)
            .build();
    BoundaryExtractor<String> extractor = BoundaryExtractorFactory.create(String.class);
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getString(1)).thenReturn("cloud");
    when(mockResultSet.getString(2)).thenReturn("spanner");

    Boundary<String> boundary =
        extractor.getBoundary(
            partitionColumn,
            mockResultSet,
            new BoundaryTypeMapper() {
              @Override
              public BigInteger mapStringToBigInteger(
                  String element,
                  int lengthTOPad,
                  PartitionColumn partitionColumn,
                  ProcessContext c) {
                return null;
              }

              @Override
              public String unMapStringFromBigInteger(
                  BigInteger element, PartitionColumn partitionColumn, ProcessContext c) {
                return null;
              }

              @Override
              public PCollectionView<Map<CollationReference, CollationMapper>>
                  getCollationMapperView() {
                return null;
              }
            });

    assertThat(boundary.start()).isEqualTo("cloud");
    assertThat(boundary.end()).isEqualTo("spanner");
    assertThat(boundary.isSplittable(null)).isTrue();
    // Null type mapper check
    assertThrows(
        IllegalArgumentException.class,
        () -> extractor.getBoundary(partitionColumn, mockResultSet, null));
    // Mismatched Type
    assertThrows(
        IllegalArgumentException.class,
        () ->
            extractor.getBoundary(
                PartitionColumn.builder()
                    .setColumnName("col1")
                    .setColumnClass(Integer.class)
                    .build(),
                mockResultSet,
                null));
  }

  @Test
  public void testFromBinary() throws SQLException {
    final BigInteger unsignedBigIntMax = new BigInteger("18446744073709551615");
    PartitionColumn partitionColumn =
        PartitionColumn.builder().setColumnName("col1").setColumnClass(BYTE_ARRAY_CLASS).build();
    BoundaryExtractor<byte[]> extractor = BoundaryExtractorFactory.create(BYTE_ARRAY_CLASS);
    when(mockResultSet.next()).thenReturn(true);
    doReturn(BigInteger.ZERO.toByteArray()).doReturn(null).when(mockResultSet).getBytes(1);
    doReturn(unsignedBigIntMax.toByteArray()).doReturn(null).when(mockResultSet).getBytes(2);
    Boundary<byte[]> boundaryMinMax = extractor.getBoundary(partitionColumn, mockResultSet, null);
    Boundary<byte[]> boundaryNull = extractor.getBoundary(partitionColumn, mockResultSet, null);

    assertThat(boundaryMinMax.start()).isEqualTo(BigInteger.ZERO.toByteArray());
    assertThat(boundaryMinMax.end()).isEqualTo(unsignedBigIntMax.toByteArray());
    assertThat(boundaryMinMax.split(null).getLeft().end())
        .isEqualTo((unsignedBigIntMax.divide(BigInteger.TWO).toByteArray()));
    assertThat(boundaryNull.start()).isNull();
    assertThat(boundaryNull.end()).isNull();
    assertThat(boundaryNull.isSplittable(null)).isFalse();
    // Mismatched Type
    assertThrows(
        IllegalArgumentException.class,
        () ->
            extractor.getBoundary(
                PartitionColumn.builder().setColumnName("col1").setColumnClass(long.class).build(),
                mockResultSet,
                null));
  }

  @Test
  public void testFromTimestamp() throws SQLException {
    PartitionColumn partitionColumn =
        PartitionColumn.builder().setColumnName("col1").setColumnClass(Timestamp.class).build();
    BoundaryExtractor<Timestamp> extractor = BoundaryExtractorFactory.create(Timestamp.class);
    Timestamp start = Timestamp.valueOf("0000-01-01 00:00:00.000000000");
    Timestamp end = Timestamp.valueOf("2041-01-31 23:59:59.999999999");

    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getTimestamp(eq(1), any())).thenReturn(start);
    when(mockResultSet.getTimestamp(eq(2), any())).thenReturn(end);
    Boundary<Timestamp> boundary = extractor.getBoundary(partitionColumn, mockResultSet, null);
    assertThat(boundary.start()).isEqualTo(start);
    assertThat(boundary.end()).isEqualTo(end);
    Pair<Boundary<Timestamp>, Boundary<Timestamp>> split = boundary.split(null);
    assertThat(split.getLeft().start()).isEqualTo(start);
    assertThat(split.getRight().end()).isEqualTo(end);
    assertThat(split.getLeft().end()).isEqualTo(Timestamp.valueOf("1020-07-10 23:59:59.999999999"));
    assertThat(split.getRight().start()).isEqualTo(split.getLeft().end());
  }

  @Test
  public void testFromTimestampsEmptyTable() throws SQLException {
    PartitionColumn partitionColumn =
        PartitionColumn.builder().setColumnName("col1").setColumnClass(Timestamp.class).build();
    BoundaryExtractor<Timestamp> extractor = BoundaryExtractorFactory.create(Timestamp.class);
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getTimestamp(eq(1), any())).thenReturn(null);
    when(mockResultSet.getTimestamp(eq(2), any())).thenReturn(null);
    Boundary<Timestamp> boundary = extractor.getBoundary(partitionColumn, mockResultSet, null);

    assertThat(boundary.start()).isNull();
    assertThat(boundary.end()).isNull();
    assertThat(boundary.split(null).getLeft().end()).isNull();
  }

  @Test
  public void testFromDate() throws SQLException {
    PartitionColumn partitionColumn =
        PartitionColumn.builder().setColumnName("col1").setColumnClass(Date.class).build();
    BoundaryExtractor<Date> extractor = BoundaryExtractorFactory.create(Date.class);
    Date start = Date.valueOf("0001-01-01");
    Date end = Date.valueOf("2041-01-31");

    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getDate(eq(1), any())).thenReturn(start);
    when(mockResultSet.getDate(eq(2), any())).thenReturn(end);
    Boundary<Date> boundary = extractor.getBoundary(partitionColumn, mockResultSet, null);
    assertThat(boundary.start()).isEqualTo(start);
    assertThat(boundary.end()).isEqualTo(end);
    Pair<Boundary<Date>, Boundary<Date>> split = boundary.split(null);
    assertThat(split.getLeft().start()).isEqualTo(start);
    assertThat(split.getRight().end()).isEqualTo(end);
    assertThat(split.getLeft().end()).isEqualTo(Date.valueOf("1021-01-16"));
    assertThat(split.getRight().start()).isEqualTo(split.getLeft().end());
  }

  @Test
  public void testFromDatesEmptyTable() throws SQLException {
    PartitionColumn partitionColumn =
        PartitionColumn.builder().setColumnName("col1").setColumnClass(Date.class).build();
    BoundaryExtractor<Date> extractor = BoundaryExtractorFactory.create(Date.class);
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getDate(eq(1), any())).thenReturn(null);
    when(mockResultSet.getDate(eq(2), any())).thenReturn(null);
    Boundary<Date> boundary = extractor.getBoundary(partitionColumn, mockResultSet, null);

    assertThat(boundary.start()).isNull();
    assertThat(boundary.end()).isNull();
    assertThat(boundary.split(null).getLeft().end()).isNull();
  }

  @Test
  public void testFromFloat() throws SQLException {
    PartitionColumn partitionColumn =
        PartitionColumn.builder()
            .setColumnName("col1")
            .setColumnClass(Float.class)
            .setDecimalStepSize(new BigDecimal("0.00001"))
            .build();
    BoundaryExtractor<Float> extractor = BoundaryExtractorFactory.create(Float.class);

    // If step between values > min step delta
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getFloat(1)).thenReturn(1.001f);
    when(mockResultSet.getFloat(2)).thenReturn(1.002f);
    Boundary<Float> boundary1 = extractor.getBoundary(partitionColumn, mockResultSet, null);
    // The diff is > minimum delta, boundary is splittable
    assertThat(boundary1.start()).isEqualTo(1.001f);
    assertThat(boundary1.end()).isEqualTo(1.002f);
    assertThat(boundary1.split(null).getLeft().end()).isEqualTo(1.0015f);
    assertThat(boundary1.split(null).getRight().start()).isEqualTo(1.0015f);
    assertThat(boundary1.isSplittable(null)).isTrue();

    // if step between values < min step delta
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getFloat(1)).thenReturn(1.000001f);
    when(mockResultSet.getFloat(2)).thenReturn(1.000002f);
    Boundary<Float> boundary2 = extractor.getBoundary(partitionColumn, mockResultSet, null);
    // The diff is < minimum delta, boundary is NOT splittable
    assertThat(boundary2.isSplittable(null)).isFalse();

    // Mismatched Type
    assertThrows(
        IllegalArgumentException.class,
        () ->
            extractor.getBoundary(
                PartitionColumn.builder().setColumnName("col1").setColumnClass(Long.class).build(),
                mockResultSet,
                null));
  }

  @Test
  public void testFromDouble() throws SQLException {
    PartitionColumn partitionColumn =
        PartitionColumn.builder()
            .setColumnName("col1")
            .setColumnClass(Double.class)
            .setDecimalStepSize(new BigDecimal("0.00001"))
            .build();
    BoundaryExtractor<Double> extractor = BoundaryExtractorFactory.create(Double.class);

    // If step between values > min step delta
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getDouble(1)).thenReturn(1.001);
    when(mockResultSet.getDouble(2)).thenReturn(1.002);
    Boundary<Double> boundary1 = extractor.getBoundary(partitionColumn, mockResultSet, null);
    // The diff is > minimum delta, boundary is splittable
    assertThat(boundary1.start()).isEqualTo(1.001);
    assertThat(boundary1.end()).isEqualTo(1.002);
    assertThat(boundary1.split(null).getLeft().end()).isEqualTo(1.0015);
    assertThat(boundary1.split(null).getRight().start()).isEqualTo(1.0015);
    assertThat(boundary1.isSplittable(null)).isTrue();

    // if step between values < min step delta
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getDouble(1)).thenReturn(1.000001);
    when(mockResultSet.getDouble(2)).thenReturn(1.000002);
    Boundary<Double> boundary2 = extractor.getBoundary(partitionColumn, mockResultSet, null);
    // The diff is < minimum delta, boundary is NOT splittable
    assertThat(boundary2.isSplittable(null)).isFalse();

    // Mismatched Type
    assertThrows(
        IllegalArgumentException.class,
        () ->
            extractor.getBoundary(
                PartitionColumn.builder().setColumnName("col1").setColumnClass(Long.class).build(),
                mockResultSet,
                null));
  }

  @Test
  public void testFromDuration() throws SQLException {
    PartitionColumn partitionColumn =
        PartitionColumn.builder()
            .setColumnName("col1")
            .setColumnClass(Duration.class)
            .setDatetimePrecision(2)
            .build();
    BoundaryExtractor<Duration> extractor = BoundaryExtractorFactory.create(Duration.class);
    String startStr = "70:10:00.15";
    String endStr = "72:30:50.40";
    Duration start = Duration.parse("PT70H10M00.15S");
    Duration end = Duration.parse("PT72H30M50.4S");

    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getString(eq(1))).thenReturn(startStr);
    when(mockResultSet.getString(eq(2))).thenReturn(endStr);
    Boundary<Duration> boundary = extractor.getBoundary(partitionColumn, mockResultSet, null);
    assertThat(boundary.start()).isEqualTo(start);
    assertThat(boundary.end()).isEqualTo(end);
    Pair<Boundary<Duration>, Boundary<Duration>> split = boundary.split(null);
    assertThat(split.getLeft().start()).isEqualTo(start);
    assertThat(split.getRight().end()).isEqualTo(end);
    assertThat(split.getLeft().end()).isEqualTo(Duration.parse("PT71H20M25.27S"));
    assertThat(split.getRight().start()).isEqualTo(split.getLeft().end());

    // Mismatched Type
    assertThrows(
        IllegalArgumentException.class,
        () ->
            extractor.getBoundary(
                PartitionColumn.builder().setColumnName("col1").setColumnClass(long.class).build(),
                mockResultSet,
                null));
  }

  @Test
  public void testFromDurationsEmptyTable() throws SQLException {
    PartitionColumn partitionColumn =
        PartitionColumn.builder()
            .setColumnName("col1")
            .setColumnClass(Duration.class)
            .setDatetimePrecision(2)
            .build();
    BoundaryExtractor<Duration> extractor = BoundaryExtractorFactory.create(Duration.class);
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getString(eq(1))).thenReturn(null);
    when(mockResultSet.getString(eq(2))).thenReturn(null);
    Boundary<Duration> boundary = extractor.getBoundary(partitionColumn, mockResultSet, null);

    assertThat(boundary.start()).isNull();
    assertThat(boundary.end()).isNull();
    assertThat(boundary.split(null).getLeft().end()).isNull();
  }

  @Test
  public void testParseTimeStringToDuration() {
    assertThat(BoundaryExtractorFactory.parseTimeStringToDuration(null)).isNull();
    assertThat(BoundaryExtractorFactory.parseTimeStringToDuration("")).isNull();
    assertThat(BoundaryExtractorFactory.parseTimeStringToDuration("   ")).isNull();
    assertThat(BoundaryExtractorFactory.parseTimeStringToDuration("30"))
        .isEqualTo(Duration.parse("PT30H"));
    assertThat(BoundaryExtractorFactory.parseTimeStringToDuration("-30"))
        .isEqualTo(Duration.parse("-PT30H"));
    assertThat(BoundaryExtractorFactory.parseTimeStringToDuration("30:15"))
        .isEqualTo(Duration.parse("PT30H15M"));
    assertThat(BoundaryExtractorFactory.parseTimeStringToDuration("-30:15"))
        .isEqualTo(Duration.parse("-PT30H15M"));
    assertThat(BoundaryExtractorFactory.parseTimeStringToDuration("30:15:22"))
        .isEqualTo(Duration.parse("PT30H15M22S"));
    assertThat(BoundaryExtractorFactory.parseTimeStringToDuration("-30:15:22"))
        .isEqualTo(Duration.parse("-PT30H15M22S"));
    assertThat(BoundaryExtractorFactory.parseTimeStringToDuration("30:15:22.984353"))
        .isEqualTo(Duration.parse("PT30H15M22.984353S"));
    assertThat(BoundaryExtractorFactory.parseTimeStringToDuration("-30:15:22.984353"))
        .isEqualTo(Duration.parse("-PT30H15M22.984353S"));
  }

  @Test
  public void testFromUnsupported() {
    assertThrows(
        UnsupportedOperationException.class,
        () -> BoundaryExtractorFactory.create(PoolableDataSourceProvider.class));
  }
}
