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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationMapper;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationReference;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import org.apache.beam.sdk.io.jdbc.JdbcIO.PoolableDataSourceProvider;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.values.PCollectionView;
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
  public void testFromBigIntegers() throws SQLException {
    final BigInteger unsignedBigIntMax = new BigInteger("18446744073709551615");
    PartitionColumn partitionColumn =
        PartitionColumn.builder().setColumnName("col1").setColumnClass(BigInteger.class).build();
    BoundaryExtractor<BigInteger> extractor = BoundaryExtractorFactory.create(BigInteger.class);
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getBigDecimal(1)).thenReturn(new BigDecimal(BigInteger.ZERO));
    // BigInt Unsigned Max in MySQL
    when(mockResultSet.getBigDecimal(2)).thenReturn(new BigDecimal(unsignedBigIntMax));
    Boundary<BigInteger> boundary = extractor.getBoundary(partitionColumn, mockResultSet, null);

    assertThat(boundary.start()).isEqualTo(BigInteger.ZERO);
    assertThat(boundary.end()).isEqualTo(unsignedBigIntMax);
    assertThat(boundary.split(null).getLeft().end())
        .isEqualTo((unsignedBigIntMax.divide(BigInteger.TWO)));
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
  public void testFromUnsupported() {
    assertThrows(
        UnsupportedOperationException.class,
        () -> BoundaryExtractorFactory.create(PoolableDataSourceProvider.class));
  }
}
