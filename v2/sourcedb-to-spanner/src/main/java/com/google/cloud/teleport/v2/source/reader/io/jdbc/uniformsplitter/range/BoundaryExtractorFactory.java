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

import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Factory to construct {@link BoundaryExtractor} for supported {@link class}. */
public class BoundaryExtractorFactory {

  public static final Class BYTE_ARRAY_CLASS = (new byte[] {}).getClass();
  private static final ImmutableMap<Class, BoundaryExtractor<?>> extractorMap =
      ImmutableMap.of(
          Integer.class,
          (BoundaryExtractor<Integer>)
              (partitionColumn, resultSet, boundaryTypeMapper) ->
                  fromIntegers(partitionColumn, resultSet, boundaryTypeMapper),
          Long.class,
          (BoundaryExtractor<Long>)
              (partitionColumn, resultSet, boundaryTypeMapper) ->
                  fromLongs(partitionColumn, resultSet, boundaryTypeMapper),
          String.class,
          (BoundaryExtractor<String>) BoundaryExtractorFactory::fromStrings,
          BigDecimal.class,
          (BoundaryExtractor<BigDecimal>)
              (partitionColumn, resultSet, boundaryTypeMapper) ->
                  fromBigDecimals(partitionColumn, resultSet, boundaryTypeMapper),
          BYTE_ARRAY_CLASS,
          (BoundaryExtractor<byte[]>)
              (partitionColumn, resultSet, boundaryTypeMapper) ->
                  fromBinary(partitionColumn, resultSet, boundaryTypeMapper));

  /**
   * Create a {@link BoundaryExtractor} for the required class.
   *
   * @param c class of the column.
   * @return boundary extractor.
   */
  public static <T extends Serializable> BoundaryExtractor<T> create(Class<T> c) {
    BoundaryExtractor<T> extractor = (BoundaryExtractor<T>) extractorMap.get(c);

    if (extractor == null) {
      throw new UnsupportedOperationException("Range Extractor not implemented for class " + c);
    }
    return extractor;
  }

  private static Boundary<Integer> fromIntegers(
      PartitionColumn partitionColumn,
      ResultSet resultSet,
      @Nullable BoundaryTypeMapper boundaryTypeMapper)
      throws SQLException {
    Preconditions.checkArgument(partitionColumn.columnClass().equals(Integer.class));
    resultSet.next();
    return Boundary.<Integer>builder()
        .setPartitionColumn(partitionColumn)
        .setStart(resultSet.getInt(1))
        .setEnd(resultSet.getInt(2))
        .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
        .setBoundaryTypeMapper(boundaryTypeMapper)
        .build();
  }

  private static Boundary<Long> fromLongs(
      PartitionColumn partitionColumn,
      ResultSet resultSet,
      @Nullable BoundaryTypeMapper boundaryTypeMapper)
      throws SQLException {
    Preconditions.checkArgument(partitionColumn.columnClass().equals(Long.class));
    resultSet.next();
    return Boundary.<Long>builder()
        .setPartitionColumn(partitionColumn)
        .setStart(resultSet.getLong(1))
        .setEnd(resultSet.getLong(2))
        .setBoundarySplitter(BoundarySplitterFactory.create(Long.class))
        .setBoundaryTypeMapper(boundaryTypeMapper)
        .build();
  }

  private static Boundary<BigDecimal> fromBigDecimals(
      PartitionColumn partitionColumn,
      ResultSet resultSet,
      @Nullable BoundaryTypeMapper boundaryTypeMapper)
      throws SQLException {
    Preconditions.checkArgument(partitionColumn.columnClass().equals(BigDecimal.class));
    resultSet.next();
    BigDecimal start = resultSet.getBigDecimal(1);
    BigDecimal end = resultSet.getBigDecimal(2);
    return Boundary.<BigDecimal>builder()
        .setPartitionColumn(partitionColumn)
        .setStart(start)
        .setEnd(end)
        .setBoundarySplitter(BoundarySplitterFactory.create(BigDecimal.class))
        .setBoundaryTypeMapper(boundaryTypeMapper)
        .build();
  }

  private static Boundary<byte[]> fromBinary(
      PartitionColumn partitionColumn,
      ResultSet resultSet,
      @Nullable BoundaryTypeMapper boundaryTypeMapper)
      throws SQLException {
    Preconditions.checkArgument(partitionColumn.columnClass().equals(BYTE_ARRAY_CLASS));
    resultSet.next();
    byte[] start = resultSet.getBytes(1);
    byte[] end = resultSet.getBytes(2);
    return Boundary.<byte[]>builder()
        .setPartitionColumn(partitionColumn)
        .setStart(start)
        .setEnd(end)
        .setBoundarySplitter(BoundarySplitterFactory.create(BYTE_ARRAY_CLASS))
        .setBoundaryTypeMapper(boundaryTypeMapper)
        .build();
  }

  private static Boundary<String> fromStrings(
      PartitionColumn partitionColumn,
      ResultSet resultSet,
      @Nullable BoundaryTypeMapper boundaryTypeMapper)
      throws SQLException {
    Preconditions.checkArgument(partitionColumn.columnClass().equals(String.class));
    Preconditions.checkArgument(
        boundaryTypeMapper != null,
        "String extractor needs boundaryTypeMapper. PartitionColumn = " + partitionColumn);
    resultSet.next();
    return Boundary.<String>builder()
        .setPartitionColumn(partitionColumn)
        .setStart(resultSet.getString(1))
        .setEnd(resultSet.getString(2))
        .setBoundarySplitter(BoundarySplitterFactory.create(String.class))
        .setBoundaryTypeMapper(boundaryTypeMapper)
        .build();
  }

  private BoundaryExtractorFactory() {}
}
