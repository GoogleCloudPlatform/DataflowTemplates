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

import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationMapper;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationReference;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link BoundarySplitterFactory}. */
@RunWith(MockitoJUnitRunner.class)
public class BoundarySplitterFactoryTest {
  @Test
  public void testLongBoundarySplitter() {
    BoundarySplitter<Long> splitter = BoundarySplitterFactory.create(Long.class);

    assertThat(splitter.getSplitPoint(Long.MAX_VALUE - 2, Long.MAX_VALUE, null, null, null))
        .isEqualTo(Long.MAX_VALUE - 1);
    assertThat(splitter.getSplitPoint(Long.MIN_VALUE, Long.MIN_VALUE + 4, null, null, null))
        .isEqualTo(Long.MIN_VALUE + 2);
    assertThat(splitter.getSplitPoint(Long.MIN_VALUE, Long.MAX_VALUE, null, null, null))
        .isEqualTo(-1L);
    assertThat(splitter.getSplitPoint(10L, 0L, null, null, null)).isEqualTo(5L);
    assertThat(splitter.getSplitPoint(null, null, null, null, null)).isNull();
    assertThat(splitter.getSplitPoint(Long.MIN_VALUE + 2L, null, null, null, null))
        .isEqualTo(Long.MIN_VALUE + 1);
    assertThat(splitter.getSplitPoint(null, Long.MIN_VALUE, null, null, null))
        .isEqualTo(Long.MIN_VALUE);
  }

  @Test
  public void testIntegerBoundarySplitter() {
    BoundarySplitter<Integer> splitter = BoundarySplitterFactory.create(Integer.class);

    assertThat(splitter.getSplitPoint(Integer.MAX_VALUE - 2, Integer.MAX_VALUE, null, null, null))
        .isEqualTo(Integer.MAX_VALUE - 1);
    assertThat(splitter.getSplitPoint(Integer.MIN_VALUE, Integer.MIN_VALUE + 4, null, null, null))
        .isEqualTo(Integer.MIN_VALUE + 2);
    assertThat(splitter.getSplitPoint(Integer.MIN_VALUE, Integer.MAX_VALUE, null, null, null))
        .isEqualTo(-1);
    assertThat(splitter.getSplitPoint(10, 0, null, null, null)).isEqualTo(5);
    assertThat(splitter.getSplitPoint(null, null, null, null, null)).isNull();
    assertThat(splitter.getSplitPoint(Integer.MIN_VALUE + 1, null, null, null, null))
        .isEqualTo(Integer.MIN_VALUE);
    assertThat(splitter.getSplitPoint(null, Integer.MIN_VALUE + 1, null, null, null))
        .isEqualTo(Integer.MIN_VALUE);
  }

  @Test
  public void testBigIntegerBoundarySplitter() {
    BoundarySplitter<BigInteger> splitter = BoundarySplitterFactory.create(BigInteger.class);
    BigInteger start = BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(10L));
    BigInteger startByTwo = BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(5L));
    BigInteger end = BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(20L));
    BigInteger mid = BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(15L));

    assertThat(splitter.getSplitPoint(start, end, null, null, null)).isEqualTo(mid);
    assertThat(splitter.getSplitPoint(start, BigInteger.valueOf(0L), null, null, null))
        .isEqualTo(startByTwo);
    assertThat(
            splitter.getSplitPoint(
                BigInteger.valueOf(Long.MIN_VALUE),
                BigInteger.valueOf(Long.MAX_VALUE),
                null,
                null,
                null))
        .isEqualTo(BigInteger.valueOf(-1L));
    assertThat(splitter.getSplitPoint(null, null, null, null, null)).isNull();
    assertThat(splitter.getSplitPoint(BigInteger.valueOf(42L), null, null, null, null))
        .isEqualTo(BigInteger.valueOf(21L));
    assertThat(splitter.getSplitPoint(null, BigInteger.valueOf(42L), null, null, null))
        .isEqualTo(BigInteger.valueOf(21L));
  }

  @Test
  public void testBigDecimalBoundarySplitter() {
    BoundarySplitter<BigDecimal> splitter = BoundarySplitterFactory.create(BigDecimal.class);
    BigDecimal start =
        new BigDecimal(BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(10L)));
    BigDecimal startByTwo =
        new BigDecimal(BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(5L)));
    BigDecimal end =
        new BigDecimal(BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(20L)));
    BigDecimal mid =
        new BigDecimal(BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(15L)));
    BigDecimal zero = new BigDecimal(BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.ZERO));
    BigDecimal negOne = new BigDecimal(BigInteger.valueOf(-1L));
    BigDecimal longMax = new BigDecimal(BigInteger.valueOf(Long.MAX_VALUE));
    BigDecimal longMin = new BigDecimal(BigInteger.valueOf(Long.MIN_VALUE));
    BigDecimal fortyTwo = new BigDecimal(BigInteger.valueOf(42L));
    BigDecimal twentyOne = new BigDecimal(BigInteger.valueOf(21L));

    assertThat(splitter.getSplitPoint(start, end, null, null, null)).isEqualTo(mid);
    assertThat(splitter.getSplitPoint(start, zero, null, null, null)).isEqualTo(startByTwo);
    assertThat(splitter.getSplitPoint(longMin, longMax, null, null, null)).isEqualTo(negOne);
    assertThat(splitter.getSplitPoint(null, null, null, null, null)).isNull();
    assertThat(splitter.getSplitPoint(fortyTwo, null, null, null, null)).isEqualTo(twentyOne);
    assertThat(splitter.getSplitPoint(null, fortyTwo, null, null, null)).isEqualTo(twentyOne);
  }

  @Test
  public void testBytesIntegerBoundarySplitter() {
    BoundarySplitter<byte[]> splitter = BoundarySplitterFactory.create(BYTE_ARRAY_CLASS);
    byte[] start =
        BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(10L)).toByteArray();
    byte[] startByTwo =
        BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(5L)).toByteArray();
    byte[] end = BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(20L)).toByteArray();
    byte[] mid = BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(15L)).toByteArray();
    byte[] zero = BigInteger.ZERO.toByteArray();
    byte[] negOne = BigInteger.valueOf(-1L).toByteArray();
    byte[] longMax = BigInteger.valueOf(Long.MAX_VALUE).toByteArray();
    byte[] longMin = BigInteger.valueOf(Long.MIN_VALUE).toByteArray();
    byte[] fortyTwo = BigInteger.valueOf(42L).toByteArray();
    byte[] twentyOne = BigInteger.valueOf(21L).toByteArray();

    assertThat(splitter.getSplitPoint(start, end, null, null, null)).isEqualTo(mid);
    assertThat(splitter.getSplitPoint(start, zero, null, null, null)).isEqualTo(startByTwo);
    assertThat(splitter.getSplitPoint(longMax, longMin, null, null, null)).isEqualTo(negOne);
    assertThat(splitter.getSplitPoint(null, null, null, null, null)).isNull();
    assertThat(splitter.getSplitPoint(fortyTwo, null, null, null, null)).isEqualTo(twentyOne);
    assertThat(splitter.getSplitPoint(null, fortyTwo, null, null, null)).isEqualTo(twentyOne);
  }

  @Test
  public void testStringBoundarySplitter() {

    BoundaryTypeMapper mapper = new TestBoundaryTypeMapper();
    BoundarySplitter<String> splitter = BoundarySplitterFactory.create(String.class);
    StringBuilder repeatedA = new StringBuilder();
    StringBuilder repeatedAvg = new StringBuilder();
    StringBuilder longPaddedAvg = new StringBuilder();
    StringBuilder repeatedZ = new StringBuilder();
    for (int i = 0; i < 255; i++) {
      repeatedA.append('a');
      if (i == 0) {
        repeatedAvg.append('m');
        longPaddedAvg.append('m');
      } else {
        // The average of zz and aa is mz, just like average of 99 and 00 is 49.
        repeatedAvg.append('z');
        longPaddedAvg.append('a');
      }
      repeatedZ.append('z');
    }

    PartitionColumn partitionColumn =
        PartitionColumn.builder()
            .setColumnName("col1")
            .setColumnClass(String.class)
            .setStringCollation(
                CollationReference.builder()
                    .setDbCharacterSet("latin1")
                    .setDbCollation("latin1_general_cs")
                    .setPadSpace(true)
                    .build())
            .setStringMaxLength(255)
            .build();

    // Same Length
    assertThat(
            splitter.getSplitPoint(
                "Spanner",
                "branner",
                partitionColumn.toBuilder().setStringMaxLength(7).build(),
                mapper,
                null))
        .isEqualTo("kdanner");
    // Variable Length
    assertThat(
            splitter.getSplitPoint(
                "cat",
                "mouse",
                partitionColumn.toBuilder().setStringMaxLength(5).build(),
                mapper,
                null))
        .isEqualTo("hhtwc");
    // Null and empty
    assertThat(splitter.getSplitPoint(null, null, partitionColumn, mapper, null)).isNull();
    assertThat(
            splitter.getSplitPoint(
                null, "z", partitionColumn.toBuilder().setStringMaxLength(1).build(), mapper, null))
        .isEqualTo("m");
    assertThat(
            splitter.getSplitPoint(
                "z", null, partitionColumn.toBuilder().setStringMaxLength(1).build(), mapper, null))
        .isEqualTo("m");
    assertThat(
            splitter.getSplitPoint(
                "z", "", partitionColumn.toBuilder().setStringMaxLength(1).build(), mapper, null))
        .isEqualTo("m");
    assertThat(
            splitter.getSplitPoint(
                "", "z", partitionColumn.toBuilder().setStringMaxLength(1).build(), mapper, null))
        .isEqualTo("m");

    // Long string
    assertThat(
            splitter.getSplitPoint(
                repeatedA.toString(),
                repeatedZ.toString(),
                partitionColumn.toBuilder().setStringMaxLength(255).build(),
                mapper,
                null))
        .isEqualTo(repeatedAvg.toString());
    assertThat(
            splitter.getSplitPoint(
                "a",
                "y",
                partitionColumn.toBuilder().setStringMaxLength(255).build(),
                mapper,
                null))
        .isEqualTo(longPaddedAvg.toString());

    // Null Checks.
    assertThrows(
        IllegalArgumentException.class,
        () -> splitter.getSplitPoint("Spanner", "branner", null, mapper, null));
    assertThrows(
        IllegalArgumentException.class,
        () -> splitter.getSplitPoint("Spanner", "branner", partitionColumn, null, null));
    // Mismatched types.
    assertThrows(
        IllegalArgumentException.class,
        () ->
            splitter.getSplitPoint(
                "Spanner",
                "branner",
                PartitionColumn.builder()
                    .setColumnName("intcol")
                    .setColumnClass(Integer.class)
                    .autoBuild(),
                mapper,
                null));
  }

  @Test
  public void testBoundarySplitterFactoryExceptions() {
    assertThrows(
        UnsupportedOperationException.class,
        () -> BoundarySplitterFactory.create(GenericObjectPool.class));
  }

  /* Not for production as it does not look at collation ordering */
  private class TestBoundaryTypeMapper implements BoundaryTypeMapper {

    private static final long CHARACTER_SET_SIZE = 26L;

    private long getOrdinalPosition(char c) {
      return Character.toLowerCase(c) - 'a';
    }

    private char getCharacterFromPosition(long position) {
      return (char) ('a' + position);
    }

    @Override
    public BigInteger mapStringToBigInteger(
        String element,
        int lengthToPad,
        PartitionColumn partitionColumn,
        ProcessContext processContext) {
      BigInteger ret = BigInteger.ZERO;
      for (char c : element.toCharArray()) {
        ret =
            ret.multiply(BigInteger.valueOf(CHARACTER_SET_SIZE))
                .add(BigInteger.valueOf(getOrdinalPosition(c)));
      }
      for (int i = element.length(); i < lengthToPad; i++) {
        ret = ret.multiply(BigInteger.valueOf(CHARACTER_SET_SIZE));
      }
      return ret;
    }

    @Override
    public String unMapStringFromBigInteger(
        BigInteger element, PartitionColumn partitionColumn, ProcessContext processContext) {
      StringBuilder word = new StringBuilder();
      while (element != BigInteger.ZERO) {
        BigInteger reminder = element.mod(BigInteger.valueOf(CHARACTER_SET_SIZE));
        char c = getCharacterFromPosition(reminder.longValue());
        word.append(c);
        element = element.divide(BigInteger.valueOf(CHARACTER_SET_SIZE));
      }
      String ret = word.reverse().toString();
      return ret;
    }

    @Override
    public PCollectionView<Map<CollationReference, CollationMapper>> getCollationMapperView() {
      return null;
    }
  }
}
