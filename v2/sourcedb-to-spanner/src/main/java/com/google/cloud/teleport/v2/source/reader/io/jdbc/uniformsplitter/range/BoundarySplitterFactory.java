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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import org.apache.beam.sdk.transforms.DoFn;

/** Factory to construct {@link BoundarySplitter} for supported classes. */
public class BoundarySplitterFactory {
  private static final ImmutableMap<Class, BoundarySplitter<?>> splittermap =
      ImmutableMap.of(
          Integer.class,
          (BoundarySplitter<Integer>)
              (start, end, partitionColumn, boundaryTypeMapper, processContext) ->
                  splitIntegers(start, end),
          Long.class,
          (BoundarySplitter<Long>)
              (start, end, partitionColumn, boundaryTypeMapper, processContext) ->
                  splitLongs(start, end),
          BigInteger.class,
          (BoundarySplitter<BigInteger>)
              (start, end, partitionColumn, boundaryTypeMapper, processContext) ->
                  splitBigIntegers(start, end),
          BigDecimal.class,
          (BoundarySplitter<BigDecimal>)
              (start, end, partitionColumn, boundaryTypeMapper, processContext) ->
                  splitBigDecimal(start, end),
          String.class,
          (BoundarySplitter<String>) BoundarySplitterFactory::splitStrings,
          BYTE_ARRAY_CLASS,
          (BoundarySplitter<byte[]>)
              (start, end, partitionColumn, boundaryTypeMapper, processContext) ->
                  splitBytes(start, end));

  /**
   * Creates {@link BoundarySplitter BoundarySplitter&lt;T&gt;} for pass class {@code c} such that
   * {@code T.getClass == c}.
   *
   * @param c Class for the type of {@link BoundarySplitter}
   * @return {@link BoundarySplitter BoundarySplitter&lt;T&gt;}
   * @throws UnsupportedOperationException if BoundarySplitter is not yet implemented.
   */
  public static <T extends Serializable> BoundarySplitter<T> create(Class c) {

    BoundarySplitter<T> splitter = (BoundarySplitter<T>) splittermap.get(c);
    if (splitter == null) {
      throw new UnsupportedOperationException("Range Splitter not implemented for class " + c);
    }
    return splitter;
  }

  private BoundarySplitterFactory() {}

  private static Integer splitIntegers(Integer start, Integer end) {
    if (start == null && end == null) {
      return null;
    }
    if (start == null) {
      start = Integer.MIN_VALUE;
    }
    if (end == null) {
      end = Integer.MIN_VALUE;
    }

    /*
     * We need to find mid point between start and end which is *consistently* rounded down.
     * There are various ways to do this:
     * 1. (start + end) /2 - This will overflow if (start + end) overflows.
     * 2. (start + (end - start)/2) will overflow if (end - start) overflow, this happens if start is closer to Integer.Min and end closer to Integer.Max.
     * 3. (start + (end/2 - start/2) does not have consistent rounding. For example this will give 11 for 9 and 12 (rounded-up) and give 11 for 10 and 13 too (rounded-down).
     * 4. The method we have used here is free of overflows. Here is how it works
     * 4.1 a + b = (a^b) + (a&b) << 1. bit wise addition is xor of individual bits + carry left shifted by 1.
     * 4.2 therefore, (a+b)/2 = (a&b) + (a^b)>>1. The right side expressions dont have any overflow.
     */
    return (start & end) + ((start ^ end) >> 1);
  }

  private static BigInteger splitBigIntegers(BigInteger start, BigInteger end) {
    BigInteger low;
    BigInteger high;
    if (start == null && end == null) {
      return null;
    }
    if (start == null) {
      start = BigInteger.ZERO;
    }
    if (end == null) {
      end = BigInteger.ZERO;
    }

    if (start.compareTo(end) <= 0) {
      low = start;
      high = end;
    } else {
      high = start;
      low = end;
    }
    return low.add((high.subtract(low)).divide(BigInteger.TWO));
  }

  private static Long splitLongs(Long start, Long end) {
    Long low;
    Long high;

    if (start == null && end == null) {
      return null;
    }
    if (start == null) {
      start = Long.MIN_VALUE;
    }
    if (end == null) {
      end = Long.MIN_VALUE;
    }

    /*
     * We need to find mid point between start and end which is *consistently* rounded down.
     * There are various ways to do this:
     * 1. (start + end) /2 - This will overflow if (start + end) overflows.
     * 2. (start + (end - start)/2) will overflow if (end - start) overflow, this happens if start is closer to Long.Min and end closer to Long.Max.
     * 3. (start + (end/2 - start/2) does not have consistent rounding. For example this will give 11 for 9 and 12 (rounded-up) and give 11 for 10 and 13 too (rounded-down).
     * 4. The method we have used here is free of overflows. Here is how it works
     * 4.1 a + b = (a^b) + (a&b) << 1. bit wise addition is xor of individual bits + carry left shifted by 1.
     * 4.2 therefore, (a+b)/2 = (a&b) + (a^b)>>1. The right side expressions dont have any overflow.
     */
    return (start & end) + ((start ^ end) >> 1);
  }

  private static BigDecimal splitBigDecimal(BigDecimal start, BigDecimal end) {
    BigInteger startBigInt = (start == null) ? null : start.toBigInteger();
    BigInteger endBigInt = (end == null) ? null : end.toBigInteger();
    BigInteger split = splitBigIntegers(startBigInt, endBigInt);
    if (split == null) {
      return null;
    }
    return new BigDecimal(split);
  }

  private static byte[] splitBytes(byte[] start, byte[] end) {
    BigInteger startBigInt = (start == null) ? null : new BigInteger(start);
    BigInteger endBigInt = (end == null) ? null : new BigInteger(end);
    BigInteger split = splitBigIntegers(startBigInt, endBigInt);
    if (split == null) {
      return null;
    }
    return split.toByteArray();
  }

  private static String splitStrings(
      String start,
      String end,
      PartitionColumn partitionColumn,
      BoundaryTypeMapper typeMapper,
      DoFn.ProcessContext c) {
    Preconditions.checkArgument(typeMapper != null, "Trying to split strings without type mapper.");
    Preconditions.checkArgument(
        partitionColumn != null, "Trying to split strings without partition column information.");
    Preconditions.checkArgument(
        partitionColumn.columnClass() == String.class,
        "Trying to split strings for a non-string class.");
    if (start == null && end == null) {
      return null;
    }
    if (start == null) {
      start = "";
    }
    if (end == null) {
      end = "";
    }
    // Ideally no string will be longer than the column's max length, unless a table is altered
    // during a run.
    // To avoid undefined behaviour in the padding logic, we take the max of the input strings and
    // the partition column width.
    int lengthToPad =
        Math.max(
            Math.max(start.length(), end.length()), partitionColumn.stringMaxLength().intValue());
    BigInteger bigIntegerStart =
        (BigInteger) typeMapper.mapStringToBigInteger(start, lengthToPad, partitionColumn, c);
    BigInteger bigIntegerEnd =
        (BigInteger) typeMapper.mapStringToBigInteger(end, lengthToPad, partitionColumn, c);
    BigInteger bigIntegerSplit = splitBigIntegers(bigIntegerStart, bigIntegerEnd);
    return (String) typeMapper.unMapStringFromBigInteger(bigIntegerSplit, partitionColumn, c);
  }
}
