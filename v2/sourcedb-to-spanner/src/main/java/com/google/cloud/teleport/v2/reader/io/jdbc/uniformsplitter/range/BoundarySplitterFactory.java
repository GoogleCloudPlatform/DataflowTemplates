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
package com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.range;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import org.apache.beam.sdk.transforms.DoFn;

/** Factory to construct {@link BoundarySplitter} for supported classes. */
public class BoundarySplitterFactory {

  private static final BigInteger SECONDS_TO_NANOS =
      BigInteger.valueOf(Duration.ofSeconds(1).toNanos());

  private static final ImmutableMap<Class, BoundarySplitter<?>> splittermap =
      ImmutableMap.<Class, BoundarySplitter<?>>builder()
          .put(
              Integer.class,
              (BoundarySplitter<Integer>)
                  (start, end, partitionColumn, boundaryTypeMapper, processContext) ->
                      splitIntegers(start, end))
          .put(
              Long.class,
              (BoundarySplitter<Long>)
                  (start, end, partitionColumn, boundaryTypeMapper, processContext) ->
                      splitLongs(start, end))
          .put(
              BigInteger.class,
              (BoundarySplitter<BigInteger>)
                  (start, end, partitionColumn, boundaryTypeMapper, processContext) ->
                      splitBigIntegers(start, end))
          .put(
              BigDecimal.class,
              (BoundarySplitter<BigDecimal>)
                  (start, end, partitionColumn, boundaryTypeMapper, processContext) ->
                      splitBigDecimals(start, end, partitionColumn))
          .put(
              LocalTime.class,
              (BoundarySplitter<LocalTime>)
                  (start, end, partitionColumn, boundaryTypeMapper, processContext) ->
                      splitLocalTimes(start, end))
          .put(
              OffsetTime.class,
              (BoundarySplitter<OffsetTime>)
                  (start, end, partitionColumn, boundaryTypeMapper, processContext) ->
                      splitOffsetTimes(start, end))
          .put(String.class, (BoundarySplitter<String>) BoundarySplitterFactory::splitStrings)
          .put(
              BoundaryExtractorFactory.BYTE_ARRAY_CLASS,
              (BoundarySplitter<byte[]>)
                  (start, end, partitionColumn, boundaryTypeMapper, processContext) ->
                      splitBytes(start, end))
          .put(
              Timestamp.class,
              (BoundarySplitter<Timestamp>)
                  (start, end, partitionColumn, boundaryTypeMapper, processContext) ->
                      splitTimestamps(start, end))
          .put(
              Date.class,
              (BoundarySplitter<Date>)
                  (start, end, partitionColumn, boundaryTypeMapper, processContext) ->
                      splitDates(start, end))
          .put(
              Float.class,
              (BoundarySplitter<Float>)
                  (start, end, partitionColumn, boundaryTypeMapper, processContext) ->
                      splitFloats(start, end))
          .put(
              Double.class,
              (BoundarySplitter<Double>)
                  (start, end, partitionColumn, boundaryTypeMapper, processContext) ->
                      splitDoubles(start, end))
          .put(
              Duration.class,
              (BoundarySplitter<Duration>)
                  (start, end, partitionColumn, boundaryTypeMapper, processContext) ->
                      splitDurations(start, end, partitionColumn))
          .build();

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

  public static BoundarySplitter<String> createBitSplitter() {
    return (start, end, partitionColumn, boundaryTypeMapper, processContext) -> {
      if (start == null && end == null) {
        return null;
      }
      BigInteger startInt = start == null ? null : new BigInteger(start, 2);
      BigInteger endInt = end == null ? null : new BigInteger(end, 2);

      BigInteger midpoint = splitBigIntegers(startInt, endInt);

      String midString = midpoint.toString(2);

      int targetLength = start != null ? start.length() : (end != null ? end.length() : 0);
      midString = Strings.padStart(midString, targetLength, '0');

      return midString;
    };
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

  private static LocalTime splitLocalTimes(LocalTime start, LocalTime end) {
    if (start == null && end == null) {
      return null;
    }
    if (start == null) {
      start = LocalTime.MIN;
    }
    if (end == null) {
      end = LocalTime.MAX;
    }

    Long midNanos = splitLongs(start.toNanoOfDay(), end.toNanoOfDay());
    if (midNanos == null) {
      return null;
    }
    return LocalTime.ofNanoOfDay(midNanos);
  }

  private static OffsetTime splitOffsetTimes(OffsetTime start, OffsetTime end) {
    if (start == null && end == null) {
      return null;
    }
    if (start == null) {
      start = OffsetTime.of(LocalTime.MIN, end.getOffset());
    }
    if (end == null) {
      end = OffsetTime.of(LocalTime.MAX, start.getOffset());
    }

    long startNanos = start.withOffsetSameInstant(ZoneOffset.UTC).toLocalTime().toNanoOfDay();
    long endNanos = end.withOffsetSameInstant(ZoneOffset.UTC).toLocalTime().toNanoOfDay();

    long nanosPerDay = 86_400_000_000_000L;
    if (endNanos < startNanos) {
      endNanos += nanosPerDay;
    }

    Long midNanos = splitLongs(startNanos, endNanos);
    if (midNanos == null) {
      return null;
    }

    midNanos = midNanos % nanosPerDay;

    OffsetTime currentEnd =
        OffsetTime.of(LocalTime.ofNanoOfDay(midNanos), ZoneOffset.UTC)
            .withOffsetSameInstant(start.getOffset());

    return currentEnd;
  }

  @VisibleForTesting
  protected static BigDecimal splitBigDecimals(
      BigDecimal start, BigDecimal end, PartitionColumn partitionColumn) {
    Preconditions.checkNotNull(
        partitionColumn, "Trying to split BigDecimals without partition column information.");
    Preconditions.checkNotNull(
        partitionColumn.numericScale(), "Trying to split BigDecimals without numeric scale.");
    int scale = partitionColumn.numericScale();
    BigInteger startBigInt = bigDecimalToBigInt(start, scale);
    BigInteger endBigInt = bigDecimalToBigInt(end, scale);

    BigInteger split = splitBigIntegers(startBigInt, endBigInt);
    if (split == null) {
      return null;
    }
    return new BigDecimal(split, scale);
  }

  private static BigInteger bigDecimalToBigInt(BigDecimal value, int scale) {
    return value == null ? null : value.setScale(scale, RoundingMode.UNNECESSARY).unscaledValue();
  }

  private static Date splitDates(Date start, Date end) {
    if (start == null && end == null) {
      return null;
    }
    if (start == null) {
      start = Date.valueOf(LocalDate.MIN);
    }
    if (end == null) {
      end = Date.valueOf(LocalDate.MIN);
    }

    long startDateLong = convertDateToLong(start);
    long endDateLong = convertDateToLong(end);

    long dateMid = splitLongs(startDateLong, endDateLong);

    return convertLongToSqlDate(dateMid);
  }

  private static long convertDateToLong(Date sqlDate) {
    return sqlDate.toLocalDate().toEpochDay();
  }

  private static Date convertLongToSqlDate(long dateLong) {
    return Date.valueOf(LocalDate.ofEpochDay(dateLong));
  }

  private static byte[] splitBytes(byte[] start, byte[] end) {
    if (start == null && end == null) {
      return null;
    }

    BigInteger startBigInt = (start == null) ? null : new BigInteger(1, start);
    BigInteger endBigInt = (end == null) ? null : new BigInteger(1, end);
    BigInteger split = splitBigIntegers(startBigInt, endBigInt);
    if (split == null) {
      return null;
    }

    if (start != null && end != null && start.length == end.length) {
      return toFixedLengthByteArray(split, start.length);
    }
    return split.toByteArray();
  }

  /**
   * Formats a {@link BigInteger} into a byte array of exactly {@code expectedLength}, handling
   * two's complement serialization edge cases (leading sign-byte truncation for values >= 0x80...
   * and zero-padding for small values) to maintain precise byte alignment.
   */
  private static byte[] toFixedLengthByteArray(BigInteger bigInt, int expectedLength) {
    final byte[] array = bigInt.toByteArray();
    if (array.length == expectedLength) {
      return array;
    }
    if (array.length > expectedLength) {
      return truncateLeadingSignByte(array, expectedLength);
    }
    return padLeadingZeroBytes(array, expectedLength);
  }

  /**
   * When BigInteger returns a positive number, it adds an extra leading 0x00 byte for values >=
   * 0x80... to ensure that the most significant bit is 0 for positive numbers. This method removes
   * that extra leading 0x00 byte to maintain precise byte alignment.
   */
  private static byte[] truncateLeadingSignByte(byte[] array, int expectedLength) {
    final byte[] result = new byte[expectedLength];
    System.arraycopy(array, array.length - expectedLength, result, 0, expectedLength);
    return result;
  }

  /**
   * BigInteger.toByteArray() strips leading zero bytes from positive numbers, returning arrays
   * shorter than expected for small values. This method prepends leading 0x00 bytes to reconstruct
   * the full expected byte length (e.g., 16 bytes for UUIDs).
   */
  private static byte[] padLeadingZeroBytes(byte[] array, int expectedLength) {
    final byte[] result = new byte[expectedLength];
    System.arraycopy(array, 0, result, expectedLength - array.length, array.length);
    return result;
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

  @VisibleForTesting
  protected static BigInteger instantToBigIntNanos(Instant instant) {
    if (instant == null) {
      return null;
    }
    BigInteger seconds = BigInteger.valueOf(instant.getEpochSecond());
    BigInteger nanos = BigInteger.valueOf(instant.getNano());
    return seconds.multiply(SECONDS_TO_NANOS).add(nanos);
  }

  @VisibleForTesting
  protected static Instant bigIntNanosToInstant(BigInteger nanos) {
    if (nanos == null) {
      return null;
    }
    BigInteger[] quotientReminder = nanos.divideAndRemainder(SECONDS_TO_NANOS);
    long seconds = quotientReminder[0].longValueExact();
    long nanoSeconds = quotientReminder[1].longValueExact();
    return Instant.ofEpochSecond(seconds, nanoSeconds);
  }

  @VisibleForTesting
  protected static Instant splitInstants(Instant start, Instant end) {
    if ((start == null) && (end == null)) {
      return null;
    }
    BigInteger bigIntegerStart = instantToBigIntNanos(start);
    BigInteger bigIntegerEnd = instantToBigIntNanos(end);
    BigInteger bigIntegerMid = splitBigIntegers(bigIntegerStart, bigIntegerEnd);
    return bigIntNanosToInstant(bigIntegerMid);
  }

  @VisibleForTesting
  protected static Instant timeStampToInstant(Timestamp timestamp) {
    return (timestamp == null) ? null : timestamp.toInstant();
  }

  @VisibleForTesting
  protected static Timestamp instantToTimestamp(Instant instant) {
    return (instant == null) ? null : Timestamp.from(instant);
  }

  private static Timestamp splitTimestamps(Timestamp start, Timestamp end) {
    return instantToTimestamp(splitInstants(timeStampToInstant(start), timeStampToInstant(end)));
  }

  private static Float splitFloats(Float start, Float end) {
    if (start == null && end == null) {
      return null;
    }
    if (start == null) {
      start = -Float.MAX_VALUE;
    }
    if (end == null) {
      end = Float.MAX_VALUE;
    }

    // Calculate overflow safe mid-point

    // If signs are different, simple addition is safe from overflow
    // because the values cancel each other out towards zero.
    if ((start < 0 && end > 0) || (start > 0 && end < 0)) {
      return (start + end) / 2.0f;
    }

    // If signs are the same (both positive or both negative),
    // we use the offset formula to prevent overflow (Infinity).
    // This works regardless of whether start > end or start < end.
    return start + (end - start) / 2.0f;
  }

  private static Double splitDoubles(Double start, Double end) {
    if (start == null && end == null) {
      return null;
    }
    if (start == null) {
      start = -Double.MAX_VALUE;
    }
    if (end == null) {
      end = Double.MAX_VALUE;
    }

    // Calculate overflow safe mid-point

    // If signs are different, simple addition is safe from overflow
    // because the values cancel each other out towards zero.
    if ((start < 0 && end > 0) || (start > 0 && end < 0)) {
      return (start + end) / 2.0;
    }

    // If signs are the same (both positive or both negative),
    // we use the offset formula to prevent overflow (Infinity).
    // This works regardless of whether start > end or start < end.
    return start + (end - start) / 2.0;
  }

  @VisibleForTesting
  protected static Duration splitDurations(
      Duration start, Duration end, PartitionColumn partitionColumn) {
    Preconditions.checkNotNull(
        partitionColumn, "Trying to split Durations without partition column information.");
    Preconditions.checkNotNull(
        partitionColumn.datetimePrecision(),
        "Trying to split Durations without datetime precision.");
    int precision = partitionColumn.datetimePrecision();
    BigInteger startBigInt = durationToBigInteger(start, precision);
    BigInteger endBigInt = durationToBigInteger(end, precision);
    BigInteger split = splitBigIntegers(startBigInt, endBigInt);
    return bigIntegerToDuration(split, precision);
  }

  private static BigInteger durationToBigInteger(Duration duration, int precision) {
    if (duration == null) {
      return null;
    }
    BigInteger seconds = BigInteger.valueOf(duration.getSeconds());
    BigInteger nanos = BigInteger.valueOf(duration.getNano());
    return seconds
        .multiply(BigInteger.TEN.pow(precision))
        .add(nanos.divide(BigInteger.TEN.pow(9 - precision)));
  }

  private static Duration bigIntegerToDuration(BigInteger bigInt, int precision) {
    if (bigInt == null) {
      return null;
    }
    BigInteger[] quotientAndRemainder = bigInt.divideAndRemainder(BigInteger.TEN.pow(precision));
    BigInteger seconds = quotientAndRemainder[0];
    BigInteger nanos = quotientAndRemainder[1].multiply(BigInteger.TEN.pow(9 - precision));
    return Duration.ofSeconds(seconds.longValueExact(), nanos.longValueExact());
  }
}
