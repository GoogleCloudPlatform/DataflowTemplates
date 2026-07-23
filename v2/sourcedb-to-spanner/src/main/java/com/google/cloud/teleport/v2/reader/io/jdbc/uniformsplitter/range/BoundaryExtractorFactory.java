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

import com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.UniformSplitterDBAdapter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.regex.Pattern;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Factory to construct {@link BoundaryExtractor} for supported {@link class}. */
public class BoundaryExtractorFactory {

  private static final Logger logger = LoggerFactory.getLogger(BoundaryExtractorFactory.class);

  private static final Pattern TIME_PATTERN = Pattern.compile("^\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?$");

  private static final Pattern TIMETZ_PATTERN =
      Pattern.compile("^\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?([+-]\\d{2}(:\\d{2}(:\\d{2})?)?)?$");

  private static final DateTimeFormatter TIMETZ_FORMAT =
      new DateTimeFormatterBuilder()
          .appendPattern("HH:mm:ss")
          .optionalStart()
          .appendFraction(ChronoField.NANO_OF_SECOND, 1, 6, true)
          .optionalEnd()
          .appendOffset("+HH:mm", "+00")
          .toFormatter();

  @FunctionalInterface
  public interface BoundaryDurationExtractor extends Serializable {
    Duration extract(ResultSet rs, int index) throws SQLException;
  }

  public static final Class BYTE_ARRAY_CLASS = (new byte[] {}).getClass();
  private static final ImmutableMap<Class, BoundaryExtractor<?>> extractorMap =
      ImmutableMap.<Class, BoundaryExtractor<?>>builder()
          .put(Integer.class, (BoundaryExtractor<Integer>) BoundaryExtractorFactory::fromIntegers)
          .put(Long.class, (BoundaryExtractor<Long>) BoundaryExtractorFactory::fromLongs)
          .put(String.class, (BoundaryExtractor<String>) BoundaryExtractorFactory::fromStrings)
          .put(
              BigDecimal.class,
              (BoundaryExtractor<BigDecimal>) BoundaryExtractorFactory::fromBigDecimals)
          .put(BYTE_ARRAY_CLASS, (BoundaryExtractor<byte[]>) BoundaryExtractorFactory::fromBinary)
          .put(
              Timestamp.class,
              (BoundaryExtractor<Timestamp>) BoundaryExtractorFactory::fromTimestamps)
          .put(Date.class, (BoundaryExtractor<Date>) BoundaryExtractorFactory::fromDates)
          .put(Float.class, (BoundaryExtractor<Float>) BoundaryExtractorFactory::fromFloats)
          .put(Double.class, (BoundaryExtractor<Double>) BoundaryExtractorFactory::fromDoubles)
          .put(
              LocalTime.class,
              (BoundaryExtractor<LocalTime>) BoundaryExtractorFactory::fromLocalTimes)
          .put(
              OffsetTime.class,
              (BoundaryExtractor<OffsetTime>) BoundaryExtractorFactory::fromOffsetTimes)
          .put(
              Duration.class,
              (BoundaryExtractor<Duration>)
                  (partitionColumn, resultSet, boundaryTypeMapper, tableIdentifier) -> {
                    // Fallback when adapter is not provided: use the default string parsing logic
                    return fromDurations(
                        partitionColumn,
                        resultSet,
                        boundaryTypeMapper,
                        tableIdentifier,
                        (rs, index) -> parseTimeStringToDuration(rs.getString(index)));
                  })
          .build();

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

  /**
   * Create a {@link BoundaryExtractor} for the required class, using dialect-specific extraction
   * logic if available from the {@link UniformSplitterDBAdapter}.
   *
   * @param c class of the column.
   * @param dbAdapter dialect adapter providing custom extraction logic.
   * @return boundary extractor.
   */
  public static <T extends Serializable> BoundaryExtractor<T> create(
      Class<T> c, UniformSplitterDBAdapter dbAdapter) {

    if (c.equals(Duration.class) && dbAdapter != null) {
      BoundaryExtractor<Duration> extractor =
          (partitionColumn, resultSet, boundaryTypeMapper, tableIdentifier) ->
              fromDurations(
                  partitionColumn,
                  resultSet,
                  boundaryTypeMapper,
                  tableIdentifier,
                  dbAdapter::extractBoundaryDuration);
      return (BoundaryExtractor<T>) extractor;
    }
    return create(c);
  }

  private static Boundary<Integer> fromIntegers(
      PartitionColumn partitionColumn,
      ResultSet resultSet,
      @Nullable BoundaryTypeMapper boundaryTypeMapper,
      TableIdentifier tableIdentifier)
      throws SQLException {
    Preconditions.checkArgument(partitionColumn.columnClass().equals(Integer.class));
    resultSet.next();
    return Boundary.<Integer>builder()
        .setTableIdentifier(tableIdentifier)
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
      @Nullable BoundaryTypeMapper boundaryTypeMapper,
      TableIdentifier tableIdentifier)
      throws SQLException {
    Preconditions.checkArgument(partitionColumn.columnClass().equals(Long.class));
    resultSet.next();
    return Boundary.<Long>builder()
        .setTableIdentifier(tableIdentifier)
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
      @Nullable BoundaryTypeMapper boundaryTypeMapper,
      TableIdentifier tableIdentifier)
      throws SQLException {
    Preconditions.checkArgument(partitionColumn.columnClass().equals(BigDecimal.class));
    resultSet.next();
    BigDecimal start = resultSet.getBigDecimal(1);
    BigDecimal end = resultSet.getBigDecimal(2);
    return Boundary.<BigDecimal>builder()
        .setTableIdentifier(tableIdentifier)
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
      @Nullable BoundaryTypeMapper boundaryTypeMapper,
      TableIdentifier tableIdentifier)
      throws SQLException {
    Preconditions.checkArgument(partitionColumn.columnClass().equals(BYTE_ARRAY_CLASS));
    resultSet.next();
    boolean isUuid =
        partitionColumn != null && "uuid".equalsIgnoreCase(partitionColumn.columnTypeName());
    byte[] start = isUuid ? extractUuidBytes(resultSet, 1) : resultSet.getBytes(1);
    byte[] end = isUuid ? extractUuidBytes(resultSet, 2) : resultSet.getBytes(2);
    return Boundary.<byte[]>builder()
        .setTableIdentifier(tableIdentifier)
        .setPartitionColumn(partitionColumn)
        .setStart(start)
        .setEnd(end)
        .setBoundarySplitter(BoundarySplitterFactory.create(BYTE_ARRAY_CLASS))
        .setBoundaryTypeMapper(boundaryTypeMapper)
        .build();
  }

  /**
   * Extracts exactly 16 raw binary bytes from a PostgreSQL UUID column.
   *
   * <p>{@code rs.getBytes()} returns 36 ASCII string bytes instead of 16 raw binary bytes, which
   * corrupts range calculations.
   */
  private static byte[] extractUuidBytes(ResultSet rs, int colIndex) throws SQLException {
    java.util.UUID uuid = rs.getObject(colIndex, java.util.UUID.class);
    if (uuid == null) {
      return null;
    }
    java.nio.ByteBuffer bb = java.nio.ByteBuffer.wrap(new byte[16]);
    bb.putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits());
    return bb.array();
  }

  private static Boundary<String> fromStrings(
      PartitionColumn partitionColumn,
      ResultSet resultSet,
      @Nullable BoundaryTypeMapper boundaryTypeMapper,
      TableIdentifier tableIdentifier)
      throws SQLException {
    Preconditions.checkArgument(partitionColumn.columnClass().equals(String.class));
    boolean isBit = "bit".equalsIgnoreCase(partitionColumn.columnTypeName());

    if (!isBit) {
      Preconditions.checkArgument(
          boundaryTypeMapper != null,
          "String extractor needs boundaryTypeMapper. PartitionColumn = " + partitionColumn);
    }

    resultSet.next();
    return Boundary.<String>builder()
        .setTableIdentifier(tableIdentifier)
        .setPartitionColumn(partitionColumn)
        .setStart(resultSet.getString(1))
        .setEnd(resultSet.getString(2))
        .setBoundarySplitter(
            isBit
                ? BoundarySplitterFactory.createBitSplitter()
                : BoundarySplitterFactory.create(String.class))
        .setBoundaryTypeMapper(boundaryTypeMapper)
        .build();
  }

  private static final Calendar utcCalendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

  private static Boundary<Timestamp> fromTimestamps(
      PartitionColumn partitionColumn,
      ResultSet resultSet,
      @Nullable BoundaryTypeMapper boundaryTypeMapper,
      TableIdentifier tableIdentifier)
      throws SQLException {
    Preconditions.checkArgument(partitionColumn.columnClass().equals(Timestamp.class));
    resultSet.next();
    return Boundary.<Timestamp>builder()
        .setTableIdentifier(tableIdentifier)
        .setPartitionColumn(partitionColumn)
        .setStart(resultSet.getTimestamp(1, utcCalendar))
        .setEnd(resultSet.getTimestamp(2, utcCalendar))
        .setBoundarySplitter(BoundarySplitterFactory.create(Timestamp.class))
        .setBoundaryTypeMapper(boundaryTypeMapper)
        .build();
  }

  private static Boundary<Date> fromDates(
      PartitionColumn partitionColumn,
      ResultSet resultSet,
      @Nullable BoundaryTypeMapper boundaryTypeMapper,
      TableIdentifier tableIdentifier)
      throws SQLException {
    Preconditions.checkArgument(partitionColumn.columnClass().equals(Date.class));
    resultSet.next();
    return Boundary.<Date>builder()
        .setTableIdentifier(tableIdentifier)
        .setPartitionColumn(partitionColumn)
        .setStart(resultSet.getDate(1, utcCalendar))
        .setEnd(resultSet.getDate(2, utcCalendar))
        .setBoundarySplitter(BoundarySplitterFactory.create(Date.class))
        .setBoundaryTypeMapper(boundaryTypeMapper)
        .build();
  }

  private static Boundary<Float> fromFloats(
      PartitionColumn partitionColumn,
      ResultSet resultSet,
      @Nullable BoundaryTypeMapper boundaryTypeMapper,
      TableIdentifier tableIdentifier)
      throws SQLException {
    Preconditions.checkArgument(partitionColumn.columnClass().equals(Float.class));
    resultSet.next();
    return Boundary.<Float>builder()
        .setTableIdentifier(tableIdentifier)
        .setPartitionColumn(partitionColumn)
        .setStart(resultSet.getFloat(1))
        .setEnd(resultSet.getFloat(2))
        .setBoundarySplitter(BoundarySplitterFactory.create(Float.class))
        .setBoundaryTypeMapper(boundaryTypeMapper)
        .build();
  }

  private static Boundary<Double> fromDoubles(
      PartitionColumn partitionColumn,
      ResultSet resultSet,
      @Nullable BoundaryTypeMapper boundaryTypeMapper,
      TableIdentifier tableIdentifier)
      throws SQLException {
    Preconditions.checkArgument(partitionColumn.columnClass().equals(Double.class));
    resultSet.next();
    return Boundary.<Double>builder()
        .setTableIdentifier(tableIdentifier)
        .setPartitionColumn(partitionColumn)
        .setStart(resultSet.getDouble(1))
        .setEnd(resultSet.getDouble(2))
        .setBoundarySplitter(BoundarySplitterFactory.create(Double.class))
        .setBoundaryTypeMapper(boundaryTypeMapper)
        .build();
  }

  private static LocalTime parsePostgresTimeBytes(byte[] bytes) {
    if (bytes == null) {
      return null;
    }

    String textFormat = new String(bytes, StandardCharsets.UTF_8);

    // Text format
    if (TIME_PATTERN.matcher(textFormat).matches()) {
      if (textFormat.startsWith("24:00:00")) {
        return LocalTime.MAX;
      }
      return LocalTime.parse(textFormat);
    } else if (bytes.length == 8) {
      // Binary format
      long microseconds = ByteBuffer.wrap(bytes).getLong();
      if (microseconds == 86400000000L) {
        return LocalTime.MAX;
      }
      return LocalTime.ofNanoOfDay(microseconds * 1000L);
    }

    throw new IllegalArgumentException("Unknown time format received for boundaries");
  }

  private static Boundary<LocalTime> fromLocalTimes(
      PartitionColumn partitionColumn,
      ResultSet resultSet,
      @Nullable BoundaryTypeMapper boundaryTypeMapper,
      TableIdentifier tableIdentifier)
      throws SQLException {
    Preconditions.checkArgument(partitionColumn.columnClass().equals(LocalTime.class));
    resultSet.next();
    return Boundary.<LocalTime>builder()
        .setTableIdentifier(tableIdentifier)
        .setPartitionColumn(partitionColumn)
        .setStart(parsePostgresTimeBytes(resultSet.getBytes(1)))
        .setEnd(parsePostgresTimeBytes(resultSet.getBytes(2)))
        .setBoundarySplitter(BoundarySplitterFactory.create(LocalTime.class))
        .setBoundaryTypeMapper(boundaryTypeMapper)
        .build();
  }

  private static Boundary<OffsetTime> fromOffsetTimes(
      PartitionColumn partitionColumn,
      ResultSet resultSet,
      @Nullable BoundaryTypeMapper boundaryTypeMapper,
      TableIdentifier tableIdentifier)
      throws SQLException {
    Preconditions.checkArgument(partitionColumn.columnClass().equals(OffsetTime.class));
    resultSet.next();

    return Boundary.<OffsetTime>builder()
        .setTableIdentifier(tableIdentifier)
        .setPartitionColumn(partitionColumn)
        .setStart(parsePostgresOffsetTimeBytes(resultSet.getBytes(1)))
        .setEnd(parsePostgresOffsetTimeBytes(resultSet.getBytes(2)))
        .setBoundarySplitter(BoundarySplitterFactory.create(OffsetTime.class))
        .setBoundaryTypeMapper(boundaryTypeMapper)
        .build();
  }

  private static OffsetTime parsePostgresOffsetTimeBytes(byte[] bytes) {
    if (bytes == null) {
      return null;
    }

    String textFormat = new String(bytes, StandardCharsets.UTF_8);

    // Text format
    if (TIMETZ_PATTERN.matcher(textFormat).matches()) {
      if (textFormat.startsWith("24:00:00")) {
        String replacedStr = "00" + textFormat.substring(2);
        OffsetTime parsed = OffsetTime.parse(replacedStr, TIMETZ_FORMAT);
        return OffsetTime.of(LocalTime.MAX, parsed.getOffset());
      }
      return OffsetTime.parse(textFormat, TIMETZ_FORMAT);
    } else if (bytes.length == 12) {
      // Binary format
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      long microseconds = buffer.getLong();
      int offsetSeconds = buffer.getInt();

      // PostgreSQL stores timezone offset inverted (West of UTC is positive).
      ZoneOffset offset = ZoneOffset.ofTotalSeconds(-offsetSeconds);

      if (microseconds == 86400000000L) {
        return OffsetTime.of(LocalTime.MAX, offset);
      }
      return OffsetTime.of(LocalTime.ofNanoOfDay(microseconds * 1000L), offset);
    }

    throw new IllegalArgumentException("Unknown TIMETZ format received for boundaries");
  }

  private static Boundary<Duration> fromDurations(
      PartitionColumn partitionColumn,
      ResultSet resultSet,
      @Nullable BoundaryTypeMapper boundaryTypeMapper,
      TableIdentifier tableIdentifier,
      BoundaryDurationExtractor durationExtractor)
      throws SQLException {
    Preconditions.checkArgument(partitionColumn.columnClass().equals(Duration.class));
    resultSet.next();
    return Boundary.<Duration>builder()
        .setTableIdentifier(tableIdentifier)
        .setPartitionColumn(partitionColumn)
        .setStart(durationExtractor.extract(resultSet, 1))
        .setEnd(durationExtractor.extract(resultSet, 2))
        .setBoundarySplitter(BoundarySplitterFactory.create(Duration.class))
        .setBoundaryTypeMapper(boundaryTypeMapper)
        .build();
  }

  /**
   * Converts a string in format "hh:mm:ss.sss" into a Duration by converting to a string with
   * format "PThhHmmMss.sssS".
   */
  @VisibleForTesting
  public static Duration parseTimeStringToDuration(String timeString) {
    if (timeString == null || timeString.isBlank()) {
      return null;
    }
    boolean isNegative = timeString.trim().startsWith("-");
    String[] parts = timeString.trim().split(":");
    StringBuilder durationStrBuilder = new StringBuilder("PT");
    durationStrBuilder.append(parts[0]).append("H");
    if (parts.length > 1) {
      if (isNegative) {
        durationStrBuilder.append("-");
      }
      durationStrBuilder.append(parts[1]).append("M");
    }
    if (parts.length > 2) {
      if (isNegative) {
        durationStrBuilder.append("-");
      }
      durationStrBuilder.append(parts[2]).append("S");
    }
    return Duration.parse(durationStrBuilder.toString());
  }

  private BoundaryExtractorFactory() {}
}
