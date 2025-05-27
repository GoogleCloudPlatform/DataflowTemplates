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
package com.google.cloud.teleport.v2.templates.dbutils.dml;

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnDefinition;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerColumnDefinition;
import com.google.cloud.teleport.v2.templates.models.PreparedStatementValueObject;
import com.google.common.net.InetAddresses;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.lang3.BooleanUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraTypeHandler {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraTypeHandler.class);

  /**
   * A singleton class representing a null or empty state.
   *
   * <p>This class cannot be instantiated directly, and its single instance is accessed via the
   * {@link #INSTANCE} field. It provides a custom {@link #toString()} implementation that returns
   * the string representation "NULL_CLASS". This can be used to signify a special state where an
   * object is not present or explicitly set to null.
   */
  public static final class NullClass {

    /**
     * Private constructor to prevent instantiation of the NULL_CLASS.
     *
     * <p>This ensures that only one instance of the NULL_CLASS exists, following the singleton
     * pattern.
     */
    private NullClass() {}

    /**
     * The singleton instance of the NULL_CLASS.
     *
     * <p>This instance can be accessed statically via this field to represent a "null" or empty
     * value in various contexts.
     */
    public static final NullClass INSTANCE = new NullClass();

    /**
     * Returns the string representation of the NULL_CLASS instance.
     *
     * @return the string "NULL_CLASS"
     */
    @Override
    public String toString() {
      return "NULL_CLASS";
    }
  }

  @FunctionalInterface
  private interface HandlerSupplier<T> {

    /**
     * Supplies a value of type {@code T}.
     *
     * @return A value of type {@code T}.
     * @throws Exception If an error occurs while supplying the value.
     */
    T get() throws Exception;
  }

  /**
   * Converts the provided {@link Object} value to a {@link BigInteger} representing a Cassandra
   * varint.
   *
   * <p>This method checks the type of the provided {@code value}. If it is a string, it tries to
   * convert it to a {@link BigInteger}. If it is a byte array, it interprets it as a varint and
   * converts it to a {@link BigInteger}. If the value is a {@link ByteBuffer}, it converts the
   * content of the buffer to a byte array and then to a {@link BigInteger}. If the value is neither
   * a valid number string, byte array, nor a {@link ByteBuffer}, it throws an {@link
   * IllegalArgumentException}.
   *
   * @param value The value to be converted to a {@link BigInteger}. This could either be a string
   *     representing a number, a byte array representing a varint, or a {@link ByteBuffer}.
   * @return A {@link BigInteger} object representing the varint value.
   * @throws IllegalArgumentException If the value is neither a valid number string, byte array, nor
   *     a valid {@link ByteBuffer} for varint representation.
   */
  private static BigInteger handleCassandraVarintType(Object value) {
    if (value instanceof byte[]) {
      return new BigInteger((byte[]) value);
    } else if (value instanceof ByteBuffer) {
      return new BigInteger(((ByteBuffer) value).array());
    }
    return new BigInteger(value.toString());
  }

  /**
   * Generates a {@link CqlDuration} based on the provided {@link CassandraTypeHandler}.
   *
   * <p>This method fetches a string value from the provided {@code valuesJson} object using the
   * column name {@code colName}, and converts it into a {@link CqlDuration} object. The string
   * value should be in the ISO-8601 duration format (e.g., "PT20.345S").
   *
   * @param durationString - The column value used to fetched from {@code valuesJson}.
   * @return A {@link CqlDuration} object representing the duration value from the Cassandra data.
   * @throws IllegalArgumentException if the value is not a valid duration string.
   */
  private static CqlDuration handleCassandraDurationType(String durationString) {
    try {
      return CqlDuration.from(durationString);
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid duration format for: " + durationString, e);
    }
  }

  /**
   * Generates a Type based on the provided {@link CassandraTypeHandler}.
   *
   * @param inetString - which is used to generate InetAddress.
   * @return a {@link InetAddress} object containing InetAddress as value represented in cassandra
   *     type.
   */
  private static InetAddress handleCassandraInetAddressType(String inetString) {
    try {
      return InetAddresses.forString(inetString);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid IP address format for: " + inetString, e);
    }
  }

  /**
   * Generates a Type based on the provided {@link CassandraTypeHandler}.
   *
   * @param colValue - contains all the key value for current incoming stream.
   * @return a {@link ByteBuffer} object containing the value represented in cassandra type.
   */
  private static ByteBuffer parseBlobType(Object colValue) {
    if (colValue instanceof byte[]) {
      return ByteBuffer.wrap((byte[]) colValue);
    } else if (colValue instanceof ByteBuffer) {
      return (ByteBuffer) colValue;
    } else {
      return ByteBuffer.wrap(java.util.Base64.getDecoder().decode((String) colValue));
    }
  }

  /**
   * Parses a timestamp value from a JSON object and returns it as an {@link Instant} in UTC.
   *
   * <p>This method extracts a timestamp value associated with the given column name from the
   * provided {@link JSONObject}. The timestamp is expected to be in an ISO-8601 compatible format
   * (e.g., "yyyy-MM-dd'T'HH:mm:ss.SSSZ"). The method ensures that the returned {@link Instant} is
   * always in UTC, regardless of the time zone present in the input.
   *
   * <p>If the input timestamp cannot be parsed directly as an {@link Instant}, the method attempts
   * to parse it as a {@link ZonedDateTime} and normalizes it to UTC before converting it to an
   * {@link Instant}. If parsing fails, an {@link IllegalArgumentException} is thrown.
   *
   * <p>This method is particularly useful for processing timestamp data stored in Cassandra, where
   * timestamps are often stored as ISO-8601 strings.
   *
   * @param timestampValue the used to parse the Instant.
   * @return an {@link Instant} representing the parsed timestamp value in UTC.
   * @throws IllegalArgumentException if the column value is missing, empty, or cannot be parsed as
   *     a valid timestamp.
   */
  private static Instant handleCassandraTimestampType(String timestampValue) {
    if (timestampValue == null || timestampValue.isEmpty()) {
      throw new IllegalArgumentException(
          "Timestamp value for " + timestampValue + " is null or empty.");
    }
    return convertToCassandraTimestamp(timestampValue);
  }

  /**
   * Generates a Type based on the provided {@link CassandraTypeHandler}.
   *
   * @param uuidString - which is used to parsed and return UUID.
   * @return a {@link UUID} object containing UUID as value represented in cassandra type.
   */
  private static UUID handleCassandraUuidType(String uuidString) {
    if (uuidString == null) {
      return null;
    }
    return UUID.fromString(uuidString);
  }

  private static Instant convertToCassandraTimestamp(String timestampValue) {
    if (timestampValue == null || timestampValue.trim().isEmpty()) {
      throw new IllegalArgumentException("Timestamp value cannot be null or empty");
    }

    List<DateTimeFormatter> formatters =
        Arrays.asList(
            DateTimeFormatter.ISO_INSTANT,
            DateTimeFormatter.ISO_DATE_TIME,
            DateTimeFormatter.ISO_LOCAL_DATE,
            DateTimeFormatter.ISO_TIME,
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
            DateTimeFormatter.ofPattern("MM/dd/yyyy"),
            DateTimeFormatter.ofPattern("yyyy/MM/dd"),
            DateTimeFormatter.ofPattern("dd-MM-yyyy"),
            DateTimeFormatter.ofPattern("dd/MM/yyyy"),
            DateTimeFormatter.ofPattern("MM-dd-yyyy"),
            DateTimeFormatter.ofPattern("dd MMM yyyy"));

    for (DateTimeFormatter formatter : formatters) {
      try {
        TemporalAccessor temporal = formatter.parse(timestampValue);

        if (temporal.isSupported(ChronoField.INSTANT_SECONDS)) {
          return Instant.from(temporal);
        }

        if (temporal.isSupported(ChronoField.EPOCH_DAY)) {
          return LocalDate.from(temporal).atStartOfDay(ZoneOffset.UTC).toInstant();
        }

        if (temporal.isSupported(ChronoField.SECOND_OF_DAY)) {
          return LocalTime.from(temporal)
              .atDate(LocalDate.now(ZoneOffset.UTC))
              .atZone(ZoneOffset.UTC)
              .toInstant();
        }
      } catch (DateTimeParseException ex) {
        LOG.debug("Formatter failed: {}, Exception: {}", formatter, ex.getMessage());
      }
    }

    throw new IllegalArgumentException("Failed to parse timestamp value: " + timestampValue);
  }

  /**
   * Safely executes a handler method, catching exceptions and rethrowing them as
   * IllegalArgumentException exceptions.
   *
   * <p>This method provides exception safety by wrapping the execution of a supplier function.
   *
   * @param <T> The return type of the handler.
   * @param supplier A functional interface providing the value.
   * @return The result of the supplier function.
   * @throws IllegalArgumentException If an exception occurs during the supplier execution.
   */
  private static <T> T safeHandle(HandlerSupplier<T> supplier) {
    try {
      return supplier.get();
    } catch (Exception e) {
      LOG.error(e.getMessage());
      throw new IllegalArgumentException("Error handling type: " + e.getMessage(), e);
    }
  }

  /**
   * Handles the conversion of a Spanner column type to an appropriate value.
   *
   * <p>This method attempts to retrieve the value for the specified column from the provided JSON
   * object and return it as a string. If the value is not found or an error occurs, it handles the
   * exception and returns null or throws an exception accordingly.
   *
   * @param spannerType The type of the Spanner column (currently unused in the method, but might be
   *     used for further expansion).
   * @param columnName The name of the column whose value is to be retrieved.
   * @param valuesJson The JSON object containing the values of the columns.
   * @return The value of the column as a string, or null if the value is not found.
   * @throws IllegalArgumentException If an error occurs during the processing of the value.
   */
  private static Object handleSpannerColumnType(
      String spannerType, String columnName, JSONObject valuesJson) {
    try {
      if (spannerType.contains("string")) {
        return valuesJson.optString(columnName, null);
      } else if (spannerType.contains("bytes")) {
        if (valuesJson.isNull(columnName)) {
          return null;
        }
        String hexEncodedString = valuesJson.optString(columnName);
        if (hexEncodedString.isEmpty()) {
          return null;
        }
        return safeHandle(() -> parseBlobType(hexEncodedString));
      } else {
        return valuesJson.isNull(columnName) ? null : valuesJson.opt(columnName);
      }
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Exception Caught During parsing for Spanner column type: " + spannerType);
    }
  }

  /**
   * Parses a column value based on its Cassandra column type and wraps it into {@link
   * PreparedStatementValueObject}.
   *
   * <p>This method processes basic Cassandra types (e.g., text, bigint, boolean, timestamp) and
   * special types such as {@link Instant}, {@link UUID}, {@link BigInteger}, and {@link
   * CqlDuration}.
   *
   * @param columnType The Cassandra column type (e.g., "text", "timestamp").
   * @param colValue The column value to parse and wrap.
   * @return A {@link PreparedStatementValueObject} containing the parsed column value.
   * @throws IllegalArgumentException If the column value cannot be converted to the specified type.
   */
  private static PreparedStatementValueObject<?> parseAndCastToCassandraType(
      String columnType, Object colValue) {
    if (colValue == null) {
      return null;
    }

    if (columnType.startsWith("frozen<")) {
      return parseAndCastToCassandraType(extractInnerType(columnType), colValue);
    }

    // Handle collection types
    if (columnType.startsWith("list<")) {
      return safeHandle(
          () -> {
            JSONArray parsedJSONArray =
                colValue instanceof JSONArray
                    ? (JSONArray) colValue
                    : new JSONArray((String) colValue);
            return PreparedStatementValueObject.create(
                columnType, parseCassandraList(columnType, parsedJSONArray));
          });
    } else if (columnType.startsWith("set<")) {
      return safeHandle(
          () -> {
            JSONArray parsedJSONArray =
                colValue instanceof JSONArray
                    ? (JSONArray) colValue
                    : new JSONArray((String) colValue);
            return PreparedStatementValueObject.create(
                columnType, parseCassandraSet(columnType, parsedJSONArray));
          });
    } else if (columnType.startsWith("map<")) {
      return safeHandle(
          () -> {
            JSONObject parsedJSON =
                colValue instanceof JSONObject
                    ? (JSONObject) colValue
                    : new JSONObject((String) colValue);
            return PreparedStatementValueObject.create(
                columnType, parseCassandraMap(columnType, parsedJSON));
          });
    }

    // Handle primitive and standard types
    switch (columnType) {
      case "ascii":
      case "text":
      case "varchar":
        return PreparedStatementValueObject.create(columnType, (String) colValue);

      case "bigint":
      case "int":
      case "smallint":
      case "tinyint":
        return safeHandle(
            () ->
                PreparedStatementValueObject.create(
                    columnType, parseNumericType(columnType, colValue.toString())));

      case "boolean":
        return PreparedStatementValueObject.create(
            columnType, safeHandle(() -> parseBoolean(colValue.toString())));

      case "decimal":
        return PreparedStatementValueObject.create(
            columnType, safeHandle(() -> parseDecimal(colValue.toString())));

      case "double":
      case "float":
        return PreparedStatementValueObject.create(
            columnType, safeHandle(() -> parseFloatingPoint(columnType, colValue.toString())));

      case "inet":
        return PreparedStatementValueObject.create(
            columnType, safeHandle(() -> handleCassandraInetAddressType(colValue.toString())));

      case "time":
        return PreparedStatementValueObject.create(
            columnType,
            safeHandle(
                () ->
                    handleCassandraTimestampType(colValue.toString())
                        .atZone(ZoneId.systemDefault())
                        .toLocalTime()));
      case "timestamp":
      case "datetime":
        return PreparedStatementValueObject.create(
            columnType, safeHandle(() -> handleCassandraTimestampType(colValue.toString())));

      case "date":
        return PreparedStatementValueObject.create(
            columnType, safeHandle(() -> parseDate(colValue.toString())));

      case "timeuuid":
      case "uuid":
        return PreparedStatementValueObject.create(
            columnType, safeHandle(() -> handleCassandraUuidType(colValue.toString())));

      case "varint":
        return PreparedStatementValueObject.create(
            columnType, safeHandle(() -> handleCassandraVarintType(colValue)));

      case "duration":
        return PreparedStatementValueObject.create(
            columnType, safeHandle(() -> handleCassandraDurationType(colValue.toString())));

      case "blob":
        return safeHandle(
            () -> PreparedStatementValueObject.create(columnType, parseBlobType(colValue)));

      default:
        return PreparedStatementValueObject.create(columnType, colValue);
    }
  }

  /**
   * Parses a numeric value to the corresponding type based on the given column type.
   *
   * @param columnType the type of the column (e.g., "bigint", "int", "smallint", "tinyint").
   * @param colValue the value to parse, either as a {@code String} or a {@code Number}.
   * @return the parsed numeric value as the appropriate type (e.g., {@code Long}, {@code Integer},
   *     {@code Short}, {@code Byte}).
   * @throws IllegalArgumentException if the {@code colValue} type is unsupported or does not match
   *     the column type.
   */
  private static Object parseNumericType(String columnType, Object colValue) {
    return safeHandle(
        () -> {
          switch (columnType) {
            case "bigint":
              return Long.parseLong((String) colValue);
            case "int":
              return Integer.parseInt((String) colValue);
            case "smallint":
              return Short.parseShort((String) colValue);
            case "tinyint":
              return Byte.parseByte((String) colValue);
          }
          throw new IllegalArgumentException(
              "Unsupported type for " + columnType + ": " + colValue.getClass());
        });
  }

  /**
   * Parses a boolean value from the provided input.
   *
   * @param colValue the value to parse, either as a {@code String} or a {@code Boolean}.
   * @return the parsed boolean value.
   * @throws ClassCastException if the {@code colValue} is not a {@code String} or {@code Boolean}.
   */
  private static Boolean parseBoolean(Object colValue) {
    if (Arrays.asList("0", "1").contains((String) colValue)) {
      return colValue.equals("1");
    }
    return BooleanUtils.toBoolean((String) colValue);
  }

  /**
   * Parses a decimal value from the provided input.
   *
   * @param colValue the value to parse, either as a {@code String} or a {@code Number}.
   * @return the parsed decimal value as a {@code BigDecimal}.
   * @throws NumberFormatException if the {@code colValue} is a {@code String} and cannot be
   *     converted to {@code BigDecimal}.
   * @throws ClassCastException if the {@code colValue} is not a {@code String}, {@code Number}, or
   *     {@code BigDecimal}.
   */
  private static BigDecimal parseDecimal(Object colValue) {
    return new BigDecimal((String) colValue);
  }

  /**
   * Parses a floating-point value to the corresponding type based on the given column type.
   *
   * @param columnType the type of the column (e.g., "double", "float").
   * @param colValue the value to parse, either as a {@code String} or a {@code Number}.
   * @return the parsed floating-point value as a {@code Double} or {@code Float}.
   * @throws IllegalArgumentException if the column type is invalid or the value cannot be parsed.
   */
  private static Object parseFloatingPoint(String columnType, Object colValue) {
    if (columnType.equals("double")) {
      return Double.parseDouble((String) colValue);
    }
    return Float.parseFloat((String) colValue);
  }

  private static LocalDate parseDate(Object colValue) {
    return handleCassandraTimestampType((String) colValue)
        .atZone(ZoneId.systemDefault())
        .toLocalDate();
  }

  /**
   * Parses a Cassandra list from the given JSON array.
   *
   * @param columnType the Cassandra column type (e.g., "list of int", "list of text")
   * @param colValue the JSON array representing the list values
   * @return a {@link List} containing parsed values, or an empty list if {@code colValue} is null
   */
  private static List<?> parseCassandraList(String columnType, JSONArray colValue) {
    if (colValue.isEmpty()) {
      return Collections.emptyList();
    }
    String innerType = extractInnerType(columnType);
    List<Object> parsedList = new ArrayList<>();
    for (int i = 0; i < colValue.length(); i++) {
      Object value = colValue.get(i);
      parsedList.add(parseNestedType(innerType, value).value());
    }
    return parsedList;
  }

  /**
   * Extracts the inner type of a Cassandra collection column (e.g., "list of int" -> "int").
   *
   * @param columnType the Cassandra column type
   * @return the extracted inner type as a {@link String}
   */
  private static String extractInnerType(String columnType) {
    return columnType.substring(columnType.indexOf('<') + 1, columnType.lastIndexOf('>'));
  }

  /**
   * Extracts the key and value types from a Cassandra map column type (e.g., "map of int and
   * text").
   *
   * @param columnType the Cassandra column type
   * @return an array of two {@link String}s, where the first element is the key type and the second
   *     element is the value type
   */
  private static String[] extractKeyValueTypes(String columnType) {
    String innerTypes =
        columnType.substring(columnType.indexOf('<') + 1, columnType.lastIndexOf('>'));
    return innerTypes.split(",", 2);
  }

  /**
   * Parses a nested Cassandra type from a given value.
   *
   * @param type the Cassandra column type (e.g., "int", "text", "map of int of text")
   * @param value the value to parse
   * @return a {@link PreparedStatementValueObject} representing the parsed type
   */
  private static PreparedStatementValueObject<?> parseNestedType(String type, Object value) {
    return parseAndCastToCassandraType(type.trim(), value);
  }

  /**
   * Parses a Cassandra set from the given JSON array.
   *
   * @param columnType the Cassandra column type (e.g., "set of int", "set of text")
   * @param colValue the JSON array representing the set values
   * @return a {@link Set} containing parsed values, or an empty set if {@code colValue} is null
   */
  private static Set<?> parseCassandraSet(String columnType, JSONArray colValue) {
    if (colValue.isEmpty()) {
      return Collections.emptySet();
    }
    String innerType = extractInnerType(columnType);
    Set<Object> parsedSet = new HashSet<>();
    for (int i = 0; i < colValue.length(); i++) {
      Object value = colValue.get(i);
      parsedSet.add(parseNestedType(innerType, value).value());
    }
    return parsedSet;
  }

  /**
   * Parses a Cassandra map from the given JSON object.
   *
   * @param columnType the Cassandra column type (e.g., "map of int and text")
   * @param colValue the JSON object representing the map values
   * @return a {@link Map} containing parsed key-value pairs, or an empty map if {@code colValue} is
   *     null
   */
  private static Map<?, ?> parseCassandraMap(String columnType, JSONObject colValue) {
    if (colValue.isEmpty()) {
      return Collections.emptyMap();
    }
    String[] keyValueTypes = extractKeyValueTypes(columnType);
    String keyType = keyValueTypes[0];
    String valueType = keyValueTypes[1];

    Map<Object, Object> parsedMap = new HashMap<>();
    for (String key : colValue.keySet()) {
      Object parsedKey = parseNestedType(keyType, key).value();
      Object parsedValue = parseNestedType(valueType, colValue.get(key)).value();
      parsedMap.put(parsedKey, parsedValue);
    }
    return parsedMap;
  }

  /**
   * Parses a column's value from a JSON object based on Spanner and source database column types.
   *
   * <p>This method determines the column type, extracts the value using helper methods, and returns
   * a {@link PreparedStatementValueObject} containing the column value formatted for Cassandra.
   *
   * @param spannerColDef The Spanner column definition containing column name and type.
   * @param sourceColDef The source database column definition containing column type.
   * @param valuesJson The JSON object containing column values.
   * @param sourceDbTimezoneOffset The timezone offset for date-time columns (if applicable).
   * @return A {@link PreparedStatementValueObject} containing the parsed column value.
   */
  public static PreparedStatementValueObject<?> getColumnValueByType(
      SpannerColumnDefinition spannerColDef,
      SourceColumnDefinition sourceColDef,
      JSONObject valuesJson,
      String sourceDbTimezoneOffset) {

    if (spannerColDef == null || sourceColDef == null) {
      throw new IllegalArgumentException("Column definitions cannot be null.");
    }

    String spannerType = spannerColDef.getType().getName().toLowerCase();
    String cassandraType = sourceColDef.getType().getName().toLowerCase();
    String columnName = spannerColDef.getName();

    Object columnValue = handleSpannerColumnType(spannerType, columnName, valuesJson);

    return PreparedStatementValueObject.create(
        cassandraType, Objects.requireNonNullElse(columnValue, NullClass.INSTANCE));
  }

  /**
   * Casts the given column value to the expected type based on the Cassandra column type.
   *
   * <p>This method attempts to parse and cast the column value to a type compatible with the
   * provided Cassandra column type using {@code parseAndGenerateCassandraType}. If the value cannot
   * be cast correctly, an error is logged, and an exception is thrown.
   *
   * @param cassandraType the Cassandra data type of the column (e.g., "text", "bigint", "list of
   *     text")
   * @param columnValue the value of the column to be cast
   * @return the column value cast to the expected type
   * @throws IllegalArgumentException if the Cassandra type is unsupported or the value is invalid
   */
  public static Object castToExpectedType(String cassandraType, Object columnValue) {
    try {
      PreparedStatementValueObject<?> valueObject =
          parseAndCastToCassandraType(cassandraType, columnValue);
      return valueObject != null ? valueObject.value() : null;
    } catch (IllegalArgumentException e) {
      LOG.error("Error converting value for column: {}, type: {}", cassandraType, e.getMessage());
      throw new IllegalArgumentException(
          "Error converting value for cassandraType: " + cassandraType);
    }
  }
}
