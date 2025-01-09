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

import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnDefinition;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerColumnDefinition;
import com.google.cloud.teleport.v2.templates.models.PreparedStatementValueObject;
import com.google.common.net.InetAddresses;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.eclipse.jetty.util.StringUtil;
import org.json.JSONArray;
import org.json.JSONException;
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

  /**
   * Functional interface for parsing an object value to a specific type.
   *
   * <p>This interface provides a contract to implement type conversion logic where an input object
   * is parsed and transformed into the desired target type.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * TypeParser<Integer> intParser = value -> Integer.parseInt(value.toString());
   * Integer parsedValue = intParser.parse("123");
   * }</pre>
   *
   * @param <T> The target type to which the value will be parsed.
   */
  @FunctionalInterface
  public interface TypeParser<T> {

    /**
     * Parses the given value and converts it into the target type {@code T}.
     *
     * @param value The input value to be parsed.
     * @return The parsed value of type {@code T}.
     */
    T parse(Object value);
  }

  /**
   * Functional interface for supplying a value with exception handling.
   *
   * <p>This interface provides a mechanism to execute logic that may throw a checked exception,
   * making it useful for methods where exception handling is required.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * HandlerSupplier<String> supplier = () -> {
   *     if (someCondition) {
   *         throw new IOException("Error occurred");
   *     }
   *     return "Success";
   * };
   *
   * try {
   *     String result = supplier.get();
   *     System.out.println(result);
   * } catch (Exception e) {
   *     e.printStackTrace();
   * }
   * }</pre>
   *
   * @param <T> The type of value supplied by the supplier.
   */
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
   * Converts a {@link String} to an ASCII representation for Cassandra's {@link String} or other
   * ASCII-based types.
   *
   * <p>This method ensures that the string contains only valid ASCII characters (0-127). If any
   * non-ASCII characters are found, an exception is thrown.
   *
   * @param colName - The column name used to fetch the key from {@code valuesJson}.
   * @param valuesJson - The {@link JSONObject} containing all the key-value pairs for the current
   *     incoming stream.
   * @return A {@link String} representing the ASCII value for the column in Cassandra.
   * @throws IllegalArgumentException If the string contains non-ASCII characters.
   */
  private static String handleCassandraAsciiType(String colName, JSONObject valuesJson) {
    Object value = valuesJson.get(colName);
    if (value instanceof String) {
      String stringValue = (String) value;
      if (isAscii(stringValue)) {
        return stringValue;
      } else {
        throw new IllegalArgumentException(
            "Invalid ASCII format for column: "
                + colName
                + ". String contains non-ASCII characters.");
      }
    }
    return null;
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
    if (value instanceof String) {
      try {
        return new BigInteger((String) value);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Invalid varint format (string) for value: " + value, e);
      }
    } else if (value instanceof byte[]) {
      try {
        return new BigInteger((byte[]) value);
      } catch (Exception e) {
        throw new IllegalArgumentException(
            "Invalid varint format (byte array) for value: " + value, e);
      }
    } else if (value instanceof ByteBuffer) {
      try {
        ByteBuffer byteBuffer = (ByteBuffer) value;
        byte[] byteArray = new byte[byteBuffer.remaining()];
        byteBuffer.get(byteArray); // Read bytes from ByteBuffer
        return new BigInteger(byteArray);
      } catch (Exception e) {
        throw new IllegalArgumentException(
            "Invalid varint format (ByteBuffer) for value: " + value, e);
      }
    } else {
      throw new IllegalArgumentException(
          "Invalid value type for varint conversion: " + value.getClass());
    }
  }

  /**
   * Generates a {@link Duration} based on the provided {@link CassandraTypeHandler}.
   *
   * <p>This method fetches a string value from the provided {@code valuesJson} object using the
   * column name {@code colName}, and converts it into a {@link Duration} object. The string value
   * should be in the ISO-8601 duration format (e.g., "PT20.345S").
   *
   * @param colName - The column name used to fetch the key from {@code valuesJson}.
   * @param valuesJson - The {@link JSONObject} containing all the key-value pairs for the current
   *     incoming stream.
   * @return A {@link Duration} object representing the duration value from the Cassandra data.
   * @throws IllegalArgumentException if the value is not a valid duration string.
   */
  private static Duration handleCassandraDurationType(String colName, JSONObject valuesJson) {
    String durationString = valuesJson.optString(colName, null);
    if (durationString == null) {
      return null;
    }
    try {
      return Duration.parse(durationString);
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid duration format for column: " + colName, e);
    }
  }

  /**
   * Generates a Type based on the provided {@link CassandraTypeHandler}.
   *
   * @param colName - which is used to fetch Key from valueJSON.
   * @param valuesJson - contains all the key value for current incoming stream.
   * @return a {@link InetAddress} object containing InetAddress as value represented in cassandra
   *     type.
   */
  private static InetAddress handleCassandraInetAddressType(String colName, JSONObject valuesJson) {
    String inetString = valuesJson.optString(colName, null);
    if (inetString == null) {
      return null;
    }
    try {
      return InetAddresses.forString(inetString);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid IP address format for column: " + colName, e);
    }
  }

  /**
   * Generates a Type based on the provided {@link CassandraTypeHandler}.
   *
   * @param colName - which is used to fetch Key from valueJSON.
   * @param valuesJson - contains all the key value for current incoming stream.
   * @return a {@link Boolean} object containing the value represented in cassandra type.
   */
  private static Boolean handleCassandraBoolType(String colName, JSONObject valuesJson) {
    return valuesJson.optBoolean(colName, false);
  }

  /**
   * Generates a Type based on the provided {@link CassandraTypeHandler}.
   *
   * @param colName - which is used to fetch Key from valueJSON.
   * @param valuesJson - contains all the key value for current incoming stream.
   * @return a {@link Float} object containing the value represented in cassandra type.
   */
  private static Float handleCassandraFloatType(String colName, JSONObject valuesJson) {
    try {
      return valuesJson.getBigDecimal(colName).floatValue();
    } catch (JSONException e) {
      return null;
    }
  }

  /**
   * Generates a Type based on the provided {@link CassandraTypeHandler}.
   *
   * @param colName - which is used to fetch Key from valueJSON.
   * @param valuesJson - contains all the key value for current incoming stream.
   * @return a {@link Double} object containing the value represented in cassandra type.
   */
  private static Double handleCassandraDoubleType(String colName, JSONObject valuesJson) {
    try {
      return valuesJson.getBigDecimal(colName).doubleValue();
    } catch (JSONException e) {
      return null;
    }
  }

  /**
   * Generates a Type based on the provided {@link CassandraTypeHandler}.
   *
   * @param colName - which is used to fetch Key from valueJSON.
   * @param valuesJson - contains all the key value for current incoming stream.
   * @return a {@link ByteBuffer} object containing the value represented in cassandra type.
   */
  private static ByteBuffer handleCassandraBlobType(String colName, JSONObject valuesJson) {
    Object colValue = valuesJson.opt(colName);
    if (colValue == null) {
      return null;
    }
    return parseBlobType(colValue);
  }

  /**
   * Generates a Type based on the provided {@link CassandraTypeHandler}.
   *
   * @param colValue - contains all the key value for current incoming stream.
   * @return a {@link ByteBuffer} object containing the value represented in cassandra type.
   */
  private static ByteBuffer parseBlobType(Object colValue) {
    byte[] byteArray;

    if (colValue instanceof byte[]) {
      byteArray = (byte[]) colValue;
    } else if (colValue instanceof String) {
      String strValue = (String) colValue;
      if (StringUtil.isHex(strValue, 0, strValue.length())) {
        byteArray = convertHexStringToByteArray(strValue);
      } else {
        byteArray = java.util.Base64.getDecoder().decode((String) colValue);
      }
    } else {
      throw new IllegalArgumentException("Unsupported type for column");
    }

    return ByteBuffer.wrap(byteArray);
  }

  /**
   * Converts a hexadecimal string into a byte array.
   *
   * @param hex the hexadecimal string to be converted. It must have an even number of characters,
   *     as each pair of characters represents one byte.
   * @return a byte array representing the binary data equivalent of the hexadecimal string.
   * @throws IllegalArgumentException if the input string contains non-hexadecimal characters.
   *     <p>This method: 1. Calculates the length of the input string and initializes a byte array
   *     of half the length, as two hexadecimal characters represent one byte. 2. Iterates through
   *     the string in steps of two characters. 3. Converts each pair of characters into a single
   *     byte by: - Extracting the numeric value of the first character (most significant 4 bits). -
   *     Extracting the numeric value of the second character (least significant 4 bits). -
   *     Combining the two values into a single byte. 4. Returns the resulting byte array.
   *     <p>Example: Input: "4A3F" Output: byte[] { 0x4A, 0x3F }
   */
  private static byte[] convertHexStringToByteArray(String hex) {
    int len = hex.length();
    byte[] data = new byte[len / 2];
    for (int i = 0; i < len; i += 2) {
      data[i / 2] =
          (byte)
              ((Character.digit(hex.charAt(i), 16) << 4) + Character.digit(hex.charAt(i + 1), 16));
    }
    return data;
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
   * @param colName the key used to fetch the value from the provided {@link JSONObject}.
   * @param valuesJson the JSON object containing key-value pairs, including the timestamp value.
   * @return an {@link Instant} representing the parsed timestamp value in UTC.
   * @throws IllegalArgumentException if the column value is missing, empty, or cannot be parsed as
   *     a valid timestamp.
   */
  private static Instant handleCassandraTimestampType(String colName, JSONObject valuesJson) {
    String timestampValue = valuesJson.optString(colName, null);
    if (timestampValue == null || timestampValue.isEmpty()) {
      throw new IllegalArgumentException(
          "Timestamp value for column " + colName + " is null or empty.");
    }
    return convertToCassandraTimestamp(timestampValue);
  }

  /**
   * Generates a Type based on the provided {@link CassandraTypeHandler}.
   *
   * @param colName - which is used to fetch Key from valueJSON.
   * @param valuesJson - contains all the key value for current incoming stream.
   * @return a {@link String} object containing String as value represented in cassandra type.
   */
  private static String handleCassandraTextType(String colName, JSONObject valuesJson) {
    return valuesJson.optString(
        colName, null); // Get the value or null if the key is not found or the value is null
  }

  /**
   * Generates a Type based on the provided {@link CassandraTypeHandler}.
   *
   * @param colName - which is used to fetch Key from valueJSON.
   * @param valuesJson - contains all the key value for current incoming stream.
   * @return a {@link UUID} object containing UUID as value represented in cassandra type.
   */
  private static UUID handleCassandraUuidType(String colName, JSONObject valuesJson) {
    String uuidString =
        valuesJson.optString(
            colName, null); // Get the value or null if the key is not found or the value is null

    if (uuidString == null) {
      return null;
    }

    return UUID.fromString(uuidString);
  }

  /**
   * Generates a Type based on the provided {@link CassandraTypeHandler}.
   *
   * @param colName - which is used to fetch Key from valueJSON.
   * @param valuesJson - contains all the key value for current incoming stream.
   * @return a {@link Long} object containing Long as value represented in cassandra type.
   */
  private static Long handleCassandraBigintType(String colName, JSONObject valuesJson) {
    try {
      return valuesJson.getBigInteger(colName).longValue();
    } catch (JSONException e) {
      return null;
    }
  }

  /**
   * Generates a Type based on the provided {@link CassandraTypeHandler}.
   *
   * @param colName - which is used to fetch Key from valueJSON.
   * @param valuesJson - contains all the key value for current incoming stream.
   * @return a {@link Integer} object containing Integer as value represented in cassandra type.
   */
  private static Integer handleCassandraIntType(String colName, JSONObject valuesJson) {
    try {
      return valuesJson.getBigInteger(colName).intValue();
    } catch (JSONException e) {
      return null;
    }
  }

  /**
   * Converts an {@link Integer} to a {@code short} (SmallInt).
   *
   * <p>This method checks if the {@code integerValue} is within the valid range for a {@code
   * smallint} (i.e., between {@link Short#MIN_VALUE} and {@link Short#MAX_VALUE}). If the value is
   * out of range, it throws an {@link IllegalArgumentException}.
   *
   * @param integerValue The integer value to be converted.
   * @return The converted {@code short} value.
   * @throws IllegalArgumentException If the {@code integerValue} is out of range for a {@code
   *     smallint}.
   */
  private static short convertToSmallInt(Integer integerValue) {
    if (integerValue < Short.MIN_VALUE || integerValue > Short.MAX_VALUE) {
      throw new IllegalArgumentException("Value is out of range for smallint.");
    }
    return integerValue.shortValue();
  }

  /**
   * Converts an {@link Integer} to a {@code byte} (TinyInt).
   *
   * <p>This method checks if the {@code integerValue} is within the valid range for a {@code
   * tinyint} (i.e., between {@link Byte#MIN_VALUE} and {@link Byte#MAX_VALUE}). If the value is out
   * of range, it throws an {@link IllegalArgumentException}.
   *
   * @param integerValue The integer value to be converted.
   * @return The converted {@code byte} value.
   * @throws IllegalArgumentException If the {@code integerValue} is out of range for a {@code
   *     tinyint}.
   */
  private static byte convertToTinyInt(Integer integerValue) {
    if (integerValue < Byte.MIN_VALUE || integerValue > Byte.MAX_VALUE) {
      throw new IllegalArgumentException("Value is out of range for tinyint.");
    }
    return integerValue.byteValue();
  }

  /**
   * Converts a string representation of a timestamp to an {@link Instant} compatible with
   * Cassandra.
   *
   * <p>The method parses the {@code dateString} into an {@link Instant}, which represents an
   * instantaneous point in time and is compatible with Cassandra timestamp types.
   *
   * @param timestampValue The timestamp string in ISO-8601 format (e.g., "2024-12-05T10:15:30Z").
   * @return The {@link Instant} representation of the timestamp.
   */
  private static Instant convertToCassandraTimestamp(String timestampValue) {
    try {
      return Instant.parse(timestampValue);
    } catch (DateTimeParseException e) {
      try {
        return ZonedDateTime.parse(timestampValue)
            .withZoneSameInstant(java.time.ZoneOffset.UTC)
            .toInstant();
      } catch (DateTimeParseException nestedException) {
        throw new IllegalArgumentException(
            "Failed to parse timestamp value" + timestampValue, nestedException);
      }
    }
  }

  /**
   * Validates if the given string represents a valid UUID.
   *
   * <p>This method attempts to parse the provided string as a UUID using {@link
   * UUID#fromString(String)}. If parsing is successful, it returns {@code true}, indicating that
   * the string is a valid UUID. Otherwise, it returns {@code false}.
   *
   * @param value The string to check if it represents a valid UUID.
   * @return {@code true} if the string is a valid UUID, {@code false} otherwise.
   */
  private static boolean isValidUUID(String value) {
    try {
      UUID.fromString(value);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  /**
   * Validates if the given string represents a valid IP address.
   *
   * <p>This method attempts to resolve the provided string as an {@link InetAddresses} using {@link
   * InetAddresses#forString(String)}. If successful, it returns {@code true}, indicating that the
   * string is a valid IP address. Otherwise, it returns {@code false}.
   *
   * @param value The string to check if it represents a valid IP address.
   * @return {@code true} if the string is a valid IP address, {@code false} otherwise.
   */
  private static boolean isValidIPAddress(String value) {
    try {
      InetAddresses.forString(value);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Validates if the given string is a valid JSONArray.
   *
   * <p>This method attempts to parse the string using {@link JSONArray} to check if the value
   * represents a valid JSON object. If the string is valid JSON, it returns {@code true}, otherwise
   * {@code false}.
   *
   * @param value The string to check if it represents a valid JSON object.
   * @return {@code true} if the string is a valid JSON object, {@code false} otherwise.
   */
  private static boolean isValidJSONArray(String value) {
    try {
      new JSONArray(value);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Validates if the given string is a valid JSONObject.
   *
   * <p>This method attempts to parse the string using {@link JSONObject} to check if the value
   * represents a valid JSON object. If the string is valid JSON, it returns {@code true}, otherwise
   * {@code false}.
   *
   * @param value The string to check if it represents a valid JSON object.
   * @return {@code true} if the string is a valid JSON object, {@code false} otherwise.
   */
  private static boolean isValidJSONObject(String value) {
    try {
      new JSONObject(value);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Helper method to check if a string contains only ASCII characters (0-127).
   *
   * @param value - The string to check.
   * @return true if the string contains only ASCII characters, false otherwise.
   */
  private static boolean isAscii(String value) {
    for (int i = 0; i < value.length(); i++) {
      if (value.charAt(i) > 127) {
        return false;
      }
    }
    return true;
  }

  /**
   * Helper method to check if a string contains Duration Character.
   *
   * @param value - The string to check.
   * @return true if the string contains Duration Character, false otherwise.
   */
  private static boolean isDurationString(String value) {
    try {
      Duration.parse(value);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Safely executes a handler method, catching exceptions and rethrowing them as runtime
   * exceptions.
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
      throw new IllegalArgumentException("Error handling type: " + e.getMessage(), e);
    }
  }

  /**
   * Handles and extracts column values based on the Spanner column type.
   *
   * <p>This method processes Spanner column types (e.g., bigint, string, timestamp, etc.) and
   * returns the parsed value for further handling.
   *
   * @param spannerType The Spanner column type (e.g., "string", "bigint").
   * @param columnName The name of the column.
   * @param valuesJson The JSON object containing the column value.
   * @return The extracted value for the column, or {@code null} if the column type is unsupported.
   */
  private static Object handleSpannerColumnType(
      String spannerType, String columnName, JSONObject valuesJson) {
    switch (spannerType) {
      case "bigint":
      case "int64":
        return CassandraTypeHandler.handleCassandraBigintType(columnName, valuesJson);

      case "string":
        return handleStringType(columnName, valuesJson);

      case "timestamp":
      case "date":
      case "datetime":
        return CassandraTypeHandler.handleCassandraTimestampType(columnName, valuesJson);

      case "boolean":
        return CassandraTypeHandler.handleCassandraBoolType(columnName, valuesJson);

      case "float64":
        return CassandraTypeHandler.handleCassandraDoubleType(columnName, valuesJson);

      case "numeric":
      case "float":
        return CassandraTypeHandler.handleCassandraFloatType(columnName, valuesJson);

      case "bytes":
      case "bytes(max)":
        return CassandraTypeHandler.handleCassandraBlobType(columnName, valuesJson);

      case "integer":
        return CassandraTypeHandler.handleCassandraIntType(columnName, valuesJson);

      default:
        LOG.warn("Unsupported Spanner column type: {}", spannerType);
        return null;
    }
  }

  /**
   * Handles and parses column values for string types, determining specific subtypes dynamically.
   *
   * <p>This method identifies if the string can be a UUID, IP address, JSON, blob, duration, or
   * ASCII type. If none match, it treats the value as a simple text type.
   *
   * @param colName The name of the column.
   * @param valuesJson The JSON object containing the column value.
   * @return The parsed value as the appropriate type (e.g., UUID, JSON, etc.).
   */
  private static Object handleStringType(String colName, JSONObject valuesJson) {
    String inputValue = CassandraTypeHandler.handleCassandraTextType(colName, valuesJson);

    if (isValidUUID(inputValue)) {
      return CassandraTypeHandler.handleCassandraUuidType(colName, valuesJson);
    } else if (isValidIPAddress(inputValue)) {
      return safeHandle(
          () -> CassandraTypeHandler.handleCassandraInetAddressType(colName, valuesJson));
    } else if (isValidJSONArray(inputValue)) {
      return new JSONArray(inputValue);
    } else if (isValidJSONObject(inputValue)) {
      return new JSONObject(inputValue);
    } else if (StringUtil.isHex(inputValue, 0, inputValue.length())) {
      return CassandraTypeHandler.handleCassandraBlobType(colName, valuesJson);
    } else if (isAscii(inputValue)) {
      return CassandraTypeHandler.handleCassandraAsciiType(colName, valuesJson);
    } else if (isDurationString(inputValue)) {
      return CassandraTypeHandler.handleCassandraDurationType(colName, valuesJson);
    }
    return inputValue;
  }

  /**
   * Parses a column value based on its Cassandra column type and wraps it into {@link
   * PreparedStatementValueObject}.
   *
   * <p>This method processes basic Cassandra types (e.g., text, bigint, boolean, timestamp) and
   * special types such as {@link Instant}, {@link UUID}, {@link BigInteger}, and {@link Duration}.
   *
   * @param columnType The Cassandra column type (e.g., "text", "timestamp").
   * @param colValue The column value to parse and wrap.
   * @return A {@link PreparedStatementValueObject} containing the parsed column value.
   * @throws IllegalArgumentException If the column value cannot be converted to the specified type.
   */
  private static PreparedStatementValueObject<?> parseAndCastToCassandraType(
      String columnType, Object colValue) {

    if (columnType.startsWith("list<") && colValue instanceof JSONArray) {
      return PreparedStatementValueObject.create(
          columnType, parseCassandraList(columnType, (JSONArray) colValue));
    } else if (columnType.startsWith("set<") && colValue instanceof JSONArray) {
      return PreparedStatementValueObject.create(
          columnType, parseCassandraSet(columnType, (JSONArray) colValue));
    } else if (columnType.startsWith("map<") && colValue instanceof JSONObject) {
      return PreparedStatementValueObject.create(
          columnType, parseCassandraMap(columnType, (JSONObject) colValue));
    }

    switch (columnType) {
      case "ascii":
      case "text":
      case "varchar":
        return PreparedStatementValueObject.create(columnType, (String) colValue);

      case "bigint":
        return PreparedStatementValueObject.create(columnType, (Long) colValue);

      case "boolean":
        return PreparedStatementValueObject.create(columnType, (Boolean) colValue);

      case "decimal":
        return PreparedStatementValueObject.create(columnType, (BigDecimal) colValue);

      case "double":
        return PreparedStatementValueObject.create(columnType, (Double) colValue);

      case "float":
        return PreparedStatementValueObject.create(columnType, (Float) colValue);

      case "inet":
        return PreparedStatementValueObject.create(columnType, (java.net.InetAddress) colValue);

      case "int":
        return PreparedStatementValueObject.create(columnType, (Integer) colValue);

      case "smallint":
        return PreparedStatementValueObject.create(
            columnType, convertToSmallInt((Integer) colValue));

      case "time":
      case "timestamp":
      case "datetime":
        return PreparedStatementValueObject.create(columnType, (Instant) colValue);

      case "date":
        return PreparedStatementValueObject.create(
            columnType,
            safeHandle(
                () -> {
                  if (colValue instanceof String) {
                    return LocalDate.parse((String) colValue);
                  } else if (colValue instanceof Instant) {
                    return ((Instant) colValue).atZone(ZoneId.systemDefault()).toLocalDate();
                  } else if (colValue instanceof Date) {
                    return ((Date) colValue)
                        .toInstant()
                        .atZone(ZoneId.systemDefault())
                        .toLocalDate();
                  }
                  throw new IllegalArgumentException(
                      "Unsupported value for date conversion: " + colValue);
                }));

      case "timeuuid":
      case "uuid":
        return PreparedStatementValueObject.create(columnType, (UUID) colValue);

      case "tinyint":
        return PreparedStatementValueObject.create(
            columnType, convertToTinyInt((Integer) colValue));

      case "varint":
        return PreparedStatementValueObject.create(columnType, handleCassandraVarintType(colValue));

      case "duration":
        return PreparedStatementValueObject.create(columnType, (Duration) colValue);

      default:
        return PreparedStatementValueObject.create(columnType, colValue);
    }
  }

  /**
   * Parses a Cassandra list from the given JSON array.
   *
   * @param columnType the Cassandra column type (e.g., "list of int", "list of text")
   * @param colValue the JSON array representing the list values
   * @return a {@link List} containing parsed values, or an empty list if {@code colValue} is null
   */
  private static List<?> parseCassandraList(String columnType, JSONArray colValue) {
    if (colValue == null) {
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
    if (colValue == null) {
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
    if (colValue == null) {
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

    if (columnValue == null) {
      LOG.warn("Column value is null for column: {}, type: {}", columnName, spannerType);
      return PreparedStatementValueObject.create(cassandraType, NullClass.INSTANCE);
    }
    return PreparedStatementValueObject.create(cassandraType, columnValue);
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
   * @throws ClassCastException if the value cannot be cast to the expected type
   * @throws IllegalArgumentException if the Cassandra type is unsupported or the value is invalid
   */
  public static Object castToExpectedType(String cassandraType, Object columnValue) {
    try {
      return parseAndCastToCassandraType(cassandraType, columnValue).value();
    } catch (ClassCastException | IllegalArgumentException e) {
      LOG.error("Error converting value for column: {}, type: {}", cassandraType, e.getMessage());
      throw e;
    }
  }
}
