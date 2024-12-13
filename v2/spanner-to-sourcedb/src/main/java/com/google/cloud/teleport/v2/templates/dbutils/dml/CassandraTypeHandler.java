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

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

class CassandraTypeHandler {

  @FunctionalInterface
  public interface TypeParser<T> {
    T parse(Object value);
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
  public static String handleCassandraAsciiType(String colName, JSONObject valuesJson) {
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
   * Generates a {@link BigInteger} based on the provided {@link CassandraTypeHandler}.
   *
   * <p>This method fetches the value associated with the given column name ({@code colName}) from
   * the {@code valuesJson} object, and converts it to a {@link BigInteger}. The value can either be
   * a string representing a number or a binary representation of a large integer (varint).
   *
   * @param colName - The column name used to fetch the key from {@code valuesJson}.
   * @param valuesJson - The {@link JSONObject} containing all the key-value pairs for the current
   *     incoming stream.
   * @return A {@link BigInteger} object representing the varint value from the Cassandra data.
   * @throws IllegalArgumentException If the value is not a valid format for varint (neither a valid
   *     number string nor a byte array).
   */
  public static BigInteger handleCassandraVarintType(String colName, JSONObject valuesJson) {
    Object value = valuesJson.get(colName);

    if (value instanceof String) {
      try {
        return new BigInteger((String) value);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            "Invalid varint format (string) for column: " + colName, e);
      }
    } else if (value instanceof byte[]) {
      try {
        return new BigInteger((byte[]) value);
      } catch (Exception e) {
        throw new IllegalArgumentException(
            "Invalid varint format (byte array) for column: " + colName, e);
      }
    } else {
      return null;
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
  public static Duration handleCassandraDurationType(String colName, JSONObject valuesJson) {
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
  public static InetAddress handleCassandraInetAddressType(String colName, JSONObject valuesJson)
      throws UnknownHostException {
    return InetAddress.getByName(valuesJson.getString(colName));
  }

  /**
   * Generates a Type based on the provided {@link CassandraTypeHandler}.
   *
   * @param colName - which is used to fetch Key from valueJSON.
   * @param valuesJson - contains all the key value for current incoming stream.
   * @return a {@link Boolean} object containing the value represented in cassandra type.
   */
  public static Boolean handleCassandraBoolType(String colName, JSONObject valuesJson) {
    return valuesJson.optBoolean(colName, false);
  }

  /**
   * Generates a Type based on the provided {@link CassandraTypeHandler}.
   *
   * @param colName - which is used to fetch Key from valueJSON.
   * @param valuesJson - contains all the key value for current incoming stream.
   * @return a {@link Float} object containing the value represented in cassandra type.
   */
  public static Float handleCassandraFloatType(String colName, JSONObject valuesJson) {
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
  public static Double handleCassandraDoubleType(String colName, JSONObject valuesJson) {
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
  public static ByteBuffer handleCassandraBlobType(String colName, JSONObject valuesJson) {
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
  public static ByteBuffer parseBlobType(Object colValue) {
    byte[] byteArray;
    if (colValue instanceof byte[]) {
      byteArray = (byte[]) colValue;
    } else if (colValue instanceof String) {
      byteArray = java.util.Base64.getDecoder().decode((String) colValue);
    } else {
      throw new IllegalArgumentException("Unsupported type for column");
    }
    return ByteBuffer.wrap(byteArray);
  }

  /**
   * Generates a {@link LocalDate} based on the provided {@link CassandraTypeHandler}.
   *
   * <p>This method processes the given JSON object to extract a date value using the specified
   * column name and formatter. It specifically handles the "Cassandra Date" format (yyyy-MM-dd).
   * The resulting {@link LocalDate} represents the date value associated with the column.
   *
   * @param colName - the key used to fetch the value from the provided {@link JSONObject}.
   * @param valuesJson - the JSON object containing all key-value pairs for the current incoming
   *     data stream.
   * @return a {@link LocalDate} object containing the date value represented in Cassandra type
   *     format. If the column is missing or contains an invalid value, this will return {@code
   *     null}.
   */
  public static LocalDate handleCassandraDateType(String colName, JSONObject valuesJson) {
    return handleCassandraGenericDateType(colName, valuesJson, "yyyy-MM-dd");
  }

  /**
   * Generates a {@link LocalDate} based on the provided {@link CassandraTypeHandler}.
   *
   * <p>This method processes the given JSON object to extract a timestamp value using the specified
   * column name and formatter. It specifically handles the "Cassandra Timestamp" format
   * (yyyy-MM-dd'T'HH:mm:ss.SSSZ). The resulting {@link LocalDate} represents the date part of the
   * timestamp value associated with the column.
   *
   * @param colName - the key used to fetch the value from the provided {@link JSONObject}.
   * @param valuesJson - the JSON object containing all key-value pairs for the current incoming
   *     data stream.
   * @return a {@link LocalDate} object containing the date value extracted from the timestamp in
   *     Cassandra type format. If the column is missing or contains an invalid value, this will
   *     return {@code null}.
   */
  public static LocalDate handleCassandraTimestampType(String colName, JSONObject valuesJson) {
    return handleCassandraGenericDateType(colName, valuesJson, "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
  }

  /**
   * A helper method that handles the conversion of a given column value to a {@link LocalDate}
   * based on the specified date format (formatter).
   *
   * <p>This method extracts the value for the given column name from the provided JSON object and
   * parses it into a {@link LocalDate} based on the provided date format. If the value is in an
   * unsupported type or format, an exception is thrown.
   *
   * @param colName - the key used to fetch the value from the provided {@link JSONObject}.
   * @param valuesJson - the JSON object containing all key-value pairs for the current incoming
   *     data stream.
   * @param formatter - the date format pattern used to parse the value (e.g., "yyyy-MM-dd").
   * @return a {@link LocalDate} object containing the parsed date value. If the column is missing
   *     or invalid, this method returns {@code null}.
   */
  private static LocalDate handleCassandraGenericDateType(
      String colName, JSONObject valuesJson, String formatter) {
    Object colValue = valuesJson.opt(colName);
    if (colValue == null) {
      return null;
    }

    if (formatter == null) {
      formatter = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
    }

    return parseDate(colName, colValue, formatter);
  }

  /**
   * Parses a column value (String, {@link java.util.Date}, or {@code Long}) into a {@link
   * LocalDate} using the specified date format.
   *
   * <p>This method handles different data types (String, Date, Long) by converting them into a
   * {@link LocalDate}. The provided formatter is used to parse date strings, while other types are
   * converted based on their corresponding representations.
   *
   * @param colName - the key used to fetch the value from the provided {@link JSONObject}.
   * @param colValue - the value to be parsed into a {@link LocalDate}.
   * @param formatter - the date format pattern used to parse date strings.
   * @return a {@link LocalDate} object parsed from the given value.
   * @throws IllegalArgumentException if the value cannot be parsed or is of an unsupported type.
   */
  private static LocalDate parseDate(String colName, Object colValue, String formatter) {
    LocalDate localDate;
    if (colValue instanceof String) {
      try {
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(formatter);
        localDate = LocalDate.parse((String) colValue, dateFormatter);
      } catch (DateTimeParseException e) {
        throw new IllegalArgumentException("Invalid date format for column " + colName, e);
      }
    } else if (colValue instanceof java.util.Date) {
      localDate =
          ((java.util.Date) colValue)
              .toInstant()
              .atZone(java.time.ZoneId.systemDefault())
              .toLocalDate();
    } else if (colValue instanceof Long) {
      localDate =
          java.time.Instant.ofEpochMilli((Long) colValue)
              .atZone(java.time.ZoneId.systemDefault())
              .toLocalDate();
    } else {
      throw new IllegalArgumentException("Unsupported type for column " + colName);
    }
    return localDate;
  }

  /**
   * Generates a Type based on the provided {@link CassandraTypeHandler}.
   *
   * @param colName - which is used to fetch Key from valueJSON.
   * @param valuesJson - contains all the key value for current incoming stream.
   * @return a {@link String} object containing String as value represented in cassandra type.
   */
  public static String handleCassandraTextType(String colName, JSONObject valuesJson) {
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
  public static UUID handleCassandraUuidType(String colName, JSONObject valuesJson) {
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
  public static Long handleCassandraBigintType(String colName, JSONObject valuesJson) {
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
  public static Integer handleCassandraIntType(String colName, JSONObject valuesJson) {
    try {
      return valuesJson.getBigInteger(colName).intValue();
    } catch (JSONException e) {
      return null;
    }
  }

  /**
   * Generates a {@link List} object containing a list of long values from Cassandra.
   *
   * @param colName - The column name used to fetch the key from {@code valuesJson}.
   * @param valuesJson - The {@link JSONObject} containing key-value pairs for the current incoming
   *     stream.
   * @return a {@link List} object containing a list of long values represented in Cassandra.
   */
  public static List<Long> handleInt64ArrayType(String colName, JSONObject valuesJson) {
    return handleArrayType(
        colName,
        valuesJson,
        obj -> {
          if (obj instanceof Number) {
            return ((Number) obj).longValue();
          } else if (obj instanceof String) {
            try {
              return Long.getLong((String) obj);
            } catch (NumberFormatException e) {
              throw new IllegalArgumentException("Invalid number format for column " + colName, e);
            }
          } else {
            throw new IllegalArgumentException("Unsupported type for column " + colName);
          }
        });
  }

  /**
   * Generates a {@link Set} object containing a set of long values from Cassandra.
   *
   * @param colName - The column name used to fetch the key from {@code valuesJson}.
   * @param valuesJson - The {@link JSONObject} containing key-value pairs for the current incoming
   *     stream.
   * @return a {@link Set} object containing a set of long values represented in Cassandra.
   */
  public static Set<Long> handleInt64SetType(String colName, JSONObject valuesJson) {
    return new HashSet<>(handleInt64ArrayType(colName, valuesJson));
  }

  /**
   * Generates a {@link List} object containing a list of integer values from Cassandra by
   * converting long values to int.
   *
   * @param colName - The column name used to fetch the key from {@code valuesJson}.
   * @param valuesJson - The {@link JSONObject} containing key-value pairs for the current incoming
   *     stream.
   * @return a {@link List} object containing a list of integer values represented in Cassandra.
   */
  public static List<Integer> handleInt64ArrayAsInt32Array(String colName, JSONObject valuesJson) {
    return handleInt64ArrayType(colName, valuesJson).stream()
        .map(Long::intValue)
        .collect(Collectors.toList());
  }

  /**
   * Generates a {@link Set} object containing a set of integer values from Cassandra by converting
   * long values to int.
   *
   * @param colName - The column name used to fetch the key from {@code valuesJson}.
   * @param valuesJson - The {@link JSONObject} containing key-value pairs for the current incoming
   *     stream.
   * @return a {@link Set} object containing a set of integer values represented in Cassandra.
   */
  public static Set<Integer> handleInt64ArrayAsInt32Set(String colName, JSONObject valuesJson) {
    return handleInt64ArrayType(colName, valuesJson).stream()
        .map(Long::intValue)
        .collect(Collectors.toSet());
  }

  /**
   * Generates a {@link Set} object containing a set of string values from Cassandra.
   *
   * @param colName - The column name used to fetch the key from {@code valuesJson}.
   * @param valuesJson - The {@link JSONObject} containing key-value pairs for the current incoming
   *     stream.
   * @return a {@link Set} object containing a set of string values represented in Cassandra.
   */
  public static Set<String> handleStringSetType(String colName, JSONObject valuesJson) {
    return new HashSet<>(handleStringArrayType(colName, valuesJson));
  }

  /**
   * Generates a {@link List} object containing a list of string values from Cassandra.
   *
   * @param colName - The column name used to fetch the key from {@code valuesJson}.
   * @param valuesJson - The {@link JSONObject} containing key-value pairs for the current incoming
   *     stream.
   * @return a {@link List} object containing a list of string values represented in Cassandra.
   */
  public static List<String> handleStringArrayType(String colName, JSONObject valuesJson) {
    return handleArrayType(colName, valuesJson, String::valueOf);
  }

  /**
   * Generates a {@link List} object containing a list of boolean values from Cassandra.
   *
   * @param colName - The column name used to fetch the key from {@code valuesJson}.
   * @param valuesJson - The {@link JSONObject} containing key-value pairs for the current incoming
   *     stream.
   * @return a {@link List} object containing a list of boolean values represented in Cassandra.
   */
  public static List<Boolean> handleBoolArrayType(String colName, JSONObject valuesJson) {
    return handleArrayType(
        colName, valuesJson, obj -> obj instanceof String && Boolean.parseBoolean((String) obj));
  }

  /**
   * Generates a {@link Set} object containing a set of boolean values from Cassandra.
   *
   * @param colName - The column name used to fetch the key from {@code valuesJson}.
   * @param valuesJson - The {@link JSONObject} containing key-value pairs for the current incoming
   *     stream.
   * @return a {@link Set} object containing a set of boolean values represented in Cassandra.
   */
  public static Set<Boolean> handleBoolSetTypeString(String colName, JSONObject valuesJson) {
    return new HashSet<>(handleBoolArrayType(colName, valuesJson));
  }

  /**
   * Generates a {@link List} object containing a list of double values from Cassandra.
   *
   * @param colName - The column name used to fetch the key from {@code valuesJson}.
   * @param valuesJson - The {@link JSONObject} containing key-value pairs for the current incoming
   *     stream.
   * @return a {@link List} object containing a list of double values represented in Cassandra.
   */
  public static List<Double> handleFloat64ArrayType(String colName, JSONObject valuesJson) {
    return handleArrayType(
        colName,
        valuesJson,
        obj -> {
          if (obj instanceof Number) {
            return ((Number) obj).doubleValue();
          } else if (obj instanceof String) {
            try {
              return Double.valueOf((String) obj);
            } catch (NumberFormatException e) {
              throw new IllegalArgumentException("Invalid number format for column " + colName, e);
            }
          } else {
            throw new IllegalArgumentException("Unsupported type for column " + colName);
          }
        });
  }

  /**
   * Generates a {@link Set} object containing a set of double values from Cassandra.
   *
   * @param colName - The column name used to fetch the key from {@code valuesJson}.
   * @param valuesJson - The {@link JSONObject} containing key-value pairs for the current incoming
   *     stream.
   * @return a {@link Set} object containing a set of double values represented in Cassandra.
   */
  public static Set<Double> handleFloat64SetType(String colName, JSONObject valuesJson) {
    return new HashSet<>(handleFloat64ArrayType(colName, valuesJson));
  }

  /**
   * Generates a {@link List} object containing a list of float values from Cassandra.
   *
   * @param colName - The column name used to fetch the key from {@code valuesJson}.
   * @param valuesJson - The {@link JSONObject} containing key-value pairs for the current incoming
   *     stream.
   * @return a {@link List} object containing a list of float values represented in Cassandra.
   */
  public static List<Float> handleFloatArrayType(String colName, JSONObject valuesJson) {
    return handleFloat64ArrayType(colName, valuesJson).stream()
        .map(Double::floatValue)
        .collect(Collectors.toList());
  }

  /**
   * Generates a {@link Set} object containing a set of float values from Cassandra.
   *
   * @param colName - The column name used to fetch the key from {@code valuesJson}.
   * @param valuesJson - The {@link JSONObject} containing key-value pairs for the current incoming
   *     stream.
   * @return a {@link Set} object containing a set of float values represented in Cassandra.
   */
  public static Set<Float> handleFloatSetType(String colName, JSONObject valuesJson) {
    return handleFloat64SetType(colName, valuesJson).stream()
        .map(Double::floatValue)
        .collect(Collectors.toSet());
  }

  /**
   * Generates a {@link List} object containing a list of LocalDate values from Cassandra.
   *
   * @param colName - The column name used to fetch the key from {@code valuesJson}.
   * @param valuesJson - The {@link JSONObject} containing key-value pairs for the current incoming
   *     stream.
   * @return a {@link List} object containing a list of LocalDate values represented in Cassandra.
   */
  public static List<LocalDate> handleDateArrayType(String colName, JSONObject valuesJson) {
    return handleArrayType(
        colName, valuesJson, obj -> LocalDate.parse(obj.toString(), DateTimeFormatter.ISO_DATE));
  }

  /**
   * Generates a {@link Set} object containing a set of LocalDate values from Cassandra.
   *
   * @param colName - The column name used to fetch the key from {@code valuesJson}.
   * @param valuesJson - The {@link JSONObject} containing key-value pairs for the current incoming
   *     stream.
   * @return a {@link Set} object containing a set of LocalDate values represented in Cassandra.
   */
  public static Set<LocalDate> handleDateSetType(String colName, JSONObject valuesJson) {
    return new HashSet<>(handleDateArrayType(colName, valuesJson));
  }

  /**
   * Generates a {@link List} object containing a list of Timestamp values from Cassandra.
   *
   * @param colName - The column name used to fetch the key from {@code valuesJson}.
   * @param valuesJson - The {@link JSONObject} containing key-value pairs for the current incoming
   *     stream.
   * @return a {@link List} object containing a list of Timestamp values represented in Cassandra.
   */
  public static List<Timestamp> handleTimestampArrayType(String colName, JSONObject valuesJson) {
    return handleArrayType(
        colName,
        valuesJson,
        value ->
            Timestamp.valueOf(
                parseDate(colName, value, "yyyy-MM-dd'T'HH:mm:ss.SSSX").atStartOfDay()));
  }

  /**
   * Generates a {@link Set} object containing a set of Timestamp values from Cassandra.
   *
   * @param colName - The column name used to fetch the key from {@code valuesJson}.
   * @param valuesJson - The {@link JSONObject} containing key-value pairs for the current incoming
   *     stream.
   * @return a {@link Set} object containing a set of Timestamp values represented in Cassandra.
   */
  public static Set<Timestamp> handleTimestampSetType(String colName, JSONObject valuesJson) {
    return new HashSet<>(handleTimestampArrayType(colName, valuesJson));
  }

  /**
   * Generates a Type based on the provided {@link CassandraTypeHandler}.
   *
   * @param colName - which is used to fetch Key from valueJSON.
   * @param valuesJson - contains all the key value for current incoming stream.
   * @return a {@link List} object containing List of ByteBuffer as value represented in cassandra
   *     type.
   */
  public static List<ByteBuffer> handleByteArrayType(String colName, JSONObject valuesJson) {
    return handleArrayType(colName, valuesJson, CassandraTypeHandler::parseBlobType);
  }

  /**
   * Generates a Type based on the provided {@link CassandraTypeHandler}.
   *
   * @param colName - which is used to fetch Key from valueJSON.
   * @param valuesJson - contains all the key value for current incoming stream.
   * @return a {@link List} object containing List of Type T as value represented in cassandra type
   *     which will be assigned runtime.
   */
  public static <T> List<T> handleArrayType(
      String colName, JSONObject valuesJson, TypeParser<T> parser) {
    return valuesJson.getJSONArray(colName).toList().stream()
        .map(parser::parse)
        .collect(Collectors.toList());
  }

  /**
   * Generates a Type based on the provided {@link CassandraTypeHandler}.
   *
   * @param colName - which is used to fetch Key from valueJSON.
   * @param valuesJson - contains all the key value for current incoming stream.
   * @return a {@link Set} object containing Set of ByteBuffer as value represented in cassandra
   *     type.
   */
  public static Set<ByteBuffer> handleByteSetType(String colName, JSONObject valuesJson) {
    return new HashSet<>(handleByteArrayType(colName, valuesJson));
  }

  /**
   * Converts a stringified JSON object to a {@link Map} representation for Cassandra.
   *
   * <p>This method fetches the value associated with the given column name ({@code colName}) from
   * the {@code valuesJson} object, parses the stringified JSON, and returns it as a {@link Map}.
   *
   * @param colName - The column name used to fetch the key from {@code valuesJson}.
   * @param valuesJson - The {@link JSONObject} containing all the key-value pairs for the current
   *     incoming stream.
   * @return A {@link Map} representing the parsed JSON from the stringified JSON.
   * @throws IllegalArgumentException If the value is not a valid stringified JSON or cannot be
   *     parsed.
   */
  public static Map<String, Object> handleStringifiedJsonToMap(
      String colName, JSONObject valuesJson) {
    Object value = valuesJson.get(colName);
    if (value instanceof String) {
      String jsonString = (String) value;
      try {
        JSONObject jsonObject = new JSONObject(jsonString);
        Map<String, Object> map = new HashMap<>();
        for (String key : jsonObject.keySet()) {
          Object jsonValue = jsonObject.get(key);
          if (jsonValue instanceof JSONArray) {
            map.put(key, jsonObject.getJSONArray(key));
          } else if (jsonValue instanceof JSONObject) {
            map.put(key, jsonObject.getJSONObject(key));
          } else {
            map.put(key, jsonValue);
          }
        }
        return map;
      } catch (Exception e) {
        throw new IllegalArgumentException(
            "Invalid stringified JSON format for column: " + colName, e);
      }
    } else {
      throw new IllegalArgumentException(
          "Invalid format for column: " + colName + ". Expected a stringified JSON.");
    }
  }

  /**
   * Converts a stringified JSON array to a {@link List} representation for Cassandra.
   *
   * <p>This method fetches the value associated with the given column name ({@code colName}) from
   * the {@code valuesJson} object, parses the stringified JSON array, and returns it as a {@link
   * List}.
   *
   * @param colName - The column name used to fetch the key from {@code valuesJson}.
   * @param valuesJson - The {@link JSONObject} containing all the key-value pairs for the current
   *     incoming stream.
   * @return A {@link List} representing the parsed JSON array from the stringified JSON.
   * @throws IllegalArgumentException If the value is not a valid stringified JSON array or cannot
   *     be parsed.
   */
  public static List<Object> handleStringifiedJsonToList(String colName, JSONObject valuesJson) {
    Object value = valuesJson.get(colName);
    if (value instanceof String) {
      String jsonString = (String) value;
      try {
        JSONArray jsonArray = new JSONArray(jsonString);
        List<Object> list = new ArrayList<>();
        for (int i = 0; i < jsonArray.length(); i++) {
          list.add(jsonArray.get(i));
        }
        return list;
      } catch (Exception e) {
        throw new IllegalArgumentException(
            "Invalid stringified JSON array format for column: " + colName, e);
      }
    } else {
      throw new IllegalArgumentException(
          "Invalid format for column: " + colName + ". Expected a stringified JSON array.");
    }
  }

  /**
   * Converts a stringified JSON array to a {@link Set} representation for Cassandra.
   *
   * <p>This method fetches the value associated with the given column name ({@code colName}) from
   * the {@code valuesJson} object, parses the stringified JSON array, and returns it as a {@link
   * Set}.
   *
   * @param colName - The column name used to fetch the key from {@code valuesJson}.
   * @param valuesJson - The {@link JSONObject} containing all the key-value pairs for the current
   *     incoming stream.
   * @return A {@link Set} representing the parsed JSON array from the stringified JSON.
   * @throws IllegalArgumentException If the value is not a valid stringified JSON array or cannot
   *     be parsed.
   */
  public static Set<Object> handleStringifiedJsonToSet(String colName, JSONObject valuesJson) {
    return new HashSet<>(handleStringifiedJsonToList(colName, valuesJson));
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
  public static short convertToSmallInt(Integer integerValue) {
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
  public static byte convertToTinyInt(Integer integerValue) {
    if (integerValue < Byte.MIN_VALUE || integerValue > Byte.MAX_VALUE) {
      throw new IllegalArgumentException("Value is out of range for tinyint.");
    }
    return integerValue.byteValue();
  }

  /**
   * Escapes single quotes in a Cassandra string by replacing them with double single quotes.
   *
   * <p>This method is commonly used to sanitize strings before inserting them into Cassandra
   * queries, where single quotes need to be escaped by doubling them (i.e., `'` becomes `''`).
   *
   * @param value The string to be escaped.
   * @return The escaped string where single quotes are replaced with double single quotes.
   */
  public static String escapeCassandraString(String value) {
    return value.replace("'", "''");
  }

  /**
   * Converts a string representation of a timestamp to a Cassandra-compatible timestamp.
   *
   * <p>The method parses the {@code value} as a {@link ZonedDateTime}, applies the given timezone
   * offset to adjust the time, and converts the result into a UTC timestamp string that is
   * compatible with Cassandra.
   *
   * @param value The timestamp string in ISO-8601 format (e.g., "2024-12-05T10:15:30+01:00").
   * @param timezoneOffset The timezone offset (e.g., "+02:00") to apply to the timestamp.
   * @return A string representation of the timestamp in UTC that is compatible with Cassandra.
   * @throws RuntimeException If the timestamp string is invalid or the conversion fails.
   */
  public static String convertToCassandraTimestamp(String value, String timezoneOffset) {
    try {
      ZonedDateTime dateTime = ZonedDateTime.parse(value);
      ZoneOffset offset = ZoneOffset.of(timezoneOffset);
      dateTime = dateTime.withZoneSameInstant(offset);
      return "'" + dateTime.withZoneSameInstant(ZoneOffset.UTC).toString() + "'";
    } catch (DateTimeParseException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Converts a string representation of a date to a {@link LocalDate} compatible with Cassandra.
   *
   * <p>The method parses the {@code dateString} into an {@link Instant}, converts it to a {@link
   * Date}, and then retrieves the corresponding {@link LocalDate} from the system's default time
   * zone.
   *
   * @param dateString The date string in ISO-8601 format (e.g., "2024-12-05T00:00:00Z").
   * @return The {@link LocalDate} representation of the date.
   */
  public static LocalDate convertToCassandraDate(String dateString) {
    Instant instant = Instant.parse(dateString);
    Date date = Date.from(instant);
    return date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
  }

  /**
   * Converts a string representation of a timestamp to an {@link Instant} compatible with
   * Cassandra.
   *
   * <p>The method parses the {@code dateString} into an {@link Instant}, which represents an
   * instantaneous point in time and is compatible with Cassandra timestamp types.
   *
   * @param dateString The timestamp string in ISO-8601 format (e.g., "2024-12-05T10:15:30Z").
   * @return The {@link Instant} representation of the timestamp.
   */
  public static Instant convertToCassandraTimestamp(String dateString) {
    return Instant.parse(dateString);
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
  public static boolean isValidUUID(String value) {
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
   * <p>This method attempts to resolve the provided string as an {@link InetAddress} using {@link
   * InetAddress#getByName(String)}. If successful, it returns {@code true}, indicating that the
   * string is a valid IP address. Otherwise, it returns {@code false}.
   *
   * @param value The string to check if it represents a valid IP address.
   * @return {@code true} if the string is a valid IP address, {@code false} otherwise.
   */
  public static boolean isValidIPAddress(String value) {
    try {
      InetAddress.getByName(value);
      return true;
    } catch (UnknownHostException e) {
      return false;
    }
  }

  /**
   * Validates if the given string is a valid JSON object.
   *
   * <p>This method attempts to parse the string using {@link JSONObject} to check if the value
   * represents a valid JSON object. If the string is valid JSON, it returns {@code true}, otherwise
   * {@code false}.
   *
   * @param value The string to check if it represents a valid JSON object.
   * @return {@code true} if the string is a valid JSON object, {@code false} otherwise.
   */
  public static boolean isValidJSON(String value) {
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
  public static boolean isAscii(String value) {
    for (int i = 0; i < value.length(); i++) {
      if (value.charAt(i) > 127) {
        return false;
      }
    }
    return true;
  }
}
