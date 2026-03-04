/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.spanner.migrations.convertors;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ChangeEventConvertorException;
import com.google.cloud.teleport.v2.spanner.type.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.regex.Pattern;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.math.NumberUtils;

/**
 * Utility class with methods which converts text fields in change events represented by JSONObject
 * to Cloud Spanner types. TODO(b/174506187) - Add support for other data types.
 */
public class ChangeEventTypeConvertor {

  // Timestamp formatter used by FormatDatastreamRecordToJson.java for encoding timestamp
  private static final DateTimeFormatter DATASTREAM_TIMESTAMP_WITH_TZ_FORMATTER =
      DateTimeFormatter.ISO_OFFSET_DATE_TIME;

  // Date formatter used by FormatDatastreamRecordToJson.java for encoding date
  // TODO: Use formatter from FormatDatastreamRecordToJson
  private static final DateTimeFormatter DATASTREAM_DATE_FORMATTER =
      DateTimeFormatter.ISO_LOCAL_DATE;
  private static final Pattern NUMERIC_PATTERN = Pattern.compile("-?\\d+(\\.\\d+)?");

  public static Value toValue(
      JsonNode changeEvent, Type columnType, String key, boolean requiredField)
      throws ChangeEventConvertorException {

    switch (columnType.getCode()) {
      case BOOL:
      case PG_BOOL:
        return Value.bool(toBoolean(changeEvent, key, requiredField));
      case INT64:
      case PG_INT8:
        return Value.int64(toLong(changeEvent, key, requiredField));
      case FLOAT64:
      case PG_FLOAT8:
        return Value.float64(toDouble(changeEvent, key, requiredField));
      case FLOAT32:
      case PG_FLOAT4:
        return Value.float32(toFloat(changeEvent, key, requiredField));
      case STRING:
      case PG_VARCHAR:
      case PG_TEXT:
        return Value.string(toString(changeEvent, key, requiredField));
      case NUMERIC:
      case PG_NUMERIC:
        return Value.numeric(toNumericBigDecimal(changeEvent, key, requiredField));
      case JSON:
      case PG_JSONB:
        return Value.string(toString(changeEvent, key, requiredField));
      case BYTES:
      case PG_BYTEA:
        return Value.bytes(toByteArray(changeEvent, key, requiredField));
      case TIMESTAMP:
      case PG_COMMIT_TIMESTAMP:
      case PG_TIMESTAMPTZ:
        return Value.timestamp(toTimestamp(changeEvent, key, requiredField));
      case DATE:
      case PG_DATE:
        return Value.date(toDate(changeEvent, key, requiredField));
      case ARRAY:
        // TODO(b/422928714): Add support for Array types.
        return null;
        // TODO(b/179070999) - Add support for other data types.
      default:
        throw new IllegalArgumentException(
            "Column name(" + key + ") has unsupported column type(" + columnType.getCode() + ")");
    }
  }

  public static Boolean toBoolean(JsonNode changeEvent, String key, boolean requiredField)
      throws ChangeEventConvertorException {

    if (!containsValue(changeEvent, key, requiredField)) {
      return null;
    }
    try {

      /* Jackson library converts only lowercase "true" to the correct boolean.
       * Everything else is false. Hence using BooleanUtils to do the necessary conversion.
       */
      JsonNode node = changeEvent.get(key);
      if (node.isTextual()) {
        if (node.asText().equalsIgnoreCase("NULL")) {
          return null;
        }
        return BooleanUtils.toBoolean(node.asText());
      }
      return Boolean.valueOf(node.asBoolean());
    } catch (Exception e) {
      throw new ChangeEventConvertorException("Unable to convert field " + key + " to boolean ", e);
    }
  }

  public static Long toLong(JsonNode changeEvent, String key, boolean requiredField)
      throws ChangeEventConvertorException {

    if (!containsValue(changeEvent, key, requiredField)) {
      return null;
    }

    try {
      JsonNode node = changeEvent.get(key);
      if (node.isTextual()) {
        if (node.asText().equalsIgnoreCase("NULL")) {
          return null;
        }
        return Long.valueOf(node.asText());
      }
      return node.asLong();

    } catch (Exception e) {
      throw new ChangeEventConvertorException("Unable to convert field " + key + " to long ", e);
    }
  }

  public static Double toDouble(JsonNode changeEvent, String key, boolean requiredField)
      throws ChangeEventConvertorException {

    if (!containsValue(changeEvent, key, requiredField)) {
      return null;
    }
    try {
      JsonNode node = changeEvent.get(key);
      if (node.isTextual()) {
        if (node.asText().equalsIgnoreCase("NULL")) {
          return null;
        }
        return Double.valueOf(node.asText());
      }
      return Double.valueOf(node.asDouble());
    } catch (Exception e) {
      throw new ChangeEventConvertorException("Unable to convert field " + key + " to double ", e);
    }
  }

  public static Float toFloat(JsonNode changeEvent, String key, boolean requiredField)
      throws ChangeEventConvertorException {

    if (!containsValue(changeEvent, key, requiredField)) {
      return null;
    }
    try {
      JsonNode node = changeEvent.get(key);
      if (node.isTextual()) {
        if (node.asText().equalsIgnoreCase("NULL")) {
          return null;
        }
        return Float.valueOf(node.asText());
      }
      return new Float(node.asDouble());
    } catch (Exception e) {
      throw new ChangeEventConvertorException("Unable to convert field " + key + " to float ", e);
    }
  }

  public static String toString(JsonNode changeEvent, String key, boolean requiredField)
      throws ChangeEventConvertorException {

    if (!containsValue(changeEvent, key, requiredField)) {
      return null;
    }
    try {
      return changeEvent.get(key).asText();
    } catch (Exception e) {
      // Throw an exception as all conversion options are exhausted.
      throw new ChangeEventConvertorException("Unable to convert field " + key + " to string ", e);
    }
  }

  public static ByteArray toByteArray(JsonNode changeEvent, String key, boolean requiredField)
      throws ChangeEventConvertorException {

    if (!containsValue(changeEvent, key, requiredField)) {
      return null;
    }
    try {
      JsonNode node = changeEvent.get(key);
      if (node.isIntegralNumber()) {
        // Datastream returns integral types (e.g. Long) for BIT and similar datatypes.
        // These should be interpreted as-is and directly converted to a byte array.
        BigInteger bigIntValue = new BigInteger(node.asText());
        return ByteArray.copyFrom(bigIntValue.toByteArray());
      }

      // For data with Spanner type as BYTES, Datastream returns a hex encoded string. We need to
      // decode it before returning to ensure data correctness.
      String s = node.asText();
      if (s.equalsIgnoreCase("NULL")) {
        return null;
      }
      // Make an odd length hex string even by appending a 0 in the beginning.
      if (s.length() % 2 == 1) {
        s = "0" + s;
      }
      return ByteArray.copyFrom(hexStringToByteArray(s));
    } catch (Exception e) {
      throw new ChangeEventConvertorException(
          "Unable to convert field " + key + " to ByteArray", e);
    }
  }

  /*
   * This function tries to convert
   * 1) From Timestamp string format
   * 2) From long value as microseconds
   */
  public static Timestamp toTimestamp(JsonNode changeEvent, String key, boolean requiredField)
      throws ChangeEventConvertorException {

    if (!containsValue(changeEvent, key, requiredField)) {
      return null;
    }
    try {
      String timeString = changeEvent.get(key).asText();
      if (timeString.equalsIgnoreCase("NULL")) {
        return null;
      }
      Instant instant = parseTimestamp(timeString);
      return Timestamp.ofTimeSecondsAndNanos(instant.getEpochSecond(), instant.getNano());
    } catch (Exception e) {
      throw new ChangeEventConvertorException(
          "Unable to convert field " + key + " to Timestamp", e);
    }
  }

  public static Date toDate(JsonNode changeEvent, String key, boolean requiredField)
      throws ChangeEventConvertorException {

    if (!containsValue(changeEvent, key, requiredField)) {
      return null;
    }
    try {
      String dateString = changeEvent.get(key).asText();
      if (dateString.equalsIgnoreCase("NULL")) {
        return null;
      }
      return Date.fromJavaUtilDate(parseLenientDate(dateString));
    } catch (Exception e) {
      throw new ChangeEventConvertorException("Unable to convert field " + key + " to Date", e);
    }
  }

  public static byte[] hexStringToByteArray(String s) {
    int len = s.length();
    byte[] data = new byte[len / 2];
    for (int i = 0; i < len; i += 2) {
      data[i / 2] =
          (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i + 1), 16));
    }
    return data;
  }

  private static boolean isNumeric(String str) {
    return NUMERIC_PATTERN.matcher(str).matches(); // match a number with optional '-' and decimal.
  }

  /*
   * This function converts the JSON field to string. In addition, this function also checks
   * if the field is a number.
   */
  public static BigDecimal toNumericBigDecimal(
      JsonNode changeEvent, String key, boolean requiredField)
      throws ChangeEventConvertorException {

    String value = toString(changeEvent, key, requiredField);
    if (value == null) {
      return null;
    }
    if (value.equalsIgnoreCase("NULL")) {
      return null;
    }
    if (NumberUtils.isCreatable(value) || NumberUtils.isParsable(value) || isNumeric(value)) {
      return new BigDecimal(value).setScale(9, RoundingMode.HALF_UP);
    }
    throw new ChangeEventConvertorException(
        "Unable to convert field "
            + key
            + " to Numeric. Creatable("
            + NumberUtils.isCreatable(value)
            + "), Parsable("
            + NumberUtils.isParsable(value)
            + ")");
  }

  /* Checks if the change event has the key and a value associated with this. This
   * function also throws an exception if it's a required field.
   */
  private static boolean containsValue(JsonNode changeEvent, String key, boolean requiredField)
      throws ChangeEventConvertorException {

    if (requiredField && !changeEvent.has(key)) {
      throw new ChangeEventConvertorException("Required key " + key + " not found in change event");
    }
    return changeEvent.hasNonNull(key);
  }

  private static java.util.Date parseDate(String date) {
    LocalDate localDate = LocalDate.parse(date, DATASTREAM_DATE_FORMATTER);
    return java.sql.Date.valueOf(localDate);
  }

  private static ZonedDateTime convertToZonedDateTime(String timestamp) {
    ZonedDateTime zonedDateTime;
    try {
      zonedDateTime =
          ZonedDateTime.parse(timestamp, DATASTREAM_TIMESTAMP_WITH_TZ_FORMATTER)
              .withZoneSameInstant(ZoneId.of("UTC"));
    } catch (DateTimeParseException e) {
      if (!timestamp.endsWith("Z")) {

        // Datastream replication in JSON format does not contain 'Z' at the end of timestamp.
        timestamp = timestamp + "Z";
        zonedDateTime =
            ZonedDateTime.parse(timestamp, DATASTREAM_TIMESTAMP_WITH_TZ_FORMATTER)
                .withZoneSameInstant(ZoneId.of("UTC"));
      } else {
        throw e;
      }
    }
    return zonedDateTime;
  }

  /* Datastream maps Oracle Date as Timestamp. This function tries to parse date
   * and then tries parse as Timestamp and extract date.
   */
  private static java.util.Date parseLenientDate(String date) {
    try {
      return parseDate(date);
    } catch (DateTimeParseException e) {
      /* Exception due to wrong format. Try parsing as Timestamp and extract date.
       * Datastream may have chosen to map date field as timestamp.
       */
      ZonedDateTime zonedDateTime = convertToZonedDateTime(date);
      return parseDate(zonedDateTime.format(DATASTREAM_DATE_FORMATTER));
    }
  }

  private static Instant parseTimestamp(String timestamp) {
    ZonedDateTime zonedDateTime = convertToZonedDateTime(timestamp);
    return Instant.from(zonedDateTime);
  }
}
