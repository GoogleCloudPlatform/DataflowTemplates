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
package com.google.cloud.teleport.v2.templates.datastream;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import org.json.JSONObject;

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

  public static Boolean toBoolean(JSONObject changeEvent, String key, boolean requiredField)
      throws ChangeEventConvertorException {

    if (!containsValue(changeEvent, key, requiredField)) {
      return null;
    }
    /*
     * JSONObject.getBoolean() converts
     * 1) From JSON Boolean objects to boolean value.
     * 2) String representing boolean to boolean value.
     */
    try {
      return Boolean.valueOf(changeEvent.getBoolean(key));
    } catch (Exception e) {
      throw new ChangeEventConvertorException(e);
    }
  }

  public static Long toLong(JSONObject changeEvent, String key, boolean requiredField)
      throws ChangeEventConvertorException {

    if (!containsValue(changeEvent, key, requiredField)) {
      return null;
    }
    /*
     * Doing manual conversion as opposed to JSONObject.getLong() because
     * 1) getLong() loses information when doing double to long conversion.
     * 2) getLong() value has issues with decimal values encoded as strings
     *  example - "123.456".
     */
    try {
      Object field = changeEvent.get(key);

      // Try parsing from Number.
      if (field instanceof Number) {
        Number num = (Number) field;
        return Long.valueOf(num.longValue());
      }

      // Try parsing from string
      if (field instanceof String) {
        Number num = Double.parseDouble((String) field);
        return Long.valueOf(num.longValue());
      }

    } catch (Exception e) {
      // Exhausted all possibilities converting. Throwing an Exception.
    }
    throw new ChangeEventConvertorException("Key " + key + " not a Long");
  }

  public static Double toDouble(JSONObject changeEvent, String key, boolean requiredField)
      throws ChangeEventConvertorException {

    if (!containsValue(changeEvent, key, requiredField)) {
      return null;
    }
    /*
     * JSONObject.getDouble() converts
     * 1) From JSON numbers to double value.
     * 2) String  to double value.
     */
    try {
      return Double.valueOf(changeEvent.getDouble(key));
    } catch (Exception e) {
      throw new ChangeEventConvertorException(e);
    }
  }

  public static String toString(JSONObject changeEvent, String key, boolean requiredField)
      throws ChangeEventConvertorException {

    if (!containsValue(changeEvent, key, requiredField)) {
      return null;
    }
    /*
     * JSONObject.getString() converts JSON strings to strings.
     * All other types cause exceptions. Hence doing manual conversion.
     */
    try {
      Object field = changeEvent.get(key);

      if (field instanceof String) {
        return (String) field;
      }

      // Try converting to Boolean.
      if (field instanceof Boolean) {
        return Boolean.toString((Boolean) field);
      }

      // Try converting to Number.
      if (field instanceof Number) {
        Number num = (Number) field;
        return num.toString();
      }
    } catch (Exception e) {
      // Throw an exception as all conversion options are exhausted.
    }
    throw new ChangeEventConvertorException(
        "Unable to convert Key: " + key + " Value: " + changeEvent.get(key) + "; Not a string");
  }

  public static ByteArray toByteArray(JSONObject changeEvent, String key, boolean requiredField)
      throws ChangeEventConvertorException {
    if (!containsValue(changeEvent, key, requiredField)) {
      return null;
    }
    try {
      String value = toString(changeEvent, key, requiredField);
      return ByteArray.copyFrom(value);
    } catch (Exception e) {
      throw new ChangeEventConvertorException(e);
    }
  }

  /*
   * This function tries to convert
   * 1) From Timestamp string format
   * 2) From long value as microseconds
   */
  public static Timestamp toTimestamp(JSONObject changeEvent, String key, boolean requiredField)
      throws ChangeEventConvertorException {
    if (!containsValue(changeEvent, key, requiredField)) {
      return null;
    }
    try {
      return Timestamp.of(parseTimestamp(changeEvent.getString(key)));
    } catch (Exception e) {
      throw new ChangeEventConvertorException(
          "Unable to convert Key: "
              + key
              + " Value: "
              + changeEvent.get(key)
              + "; Not a Timestamp");
    }
  }

  public static Date toDate(JSONObject changeEvent, String key, boolean requiredField)
      throws ChangeEventConvertorException {
    if (!containsValue(changeEvent, key, requiredField)) {
      return null;
    }
    try {
      return Date.fromJavaUtilDate(parseLenientDate(changeEvent.getString(key)));
    } catch (Exception e) {
      throw new ChangeEventConvertorException(e);
    }
  }

  /* Checks if the change event has the key and a value associated with this. This
   * function also throws an exception if it's a required field.
   */
  private static boolean containsValue(JSONObject changeEvent, String key, boolean requiredField)
      throws ChangeEventConvertorException {
    boolean containsValue = !changeEvent.isNull(key);
    if (requiredField && !containsValue) {
      throw new ChangeEventConvertorException("Required key " + key + " not found in change event");
    }
    return containsValue;
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

  private static java.util.Date parseTimestamp(String timestamp) {
    ZonedDateTime zonedDateTime = convertToZonedDateTime(timestamp);
    Instant result = Instant.from(zonedDateTime);
    return java.util.Date.from(result);
  }
}
