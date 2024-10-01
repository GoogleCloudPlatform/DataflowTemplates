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
package com.google.cloud.teleport.v2.templates.changestream;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.regex.Pattern;
import org.apache.commons.lang3.math.NumberUtils;

/**
 * Utility class with methods which converts text fields in change events represented by JSONObject
 * to Cloud Spanner types. The field types come from spanner change streams hence the data formats
 * are going to the the same.
 */
public class DataChangeRecordTypeConvertor {

  // TODO : check if we can use a common ChangeEventTypeConvertor for this.
  private static final Pattern NUMERIC_PATTERN = Pattern.compile("-?\\d+(\\.\\d+)?");

  public static Boolean toBoolean(JsonNode changeEvent, String key, boolean requiredField)
      throws DataChangeRecordConvertorException {

    if (!containsValue(changeEvent, key, requiredField)) {
      return null;
    }
    JsonNode node = changeEvent.get(key);
    return Boolean.valueOf(node.asBoolean()); // This never throws exception
    // https://fasterxml.github.io/jackson-databind/javadoc/2.7/com/fasterxml/jackson/databind/JsonNode.html#asBoolean()
  }

  public static Long toLong(JsonNode changeEvent, String key, boolean requiredField)
      throws DataChangeRecordConvertorException {

    if (!containsValue(changeEvent, key, requiredField)) {
      return null;
    }

    JsonNode node = changeEvent.get(key);
    return node.asLong(); // No exceptions are thrown
    // https://fasterxml.github.io/jackson-databind/javadoc/2.7/com/fasterxml/jackson/databind/JsonNode.html#asLong()

  }

  public static Double toDouble(JsonNode changeEvent, String key, boolean requiredField)
      throws DataChangeRecordConvertorException {

    if (!containsValue(changeEvent, key, requiredField)) {
      return null;
    }
    JsonNode node = changeEvent.get(key);
    return Double.valueOf(node.asDouble()); // No exceptions are thrown
    // https://fasterxml.github.io/jackson-databind/javadoc/2.7/com/fasterxml/jackson/databind/JsonNode.html#asDouble()

  }

  public static String toString(JsonNode changeEvent, String key, boolean requiredField)
      throws DataChangeRecordConvertorException {

    if (!containsValue(changeEvent, key, requiredField)) {
      return null;
    }

    return changeEvent.get(key).asText(); // No exceptions are thrown
    // https://fasterxml.github.io/jackson-databind/javadoc/2.7/com/fasterxml/jackson/databind/JsonNode.html#asText()

  }

  public static ByteArray toByteArray(JsonNode changeEvent, String key, boolean requiredField)
      throws DataChangeRecordConvertorException {

    if (!containsValue(changeEvent, key, requiredField)) {
      return null;
    }
    try {
      // Spanner stores bytes in base64 format.
      String byteString = changeEvent.get(key).asText();
      return ByteArray.fromBase64(byteString);
    } catch (Exception e) {
      throw new DataChangeRecordConvertorException(
          "Unable to convert field " + key + " to ByteArray", e);
    }
  }

  public static Timestamp toTimestamp(JsonNode changeEvent, String key, boolean requiredField)
      throws DataChangeRecordConvertorException {

    if (!containsValue(changeEvent, key, requiredField)) {
      return null;
    }
    try {
      String timeString = changeEvent.get(key).asText();
      return Timestamp.parseTimestamp(timeString);
    } catch (Exception e) {
      throw new DataChangeRecordConvertorException(
          "Unable to convert field " + key + " to Timestamp", e);
    }
  }

  public static Date toDate(JsonNode changeEvent, String key, boolean requiredField)
      throws DataChangeRecordConvertorException {

    if (!containsValue(changeEvent, key, requiredField)) {
      return null;
    }
    try {
      String dateString = changeEvent.get(key).asText();
      return Date.parseDate(dateString);
    } catch (Exception e) {
      throw new DataChangeRecordConvertorException(
          "Unable to convert field " + key + " to Date", e);
    }
  }

  private static boolean isNumeric(String str) {
    return NUMERIC_PATTERN.matcher(str).matches(); // match a number with optional '-' and decimal.
  }

  /*
   * This function converts the JSON field to BigDecimal. In addition, this function also checks
   * if the field is a number.
   */
  public static BigDecimal toNumericBigDecimal(
      JsonNode changeEvent, String key, boolean requiredField)
      throws DataChangeRecordConvertorException {

    String value = toString(changeEvent, key, requiredField);
    if (value == null) {
      return null;
    }
    if (NumberUtils.isCreatable(value) || NumberUtils.isParsable(value) || isNumeric(value)) {
      return new BigDecimal(value).setScale(9, RoundingMode.HALF_UP);
    }
    throw new DataChangeRecordConvertorException(
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
      throws DataChangeRecordConvertorException {
    boolean containsValue = changeEvent.hasNonNull(key);
    if (requiredField && !containsValue) {
      throw new DataChangeRecordConvertorException(
          "Required key " + key + " not found in change event");
    }
    return containsValue;
  }
}
