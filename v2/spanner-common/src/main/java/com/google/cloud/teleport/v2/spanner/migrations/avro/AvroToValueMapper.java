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
package com.google.cloud.teleport.v2.spanner.migrations.avro;

import static com.google.cloud.teleport.v2.spanner.migrations.avro.GenericRecordTypeConvertor.CUSTOM_TRANSFORMATION_AVRO_SCHEMA;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.AvroTypeConvertorException;
import com.google.cloud.teleport.v2.spanner.type.Type;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.math.NumberUtils;

/**
 * Provides a mapping from Spanner dialects and types to functions that convert Avro record values
 * to corresponding Spanner `Value` objects.
 *
 * <p>This class uses a nested map structure where the outer map keys are {@link Dialect} enums, and
 * the inner map keys are {@link Type} enums. The values in the inner maps are {@link
 * AvroToValueFunction} instances that perform the actual conversion.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * Dialect dialect = Dialect.POSTGRESQL;
 * Type type = Type.pgInt8();
 * Object recordValue = ...; // Avro record value
 * Schema fieldSchema = ...; // Avro field schema
 *
 * Value value = AvroToValueMapper.convertorMap()
 *     .get(dialect)
 *     .get(type)
 *     .apply(recordValue, fieldSchema);
 * }</pre>
 */
public class AvroToValueMapper {

  interface AvroToValueFunction {
    Value apply(Object recordValue, Schema fieldSchema);
  }

  /**
   * Extract value compatible with {@link Value} interface from avro.
   *
   * @param <T> type of the value.
   */
  interface AvroValueExtractor<T> {
    T extract(Object recordValue, Schema fieldSchema);
  }

  static final Map<Dialect, Map<Type, AvroToValueFunction>> CONVERTOR_MAP = initConvertorMap();

  public static Map<Dialect, Map<Type, AvroToValueFunction>> convertorMap() {
    return CONVERTOR_MAP;
  }

  static Map<Dialect, Map<Type, AvroToValueFunction>> initConvertorMap() {
    Map<Dialect, Map<Type, AvroToValueFunction>> convertorMap = new HashMap<>();
    convertorMap.put(Dialect.GOOGLE_STANDARD_SQL, getGsqlMap());
    convertorMap.put(Dialect.POSTGRESQL, getPgMap());
    return convertorMap;
  }

  /* TODO refactor this to avoid redundancy in Value.Foo and Value.FooArray */
  static Map<Type, AvroToValueFunction> getGsqlMap() {
    Map<Type, AvroToValueFunction> gsqlFunctions = new HashMap<>();
    gsqlFunctions.put(
        Type.bool(),
        (recordValue, fieldSchema) -> Value.bool(avroFieldToBoolean(recordValue, fieldSchema)));
    gsqlFunctions.put(
        Type.array(Type.bool()),
        (recordValue, fieldSchema) ->
            Value.boolArray(
                avroArrayFieldToSpannerArray(
                    recordValue, fieldSchema, AvroToValueMapper::avroFieldToBoolean)));

    gsqlFunctions.put(
        Type.int64(),
        (recordValue, fieldSchema) -> Value.int64(avroFieldToLong(recordValue, fieldSchema)));
    gsqlFunctions.put(
        Type.array(Type.int64()),
        (recordValue, fieldSchema) ->
            Value.int64Array(
                avroArrayFieldToSpannerArray(
                    recordValue, fieldSchema, AvroToValueMapper::avroFieldToLong)));

    gsqlFunctions.put(
        Type.float32(),
        (recordValue, fieldSchema) -> Value.float32(avroFieldToFloat32(recordValue, fieldSchema)));
    gsqlFunctions.put(
        Type.array(Type.float32()),
        (recordValue, fieldSchema) ->
            Value.float32Array(
                avroArrayFieldToSpannerArray(
                    recordValue, fieldSchema, AvroToValueMapper::avroFieldToFloat32)));

    gsqlFunctions.put(
        Type.float64(),
        (recordValue, fieldSchema) -> Value.float64(avroFieldToDouble(recordValue, fieldSchema)));
    gsqlFunctions.put(
        Type.array(Type.float64()),
        (recordValue, fieldSchema) ->
            Value.float64Array(
                avroArrayFieldToSpannerArray(
                    recordValue, fieldSchema, AvroToValueMapper::avroFieldToDouble)));

    gsqlFunctions.put(
        Type.string(),
        (recordValue, fieldSchema) -> Value.string(avroFieldToString(recordValue, fieldSchema)));
    gsqlFunctions.put(
        Type.array(Type.string()),
        (recordValue, fieldSchema) ->
            Value.stringArray(
                avroArrayFieldToSpannerArray(
                    recordValue, fieldSchema, AvroToValueMapper::avroFieldToString)));

    gsqlFunctions.put(
        Type.json(),
        (recordValue, fieldSchema) -> Value.string(avroFieldToString(recordValue, fieldSchema)));
    gsqlFunctions.put(
        Type.array(Type.json()),
        (recordValue, fieldSchema) ->
            Value.jsonArray(
                avroArrayFieldToSpannerArray(
                    recordValue, fieldSchema, AvroToValueMapper::avroFieldToString)));

    gsqlFunctions.put(
        Type.numeric(),
        (recordValue, fieldSchema) ->
            Value.numeric(avroFieldToNumericBigDecimal(recordValue, fieldSchema)));
    gsqlFunctions.put(
        Type.array(Type.numeric()),
        (recordValue, fieldSchema) ->
            Value.numericArray(
                avroArrayFieldToSpannerArray(
                    recordValue, fieldSchema, AvroToValueMapper::avroFieldToNumericBigDecimal)));

    gsqlFunctions.put(
        Type.bytes(),
        (recordValue, fieldSchema) -> Value.bytes(avroFieldToByteArray(recordValue, fieldSchema)));
    gsqlFunctions.put(
        Type.array(Type.bytes()),
        (recordValue, fieldSchema) ->
            Value.bytesArray(
                avroArrayFieldToSpannerArray(
                    recordValue, fieldSchema, AvroToValueMapper::avroFieldToByteArray)));

    gsqlFunctions.put(
        Type.timestamp(),
        (recordValue, fieldSchema) ->
            Value.timestamp(avroFieldToTimestamp(recordValue, fieldSchema)));
    gsqlFunctions.put(
        Type.array(Type.timestamp()),
        (recordValue, fieldSchema) ->
            Value.timestampArray(
                avroArrayFieldToSpannerArray(
                    recordValue, fieldSchema, AvroToValueMapper::avroFieldToTimestamp)));

    gsqlFunctions.put(
        Type.date(),
        (recordValue, fieldSchema) -> Value.date(avroFieldToDate(recordValue, fieldSchema)));
    gsqlFunctions.put(
        Type.array(Type.date()),
        (recordValue, fieldSchema) ->
            Value.dateArray(
                avroArrayFieldToSpannerArray(
                    recordValue, fieldSchema, AvroToValueMapper::avroFieldToDate)));

    return gsqlFunctions;
  }

  /* TODO Support for AvroArrays to PG */
  static Map<Type, AvroToValueFunction> getPgMap() {
    Map<Type, AvroToValueFunction> pgFunctions = new HashMap<>();
    pgFunctions.put(
        Type.pgBool(),
        (recordValue, fieldSchema) -> Value.bool(avroFieldToBoolean(recordValue, fieldSchema)));
    pgFunctions.put(
        Type.pgInt8(),
        (recordValue, fieldSchema) -> Value.int64(avroFieldToLong(recordValue, fieldSchema)));
    pgFunctions.put(
        Type.pgFloat8(),
        (recordValue, fieldSchema) -> Value.float64(avroFieldToDouble(recordValue, fieldSchema)));
    pgFunctions.put(
        Type.pgVarchar(),
        (recordValue, fieldSchema) -> Value.string(avroFieldToString(recordValue, fieldSchema)));
    pgFunctions.put(
        Type.pgText(),
        (recordValue, fieldSchema) -> Value.string(avroFieldToString(recordValue, fieldSchema)));
    pgFunctions.put(
        Type.pgJsonb(),
        (recordValue, fieldSchema) -> Value.string(avroFieldToString(recordValue, fieldSchema)));
    pgFunctions.put(
        Type.pgNumeric(),
        (recordValue, fieldSchema) ->
            Value.numeric(avroFieldToNumericBigDecimal(recordValue, fieldSchema)));
    pgFunctions.put(
        Type.pgBytea(),
        (recordValue, fieldSchema) -> Value.bytes(avroFieldToByteArray(recordValue, fieldSchema)));
    pgFunctions.put(
        Type.pgCommitTimestamp(),
        (recordValue, fieldSchema) ->
            Value.timestamp(avroFieldToTimestamp(recordValue, fieldSchema)));
    pgFunctions.put(
        Type.pgTimestamptz(),
        (recordValue, fieldSchema) ->
            Value.timestamp(avroFieldToTimestamp(recordValue, fieldSchema)));
    pgFunctions.put(
        Type.pgDate(),
        (recordValue, fieldSchema) -> Value.date(avroFieldToDate(recordValue, fieldSchema)));
    return pgFunctions;
  }

  /**
   * This method tries to map different kinds of source types to a boolean. This could be longs,
   * string as well as booleans.
   */
  static Boolean avroFieldToBoolean(Object recordValue, Schema fieldSchema) {
    if (recordValue == null) {
      return null;
    }
    String val = recordValue.toString();
    // If the value can be converted to a "0" or a "1", rely on this to map to boolean. This could
    // be used for strings and long data types.
    // For ex: BIT(1) -> Unified type long -> Spanner Boolean uses this path.
    if (Arrays.asList("0", "1").contains(val)) {
      return val.equals("1");
    }
    // Rely on booleanUtils to map the string to a bool if it was not a "0" or "1". This handles
    // cases like True, true, t, f etc.
    return BooleanUtils.toBoolean(val);
  }

  static Long avroFieldToLong(Object recordValue, Schema fieldSchema) {
    try {
      if (recordValue == null) {
        return null;
      }
      return Long.parseLong(recordValue.toString());
    } catch (Exception e) {
      throw new AvroTypeConvertorException(
          "Unable to convert "
              + fieldSchema.getType()
              + " to Long, with value: "
              + recordValue
              + ", Exception: "
              + e.getMessage());
    }
  }

  static Float avroFieldToFloat32(Object recordValue, Schema fieldSchema) {
    try {
      if (recordValue == null) {
        return null;
      }
      return Float.valueOf(recordValue.toString());
    } catch (Exception e) {
      throw new AvroTypeConvertorException(
          "Unable to convert "
              + fieldSchema.getType()
              + " to float, with value: "
              + recordValue
              + ", Exception: "
              + e.getMessage());
    }
  }

  static Double avroFieldToDouble(Object recordValue, Schema fieldSchema) {
    try {
      if (recordValue == null) {
        return null;
      }
      return Double.valueOf(recordValue.toString());
    } catch (Exception e) {
      throw new AvroTypeConvertorException(
          "Unable to convert "
              + fieldSchema.getType()
              + " to double, with value: "
              + recordValue
              + ", Exception: "
              + e.getMessage());
    }
  }

  static String avroFieldToString(Object recordValue, Schema fieldSchema) {
    try {
      if (recordValue == null) {
        return null;
      }
      return recordValue.toString();
    } catch (Exception e) {
      throw new AvroTypeConvertorException(
          "Unable to convert "
              + String.valueOf(fieldSchema.getType())
              + " to string, Exception: "
              + e.getMessage());
    }
  }

  static BigDecimal avroFieldToNumericBigDecimal(Object recordValue, Schema fieldSchema) {
    try {
      if (recordValue == null) {
        return null;
      }
      String value = recordValue.toString();
      if (NumberUtils.isCreatable(value) || NumberUtils.isParsable(value) || isNumeric(value)) {
        return new BigDecimal(value).setScale(9, RoundingMode.HALF_UP);
      } else {
        throw new AvroTypeConvertorException(
            "Unable to convert field "
                + value
                + " as isCreatable("
                + value
                + ") = "
                + NumberUtils.isCreatable(value)
                + ", isParsable("
                + value
                + ") = "
                + NumberUtils.isParsable(value)
                + ".");
      }
    } catch (Exception e) {
      throw new AvroTypeConvertorException(
          "Unable to convert "
              + fieldSchema.getType()
              + " to numeric big decimal, with value: "
              + recordValue
              + ", Exception: "
              + e.getMessage());
    }
  }

  private static boolean isNumeric(String str) {
    return Pattern.compile("-?\\d+(\\.\\d+)?")
        .matcher(str)
        .matches(); // match a number with optional '-' and decimal.
  }

  static ByteArray avroFieldToByteArray(Object recordValue, Schema fieldSchema) {
    try {
      if (recordValue == null) {
        return null;
      }
      // BIT types are by default mapped to ByteArray by SMT and the unified type is LONG.
      if (fieldSchema.getType().equals(Schema.Type.LONG)) {
        BigInteger bigInt = BigInteger.valueOf(Long.valueOf(recordValue.toString()));
        return ByteArray.copyFrom(bigInt.toByteArray());
      }

      if (fieldSchema.getType().equals(Schema.Type.STRING)) {
        // For string avro type, expect hex encoded string.
        String s = recordValue.toString();
        if (s.length() % 2 == 1) {
          s = "0" + s;
        }
        return ByteArray.copyFrom(Hex.decodeHex(s));
      }
      return ByteArray.copyFrom(((ByteBuffer) recordValue).array());
    } catch (Exception e) {
      throw new AvroTypeConvertorException(
          "Unable to convert "
              + fieldSchema.getType()
              + " to byte array, with value: "
              + recordValue
              + ", Exception: "
              + e.getMessage());
    }
  }

  static com.google.cloud.Timestamp avroFieldToTimestamp(Object recordValue, Schema fieldSchema) {
    try {
      if (recordValue == null) {
        return null;
      }
      return com.google.cloud.Timestamp.parseTimestamp(recordValue.toString());
    } catch (Exception e) {
      throw new AvroTypeConvertorException(
          "Unable to convert "
              + fieldSchema.getName()
              + " to Timestamp, with value: "
              + recordValue
              + ", Exception: "
              + e.getMessage());
    }
  }

  static com.google.cloud.Date avroFieldToDate(Object recordValue, Schema fieldSchema) {
    try {
      if (recordValue == null) {
        return null;
      }
      return Date.fromJavaUtilDate(parseLenientDate(recordValue.toString()));
    } catch (Exception e) {
      throw new AvroTypeConvertorException(
          "Unable to convert "
              + fieldSchema.getName()
              + " to Date, with value: "
              + recordValue
              + ", Exception: "
              + e.getMessage());
    }
  }

  /**
   * Maps an Avro Array to the expected Array type on Spanner.
   *
   * @param recordValue value of the array field.
   * @param fieldSchema schema of the array type.
   * @param valueExtractor - Converter for converting avro field to expected type of Spanner's array
   *     element.
   * @return - Iterable for the spanner mapped values.
   * @param <T> - Expected Type of Spanner's array element.
   * @throws AvroTypeConvertorException - For any conversion exception.
   */
  static <T> Iterable<T> avroArrayFieldToSpannerArray(
      Object recordValue, Schema fieldSchema, AvroValueExtractor<T> valueExtractor) {
    Schema elementSchema =
        (fieldSchema.equals(CUSTOM_TRANSFORMATION_AVRO_SCHEMA))
            ? CUSTOM_TRANSFORMATION_AVRO_SCHEMA
            : SchemaBuilder.builder().type(fieldSchema.getElementType());
    List<T> recordArrayList = new ArrayList<T>();
    try {
      if (recordValue == null) {
        return null;
      }
      for (int i = 0; i < Array.getLength(recordValue); i++) {
        recordArrayList.add(valueExtractor.extract(Array.get(recordValue, i), elementSchema));
      }
      return recordArrayList;

    } catch (Exception e) {
      throw new AvroTypeConvertorException(
          "Unable to convert "
              + fieldSchema.getName()
              + " to Array, with value: "
              + recordValue
              + ", Exception: "
              + e.getMessage());
    }
  }

  private static java.util.Date parseLenientDate(String date) {
    try {
      return parseDate(date);
    } catch (DateTimeParseException e) {
      /* Exception due to wrong format. Try parsing as Timestamp and extract date.
       */
      ZonedDateTime zonedDateTime = convertToZonedDateTime(date);
      return parseDate(zonedDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE));
    }
  }

  private static java.util.Date parseDate(String date) {
    LocalDate localDate = LocalDate.parse(date, DateTimeFormatter.ISO_LOCAL_DATE);
    return java.sql.Date.valueOf(localDate);
  }

  private static ZonedDateTime convertToZonedDateTime(String timestamp) {
    ZonedDateTime zonedDateTime;
    try {
      zonedDateTime =
          ZonedDateTime.parse(timestamp, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
              .withZoneSameInstant(ZoneId.of("UTC"));
    } catch (DateTimeParseException e) {
      if (!timestamp.endsWith("Z")) {

        // Datastream replication in JSON format does not contain 'Z' at the end of timestamp.
        timestamp = timestamp + "Z";
        zonedDateTime =
            ZonedDateTime.parse(timestamp, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                .withZoneSameInstant(ZoneId.of("UTC"));
      } else {
        throw e;
      }
    }
    return zonedDateTime;
  }
}
