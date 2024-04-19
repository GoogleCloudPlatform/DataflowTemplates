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

import com.google.auto.value.AutoValue;
import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.AvroTypeConvertorException;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.type.Type;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convertor Class containing methods for type conversion of various AvroTypes to Spanner {@link
 * Value} types.
 */
@AutoValue
public abstract class GenericRecordTypeConvertor {
  private static final Logger LOG = LoggerFactory.getLogger(GenericRecordTypeConvertor.class);

  public abstract ISchemaMapper iSchemaMapper();

  public abstract String namespace();

  public static GenericRecordTypeConvertor create(ISchemaMapper iSchemaMapper, String namespace) {
    return new AutoValue_GenericRecordTypeConvertor(iSchemaMapper, namespace);
  }

  /**
   * This method takes in a generic record and returns a map between the Spanner column name and the
   * corresponding Spanner column value. This handles the data conversion logic from a GenericRecord
   * field to a spanner Value.
   */
  public Map<String, Value> transformChangeEvent(GenericRecord record, String srcTableName) {
    Map<String, Value> result = new HashMap<>();
    String spannerTableName = iSchemaMapper().getSpannerTableName(namespace(), srcTableName);
    List<String> spannerColNames = iSchemaMapper().getSpannerColumns(namespace(), spannerTableName);
    for (String spannerColName : spannerColNames) {
      /**
       * TODO: Handle columns that will not exist at source - synth id - shard id - multi-column
       * transformations - auto-gen keys - Default columns - generated columns
       */
      String srcColName =
          iSchemaMapper().getSourceColumnName(namespace(), spannerTableName, spannerColName);
      Type spannerColumnType =
          iSchemaMapper().getSpannerColumnType(namespace(), spannerTableName, spannerColName);
      Value value = getSpannerValue(record, srcColName, spannerColumnType);
      result.put(spannerColName, value);
    }
    return result;
  }

  /** Extract the field value from Generic Record and try to convert it to @spannerType. */
  static Value getSpannerValue(GenericRecord record, String recordColName, Type spannerType) {
    Object recordValue = record.get(recordColName);
    Schema fieldSchema = record.getSchema().getField(recordColName).schema();
    if (fieldSchema.getLogicalType() != null) {
      // Logical should be converted to string.
      recordValue = handleLogicalFieldType(recordColName, record);
    } else if (fieldSchema.getType().equals(Schema.Type.RECORD)) {
      // Record types should be converted to string.
      recordValue = handleRecordFieldType(recordColName, record);
    }
    Schema.Type fieldType = fieldSchema.getType();
    switch (spannerType.getCode()) {
      case BOOL:
      case PG_BOOL:
        return Value.bool(avroFieldToBoolean(recordValue, fieldType));
      case INT64:
      case PG_INT8:
        return Value.int64(avroFieldToLong(recordValue, fieldType));
      case FLOAT64:
      case PG_FLOAT8:
        return Value.float64(avroFieldToDouble(recordValue, fieldType));
      case STRING:
      case PG_VARCHAR:
      case PG_TEXT:
      case JSON:
      case PG_JSONB:
        return Value.string(recordValue.toString());
      case NUMERIC:
      case PG_NUMERIC:
        return Value.numeric(avroFieldToNumericBigDecimal(recordValue, fieldType));
      case BYTES:
      case PG_BYTEA:
        return Value.bytes(avroFieldToByteArray(recordValue, fieldType));
      case TIMESTAMP:
      case PG_COMMIT_TIMESTAMP:
      case PG_TIMESTAMPTZ:
        return Value.timestamp(avroFieldToTimestamp(recordValue, fieldSchema));
      case DATE:
      case PG_DATE:
        return Value.date(avroFieldToDate(recordValue, fieldSchema));
      default:
        throw new IllegalArgumentException(
            "Found unsupported Spanner column type("
                + spannerType.getCode()
                + ") for column "
                + recordColName);
    }
  }

  static Boolean avroFieldToBoolean(Object recordValue, Schema.Type type) {
    if (recordValue == null) {
      return null;
    }
    // BooleanUtils.toBoolean() never throws exception, so we don't need to catch it.
    return BooleanUtils.toBoolean(recordValue.toString());
  }

  static Long avroFieldToLong(Object recordValue, Schema.Type type) {
    try {
      if (recordValue == null) {
        return null;
      }
      return Long.parseLong(recordValue.toString());
    } catch (Exception e) {
      throw new AvroTypeConvertorException(
          "Unable to convert "
              + type
              + " to Long, with value: "
              + recordValue
              + ", Exception: "
              + e.getMessage());
    }
  }

  static Double avroFieldToDouble(Object recordValue, Schema.Type type) {
    try {
      if (recordValue == null) {
        return null;
      }
      return Double.valueOf(recordValue.toString());
    } catch (Exception e) {
      throw new AvroTypeConvertorException(
          "Unable to convert "
              + type
              + " to double, with value: "
              + recordValue
              + ", Exception: "
              + e.getMessage());
    }
  }

  static BigDecimal avroFieldToNumericBigDecimal(Object recordValue, Schema.Type type) {
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
              + type
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

  static ByteArray avroFieldToByteArray(Object recordValue, Schema.Type type) {
    try {
      if (recordValue == null) {
        return null;
      }
      if (type.equals(Schema.Type.STRING)) {
        // For string avro type, expect hex encoded string.
        String s = recordValue.toString();
        if (s.length() % 2 == 1) {
          s = "0" + s;
        }
        return ByteArray.copyFrom(Hex.decodeHex(s));
      }
      return ByteArray.copyFrom((byte[]) recordValue);
    } catch (Exception e) {
      throw new AvroTypeConvertorException(
          "Unable to convert "
              + type
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

  static class CustomAvroTypes {
    public static final String VARCHAR = "varchar";
    public static final String NUMBER = "number";
    public static final String JSON = "json";
  }

  /** Avro logical types are converted to an equivalent string type. */
  static String handleLogicalFieldType(String fieldName, GenericRecord element) {
    if (element.get(fieldName) == null) {
      return null;
    }
    Schema fieldSchema = element.getSchema().getField(fieldName).schema();
    if (fieldSchema.getLogicalType() instanceof LogicalTypes.Date) {
      TimeConversions.DateConversion dataConversion = new TimeConversions.DateConversion();
      LocalDate date =
          dataConversion.fromInt(
              (Integer) element.get(fieldName), fieldSchema, fieldSchema.getLogicalType());
      return date.format(DateTimeFormatter.ISO_LOCAL_DATE);
    } else if (fieldSchema.getLogicalType() instanceof LogicalTypes.Decimal) {
      Conversions.DecimalConversion decimalConversion = new Conversions.DecimalConversion();
      BigDecimal bigDecimal =
          decimalConversion.fromBytes(
              (ByteBuffer) element.get(fieldName), fieldSchema, fieldSchema.getLogicalType());
      return bigDecimal.toPlainString();
    } else if (fieldSchema.getLogicalType() instanceof LogicalTypes.TimeMicros) {
      Long nanoseconds = (Long) element.get(fieldName) * TimeUnit.MICROSECONDS.toNanos(1);
      return LocalTime.ofNanoOfDay(nanoseconds).format(DateTimeFormatter.ISO_LOCAL_TIME);
    } else if (fieldSchema.getLogicalType() instanceof LogicalTypes.TimeMillis) {
      Long nanoseconds =
          TimeUnit.MILLISECONDS.toNanos(Long.valueOf(element.get(fieldName).toString()));
      return LocalTime.ofNanoOfDay(nanoseconds).format(DateTimeFormatter.ISO_LOCAL_TIME);
    } else if (fieldSchema.getLogicalType() instanceof LogicalTypes.TimestampMicros) {
      Long nanoseconds = (Long) element.get(fieldName) * TimeUnit.MICROSECONDS.toNanos(1);
      Instant timestamp =
          Instant.ofEpochSecond(
              TimeUnit.NANOSECONDS.toSeconds(nanoseconds),
              nanoseconds % TimeUnit.SECONDS.toNanos(1));
      return timestamp.atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    } else if (fieldSchema.getLogicalType() instanceof LogicalTypes.TimestampMillis) {
      Instant timestamp = Instant.ofEpochMilli(((Long) element.get(fieldName)));
      return timestamp.atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    } // TODO: add support for custom logical types VARCHAR, JSON and NUMBER once format is
    // finalised.
    else {
      LOG.error(
          "Unknown field type {} for field {} in {}. Ignoring it.",
          fieldSchema,
          fieldName,
          element.get(fieldName));
      throw new UnsupportedOperationException(
          String.format(
              "Unknown field type %s for field %s in %s.",
              fieldSchema, fieldName, element.get(fieldName)));
    }
  }

  /** Record field types are converted to an equivalent string type. */
  static String handleRecordFieldType(String fieldName, GenericRecord record) {
    if (record.get(fieldName) == null) {
      return null;
    }
    Schema fieldSchema = record.getSchema().getField(fieldName).schema();
    // Get the avro field of type record from the whole record.
    GenericRecord element = (GenericRecord) record.get(fieldName);
    if (fieldSchema.getName().equals("timestampTz")) {
      Long nanoseconds = (Long) element.get("timestamp") * TimeUnit.MICROSECONDS.toNanos(1);
      Instant timestamp =
          Instant.ofEpochSecond(
              TimeUnit.NANOSECONDS.toSeconds(nanoseconds),
              nanoseconds % TimeUnit.SECONDS.toNanos(1));
      // Offset comes in milliseconds
      ZoneOffset offset =
          ZoneOffset.ofTotalSeconds(Integer.valueOf(element.get("offset").toString()) / 1000);
      ZonedDateTime fullDate = timestamp.atOffset(offset).toZonedDateTime();
      // Convert to UTC.
      return fullDate
          .withZoneSameInstant(ZoneId.of("UTC"))
          .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    } else if (fieldSchema.getName().equals("datetime")) {
      // Convert to timestamp string.
      Long totalMicros = TimeUnit.DAYS.toMicros(Long.valueOf(element.get("date").toString()));
      totalMicros += (Long) element.get("time");
      Instant timestamp = Instant.ofEpochSecond(TimeUnit.MICROSECONDS.toSeconds(totalMicros));
      return timestamp.atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    }
    // TODO: Add support for INTERVAL type when format is finalised.
    else {
      throw new UnsupportedOperationException(
          String.format(
              "Unknown field schema %s for 'Record' type in %s for field %s",
              fieldSchema.getName(), element, fieldName));
    }
  }
}
