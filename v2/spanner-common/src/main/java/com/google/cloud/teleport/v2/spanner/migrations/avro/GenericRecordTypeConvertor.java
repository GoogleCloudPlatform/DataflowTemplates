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

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.type.Type;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convertor Class containing methods for type conversion of various AvroTypes to Spanner {@link
 * Value} types.
 */
public class GenericRecordTypeConvertor {
  private static final Logger LOG = LoggerFactory.getLogger(GenericRecordTypeConvertor.class);

  private final ISchemaMapper schemaMapper;

  private final String namespace;

  public GenericRecordTypeConvertor(ISchemaMapper schemaMapper, String namespace) {
    this.schemaMapper = schemaMapper;
    this.namespace = namespace;
  }

  /**
   * This method takes in a generic record and returns a map between the Spanner column name and the
   * corresponding Spanner column value. This handles the data conversion logic from a GenericRecord
   * field to a Map of Spanner column name to spanner Value.
   */
  public Map<String, Value> transformChangeEvent(GenericRecord record, String srcTableName) {
    Map<String, Value> result = new HashMap<>();
    String spannerTableName = schemaMapper.getSpannerTableName(namespace, srcTableName);
    List<String> spannerColNames = schemaMapper.getSpannerColumns(namespace, spannerTableName);
    for (String spannerColName : spannerColNames) {
      /**
       * TODO: Handle columns that will not exist at source - synth id - shard id - multi-column
       * transformations - auto-gen keys - Default columns - generated columns
       */
      try {
        String srcColName =
            schemaMapper.getSourceColumnName(namespace, spannerTableName, spannerColName);
        Type spannerColumnType =
            schemaMapper.getSpannerColumnType(namespace, spannerTableName, spannerColName);
        LOG.debug(
            "Transformer processing srcCol: {} spannerColumnType:{}",
            srcColName,
            spannerColumnType);

        Value value =
            getSpannerValue(
                record.get(srcColName),
                record.getSchema().getField(srcColName).schema(),
                srcColName,
                spannerColumnType);
        result.put(spannerColName, value);
      } catch (NullPointerException e) {
        LOG.error("Unable to transform change event", e);
        throw e;
      } catch (IllegalArgumentException e) {
        LOG.error("Unable to transform change event", e);
        throw e;
      } catch (Exception e) {
        LOG.error(
            String.format("Unable to convert spanner value for spanner col: %s", spannerColName),
            e);
        throw new RuntimeException(
            String.format("Unable to convert spanner value for spanner col: %s", spannerColName),
            e);
      }
    }
    return result;
  }

  /** Extract the field value from Generic Record and try to convert it to @spannerType. */
  public Value getSpannerValue(
      Object recordValue, Schema fieldSchema, String recordColName, Type spannerType) {
    // Logical and record types should be converted to string.
    LOG.debug(
        "gettingSpannerValue for recordValue: {}, fieldSchema: {}, recordColName: {}, spannerType: {}",
        recordColName,
        recordValue,
        fieldSchema,
        spannerType);
    if (fieldSchema.getType().equals(Schema.Type.UNION)) {
      List<Schema> types = fieldSchema.getTypes();
      LOG.debug("found union type: {}", types);
      // Schema types can only union with Type NULL. Any other UNION is unsupported.
      if (types.size() == 2 && types.stream().anyMatch(s -> s.getType().equals(Schema.Type.NULL))) {
        fieldSchema =
            types.stream().filter(s -> !s.getType().equals(Schema.Type.NULL)).findFirst().get();
      } else {
        throw new IllegalArgumentException(
            String.format(
                "Unknown schema field type %s for field %s with value %s.",
                fieldSchema, recordColName, recordValue));
      }
    }
    if (fieldSchema.getLogicalType() != null) {
      recordValue = handleLogicalFieldType(recordColName, recordValue, fieldSchema);
    } else if (fieldSchema.getType().equals(Schema.Type.RECORD)) {
      // Get the avro field of type record from the whole record.
      recordValue = handleRecordFieldType(recordColName, (GenericRecord) recordValue, fieldSchema);
    }
    LOG.debug("Updated record value is {} for recordColName {}", recordValue, recordColName);
    Dialect dialect = schemaMapper.getDialect();
    if (dialect == null) {
      throw new NullPointerException("schemaMapper returned null spanner dialect.");
    }
    if (AvroToValueMapper.convertorMap().get(dialect).containsKey(spannerType)) {
      return AvroToValueMapper.convertorMap()
          .get(dialect)
          .get(spannerType)
          .apply(recordValue, fieldSchema);
    } else {
      throw new IllegalArgumentException(
          "Found unsupported Spanner column type("
              + spannerType.getCode()
              + ") for column "
              + recordColName);
    }
  }

  static class CustomAvroTypes {
    public static final String VARCHAR = "varchar";
    public static final String NUMBER = "number";
    public static final String JSON = "json";
  }

  /** Avro logical types are converted to an equivalent string type. */
  static String handleLogicalFieldType(String fieldName, Object recordValue, Schema fieldSchema) {
    LOG.debug("found logical type for col {} with schema {}", fieldName, fieldSchema);
    if (recordValue == null) {
      return null;
    }
    if (fieldSchema.getLogicalType() instanceof LogicalTypes.Date) {
      TimeConversions.DateConversion dataConversion = new TimeConversions.DateConversion();
      LocalDate date =
          dataConversion.fromInt(
              Integer.valueOf(recordValue.toString()), fieldSchema, fieldSchema.getLogicalType());
      return date.format(DateTimeFormatter.ISO_LOCAL_DATE);
    } else if (fieldSchema.getLogicalType() instanceof LogicalTypes.Decimal) {
      Conversions.DecimalConversion decimalConversion = new Conversions.DecimalConversion();
      BigDecimal bigDecimal =
          decimalConversion.fromBytes(
              (ByteBuffer) recordValue, fieldSchema, fieldSchema.getLogicalType());
      return bigDecimal.toPlainString();
    } else if (fieldSchema.getLogicalType() instanceof LogicalTypes.TimeMicros) {
      Long nanoseconds = Long.valueOf(recordValue.toString()) * TimeUnit.MICROSECONDS.toNanos(1);
      return LocalTime.ofNanoOfDay(nanoseconds).format(DateTimeFormatter.ISO_LOCAL_TIME);
    } else if (fieldSchema.getLogicalType() instanceof LogicalTypes.TimeMillis) {
      Long nanoseconds = TimeUnit.MILLISECONDS.toNanos(Long.valueOf(recordValue.toString()));
      return LocalTime.ofNanoOfDay(nanoseconds).format(DateTimeFormatter.ISO_LOCAL_TIME);
    } else if (fieldSchema.getLogicalType() instanceof LogicalTypes.TimestampMicros) {
      Long nanoseconds = Long.valueOf(recordValue.toString()) * TimeUnit.MICROSECONDS.toNanos(1);
      Instant timestamp =
          Instant.ofEpochSecond(
              TimeUnit.NANOSECONDS.toSeconds(nanoseconds),
              nanoseconds % TimeUnit.SECONDS.toNanos(1));
      return timestamp.atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    } else if (fieldSchema.getLogicalType() instanceof LogicalTypes.TimestampMillis) {
      Instant timestamp = Instant.ofEpochMilli(Long.valueOf(recordValue.toString()));
      return timestamp.atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    } else if (fieldSchema.getLogicalType() != null
        && fieldSchema.getLogicalType().getName().equals(CustomAvroTypes.JSON)) {
      return recordValue.toString();
    } else if (fieldSchema.getLogicalType() != null
        && fieldSchema.getLogicalType().getName().equals(CustomAvroTypes.NUMBER)) {
      return recordValue.toString();
    } else if (fieldSchema.getLogicalType() != null
        && fieldSchema.getLogicalType().getName().equals(CustomAvroTypes.VARCHAR)) {
      return recordValue.toString();
    } else {
      LOG.error("Unknown field type {} for field {} in {}.", fieldSchema, fieldName, recordValue);
      throw new UnsupportedOperationException(
          String.format(
              "Unknown field type %s for field %s in %s.", fieldSchema, fieldName, recordValue));
    }
  }

  /** Record field types are converted to an equivalent string type. */
  static String handleRecordFieldType(String fieldName, GenericRecord element, Schema fieldSchema) {
    LOG.debug("found record type for col {} with schema: {}", fieldName, fieldSchema);
    if (element == null) {
      return null;
    }
    if (fieldSchema.getName().equals("timestampTz")) {
      Long nanoseconds =
          Long.valueOf(element.get("timestamp").toString()) * TimeUnit.MICROSECONDS.toNanos(1);
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
      totalMicros += Long.valueOf(element.get("time").toString());
      Instant timestamp = Instant.ofEpochSecond(TimeUnit.MICROSECONDS.toSeconds(totalMicros));
      return timestamp.atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    } else if (fieldSchema.getName().equals("interval")) {
      // TODO: For MySQL, we ignore the months field. This might require source-specific handling
      // when PG is supported.
      String hours = element.get("hours").toString();
      Long totalMicros = Long.valueOf(element.get("micros").toString());
      if (totalMicros >= TimeUnit.MINUTES.toMicros(60)) {
        throw new IllegalArgumentException(
            String.format(
                "found duration %s for interval type, micros field. This field should be strictly less than 60 minutes.",
                totalMicros));
      }
      String localTime =
          LocalTime.ofNanoOfDay(TimeUnit.MICROSECONDS.toNanos(totalMicros))
              .format(DateTimeFormatter.ISO_LOCAL_TIME);
      // Handle hours separately since that can also be negative. We convert micros to localTime
      // format (HH:MM:SS), then strip of HH:, which will always be "00:".
      return String.format("%s:%s", hours, localTime.substring(3));
    } else {
      throw new UnsupportedOperationException(
          String.format(
              "Unknown field schema %s for 'Record' type in %s for field %s",
              fieldSchema.getName(), element, fieldName));
    }
  }
}
