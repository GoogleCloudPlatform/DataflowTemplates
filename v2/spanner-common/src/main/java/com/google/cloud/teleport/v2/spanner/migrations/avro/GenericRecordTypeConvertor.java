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
import com.google.cloud.teleport.v2.spanner.exceptions.InvalidTransformationException;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.type.Type;
import com.google.cloud.teleport.v2.spanner.utils.ISpannerMigrationTransformer;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationRequest;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationResponse;
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
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.kerby.util.Hex;
import org.joda.time.Duration;
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

  private final String shardId;

  private final ISpannerMigrationTransformer customTransformer;

  static final String LOGICAL_TYPE = "logicalType";

  private static final Schema CUSTOM_TRANSFORMATION_AVRO_SCHEMA =
      new LogicalType("custom_transform").addToSchema(SchemaBuilder.builder().stringType());

  private final Distribution applyCustomTransformationResponseTimeMetric =
      Metrics.distribution(
          GenericRecordTypeConvertor.class, "apply_custom_transformation_impl_latency_ms");

  public GenericRecordTypeConvertor(
      ISchemaMapper schemaMapper,
      String namespace,
      String shardId,
      ISpannerMigrationTransformer customTransformer) {
    this.schemaMapper = schemaMapper;
    this.namespace = namespace;
    this.shardId = shardId;
    this.customTransformer = customTransformer;
  }

  /**
   * This method takes in a generic record and returns a map between the Spanner column name and the
   * corresponding Spanner column value. This handles the data conversion logic from a GenericRecord
   * field to a Map of Spanner column name to spanner Value.
   *
   * <p>This method can return 'null' which indicates the change event needs to be skipped.
   */
  public Map<String, Value> transformChangeEvent(GenericRecord record, String srcTableName)
      throws InvalidTransformationException {
    Map<String, Value> result = new HashMap<>();
    result = populateCustomTransformations(result, record, srcTableName);
    // If the row needs to be filtered.
    if (result == null) {
      LOG.debug(
          "Filtered out row based on Customer Transformation response for table {}, record {}",
          srcTableName,
          record);
      return null;
    }
    String spannerTableName = schemaMapper.getSpannerTableName(namespace, srcTableName);
    List<String> spannerColNames = schemaMapper.getSpannerColumns(namespace, spannerTableName);
    // This is null/blank for identity/non-sharded cases.
    String shardIdCol = schemaMapper.getShardIdColumnName(namespace, spannerTableName);
    for (String spannerColName : spannerColNames) {
      try {
        // Skip if the column was already populated by custom transformation.
        if (result.containsKey(spannerColName)) {
          continue;
        }
        // If current column is migration shard id, populate value.
        if (spannerColName.equals(shardIdCol)) {
          result = populateShardId(result, shardIdCol);
          continue;
        }

        // For session based mapper, populate synthetic primary key with UUID. For identity mapper,
        // the schemaMapper returns null.
        if (spannerColName.equals(
            schemaMapper.getSyntheticPrimaryKeyColName(namespace, spannerTableName))) {
          result.put(spannerColName, Value.string(getUUID()));
          continue;
        }

        // If a Spanner column does not exist in the source data, there are several possible
        // explanations:
        // 1. The column might be an auto-value column in Spanner, such as generated column,
        // default, auto-gen keys.
        // 2. Column was supposed to be populated by custom transform, but user error missed this
        // column during custom transform.
        // 3. The column might have been accidentally left over in the Spanner column without the
        // right handling.
        // In all of these cases, we omit this column from the Spanner mutation and user errors will
        // fail on Spanner. The writer's Dead Letter Queue (DLQ) is responsible for catching any
        // misconfigurations  where a required column is missing.
        if (!(schemaMapper.colExistsAtSource(namespace, spannerTableName, spannerColName)
            && record.hasField(
                schemaMapper.getSourceColumnName(namespace, spannerTableName, spannerColName)))) {
          continue;
        }

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
        throw e;
      } catch (IllegalArgumentException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Unable to convert spanner value for spanner col: %s", spannerColName),
            e);
      }
    }
    return result;
  }

  private String getUUID() {
    return UUID.randomUUID().toString();
  }

  /**
   * Applies custom transformations to the source row (Generic Record) and returns a Map of Spanner
   * Columns to values to be overwritten.
   *
   * <p>This method leverages a customTransformer to modify the data, potentially filtering out
   * records or adding new columns.
   *
   * @param result The initial map of column names to Spanner values. This map will be modified
   *     in-place with any additional or changed values.
   * @param record The GenericRecord representing the source data to be transformed.
   * @param srcTableName The name of the source table.
   * @return The updated map with custom transformations applied, or `null` if the record should be
   *     filtered out based on transformation rules.
   * @throws InvalidTransformationException If an error occurs during the transformation process,
   *     such as incompatible data types or missing columns.
   */
  private Map<String, Value> populateCustomTransformations(
      Map<String, Value> result, GenericRecord record, String srcTableName)
      throws InvalidTransformationException {
    if (customTransformer == null) {
      return result;
    }
    LOG.debug("Populating custom transformation for table {}: {}", srcTableName, result);
    String spannerTableName = schemaMapper.getSpannerTableName(namespace, srcTableName);
    // TODO: verify if direct to object (Current) works the same as Object -> JsonNode-> Object
    // (Live).
    Map<String, Object> sourceRowMap = genericRecordToMap(record);
    MigrationTransformationResponse migrationTransformationResponse =
        getCustomTransformationResponse(sourceRowMap, srcTableName, shardId);
    if (migrationTransformationResponse.isEventFiltered()) {
      return null;
    }
    Map<String, Object> transformedCols = migrationTransformationResponse.getResponseRow();
    for (Map.Entry<String, Object> entry : transformedCols.entrySet()) {
      LOG.debug(
          "Updating record with {} from custom transformations for table {}", entry, srcTableName);
      String spannerColName = entry.getKey();
      Type spannerType =
          schemaMapper.getSpannerColumnType(namespace, spannerTableName, spannerColName);
      Value val =
          getSpannerValueFromObject(
              entry.getValue(), CUSTOM_TRANSFORMATION_AVRO_SCHEMA, spannerColName, spannerType);
      result.put(spannerColName, val);
    }
    LOG.debug("Updated record with custom transformations for table {}: {}", srcTableName, result);
    return result;
  }

  /** Converts a generic record to a Map. */
  private Map<String, Object> genericRecordToMap(GenericRecord record) {
    Map<String, Object> map = new HashMap<>();
    Schema schema = record.getSchema();

    for (Schema.Field field : schema.getFields()) {
      String fieldName = field.name();
      Object fieldValue = record.get(fieldName);
      if (fieldValue == null) {
        map.put(fieldName, null);
        continue;
      }
      Schema fieldSchema = filterNullSchema(field.schema(), fieldName, fieldValue);
      // Handle logical/record types.
      fieldValue = handleNonPrimitiveAvroTypes(fieldValue, fieldSchema, fieldName);
      // Standardizing the types for custom jar input.
      if (fieldSchema.getProp(LOGICAL_TYPE) != null
          || fieldSchema.getType() == Schema.Type.RECORD) {
        map.put(fieldName, fieldValue);
        continue;
      }
      Schema.Type fieldType = fieldSchema.getType();
      switch (fieldType) {
        case INT:
        case LONG:
          fieldValue = Long.valueOf(fieldValue.toString());
          break;
        case BOOLEAN:
          fieldValue = Boolean.valueOf(fieldValue.toString());
          break;
        case FLOAT:
        case DOUBLE:
          fieldValue = Double.valueOf(fieldValue.toString());
          break;
        case BYTES:
          fieldValue = Hex.encode(((ByteBuffer) fieldValue).array());
          break;
        default:
          fieldValue = fieldValue.toString();
      }
      map.put(fieldName, fieldValue);
    }
    return map;
  }

  @VisibleForTesting
  private MigrationTransformationResponse getCustomTransformationResponse(
      Map<String, Object> sourceRecord, String tableName, String shardId)
      throws InvalidTransformationException {

    org.joda.time.Instant startTimestamp = org.joda.time.Instant.now();
    MigrationTransformationRequest migrationTransformationRequest =
        new MigrationTransformationRequest(tableName, sourceRecord, shardId, "INSERT");
    LOG.debug(
        "using migration transformation request {} for table {}",
        migrationTransformationRequest,
        tableName);
    MigrationTransformationResponse migrationTransformationResponse;
    try {
      migrationTransformationResponse =
          customTransformer.toSpannerRow(migrationTransformationRequest);
    } finally {
      org.joda.time.Instant endTimestamp = org.joda.time.Instant.now();
      applyCustomTransformationResponseTimeMetric.update(
          new Duration(startTimestamp, endTimestamp).getMillis());
    }
    LOG.debug(
        "Got migration transformation response {} for table {}",
        migrationTransformationResponse,
        tableName);
    return migrationTransformationResponse;
  }

  private Map<String, Value> populateShardId(Map<String, Value> result, String shardIdCol) {
    if (shardId == null || shardId.isBlank()) {
      return result;
    }
    result.put(shardIdCol, Value.string(shardId));
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
    fieldSchema = filterNullSchema(fieldSchema, recordColName, recordValue);
    recordValue = handleNonPrimitiveAvroTypes(recordValue, fieldSchema, recordColName);
    return getSpannerValueFromObject(recordValue, fieldSchema, recordColName, spannerType);
  }

  /**
   * Filters a union schema to remove the nullable (NULL) option.
   *
   * <p>This method checks if the input schema is a UNION type. If it is, and the union only
   * contains two types (one of which is NULL), it returns the non-nullable type. If the schema is
   * not a union, or if it's a union with more than two types or a non-nullable type other than
   * NULL, an IllegalArgumentException is thrown.
   */
  private Schema filterNullSchema(Schema fieldSchema, String recordColName, Object recordValue) {
    if (fieldSchema.getType().equals(Schema.Type.UNION)) {
      List<Schema> types = fieldSchema.getTypes();
      LOG.debug("found union type: {}", types);
      // Schema types can only union with Type NULL. Any other UNION is unsupported.
      if (types.size() == 2 && types.stream().anyMatch(s -> s.getType().equals(Schema.Type.NULL))) {
        return types.stream().filter(s -> !s.getType().equals(Schema.Type.NULL)).findFirst().get();
      } else {
        throw new IllegalArgumentException(
            String.format(
                "Unknown schema field type %s for field %s with value %s.",
                fieldSchema, recordColName, recordValue));
      }
    }
    return fieldSchema;
  }

  /**
   * Handles complex Avro types (logical types or records) within a GenericRecord.
   *
   * <p>This method examines the schema of the given field and applies appropriate processing based
   * on whether it is a logical type or a nested record. The resulting value is potentially
   * transformed and returned.
   *
   * @param recordValue The original value of the field in the GenericRecord.
   * @param fieldSchema The Avro schema describing the field's type.
   * @param recordColName The name of the column (field) in the record.
   * @return The processed value of the field, potentially transformed according to its complex
   *     type.
   */
  private Object handleNonPrimitiveAvroTypes(
      Object recordValue, Schema fieldSchema, String recordColName) {
    if (fieldSchema.getLogicalType() != null || fieldSchema.getProp(LOGICAL_TYPE) != null) {
      recordValue = handleLogicalFieldType(recordColName, recordValue, fieldSchema);
    } else if (fieldSchema.getType().equals(Schema.Type.RECORD)) {
      // Get the avro field of type record from the whole record.
      recordValue = handleRecordFieldType(recordColName, (GenericRecord) recordValue, fieldSchema);
    }
    LOG.debug("Updated record value is {} for recordColName {}", recordValue, recordColName);
    return recordValue;
  }

  /** Converts an avro object to Spanner Value of the specified type. */
  private Value getSpannerValueFromObject(
      Object value, Schema fieldSchema, String recordColName, Type spannerType) {
    Dialect dialect = schemaMapper.getDialect();
    if (dialect == null) {
      throw new NullPointerException("schemaMapper returned null spanner dialect.");
    }
    if (AvroToValueMapper.convertorMap().get(dialect).containsKey(spannerType)) {
      return AvroToValueMapper.convertorMap()
          .get(dialect)
          .get(spannerType)
          .apply(value, fieldSchema);
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

    public static final String TIME_INTERVAL = "time-interval-micros";

    public static final String UNSUPPORTED = "unsupported";
  }

  /** Avro logical types are converted to an equivalent string type. */
  static String handleLogicalFieldType(String fieldName, Object recordValue, Schema fieldSchema) {
    LOG.debug("found logical type for col {} with schema {}", fieldName, fieldSchema);
    if (recordValue == null) {
      return null;
    }
    if (fieldSchema.getLogicalType() instanceof LogicalTypes.Date) {
      TimeConversions.DateConversion dateConversion = new TimeConversions.DateConversion();
      LocalDate date =
          dateConversion.fromInt(
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
      // We cannot convert to nanoseconds directly here since that can overflow for Long.
      Long micros = Long.valueOf(recordValue.toString());
      Instant timestamp =
          Instant.ofEpochSecond(
              TimeUnit.MICROSECONDS.toSeconds(micros),
              (micros % TimeUnit.SECONDS.toMicros(1)) * TimeUnit.MICROSECONDS.toNanos(1));
      return timestamp.atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    } else if (fieldSchema.getLogicalType() instanceof LogicalTypes.TimestampMillis) {
      Instant timestamp = Instant.ofEpochMilli(Long.valueOf(recordValue.toString()));
      return timestamp.atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    } else if (fieldSchema.getProp(LOGICAL_TYPE) != null
        && fieldSchema.getProp(LOGICAL_TYPE).equals(CustomAvroTypes.JSON)) {
      return recordValue.toString();
    } else if (fieldSchema.getProp(LOGICAL_TYPE) != null
        && fieldSchema.getProp(LOGICAL_TYPE).equals(CustomAvroTypes.NUMBER)) {
      return recordValue.toString();
    } else if (fieldSchema.getProp(LOGICAL_TYPE) != null
        && fieldSchema.getProp(LOGICAL_TYPE).equals(CustomAvroTypes.VARCHAR)) {
      return recordValue.toString();
    } else if (fieldSchema.getProp(LOGICAL_TYPE) != null
        && fieldSchema.getProp(LOGICAL_TYPE).equals(CustomAvroTypes.TIME_INTERVAL)) {
      Long timeMicrosTotal = Long.valueOf(recordValue.toString());
      boolean isNegative = false;
      if (timeMicrosTotal < 0) {
        timeMicrosTotal *= -1;
        isNegative = true;
      }
      Long nanoseconds = timeMicrosTotal * TimeUnit.MICROSECONDS.toNanos(1);
      Long hours = TimeUnit.NANOSECONDS.toHours(nanoseconds);
      nanoseconds -= TimeUnit.HOURS.toNanos(hours);
      Long minutes = TimeUnit.NANOSECONDS.toMinutes(nanoseconds);
      nanoseconds -= TimeUnit.MINUTES.toNanos(minutes);
      Long seconds = TimeUnit.NANOSECONDS.toSeconds(nanoseconds);
      nanoseconds -= TimeUnit.SECONDS.toNanos(seconds);
      Long micros = TimeUnit.NANOSECONDS.toMicros(nanoseconds);
      // Pad 0 if single digit hour.
      String timeString = (hours < 10) ? String.format("%02d", hours) : String.format("%d", hours);
      timeString += String.format(":%02d:%02d", minutes, seconds);
      if (micros > 0) {
        timeString += String.format(".%d", micros);
      }
      return isNegative ? "-" + timeString : timeString;
    } else if (fieldSchema.getProp(LOGICAL_TYPE) != null
        && fieldSchema.getProp(LOGICAL_TYPE).equals(CustomAvroTypes.UNSUPPORTED)) {
      return null;
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
