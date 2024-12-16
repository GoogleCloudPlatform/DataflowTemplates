/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.v2.datastream.transforms;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Conversions.DecimalConversion;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.data.TimeConversions.DateConversion;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Formats a plain Avro-to-json record coming from Datastream into the full JSON record that we will
 * use downstream.
 */
public class FormatDatastreamRecordToJson
    implements SerializableFunction<GenericRecord, FailsafeElement<String, String>> {

  /** Names of the custom avro types that we'll be using. */
  public static class CustomAvroTypes {
    public static final String VARCHAR = "varchar";
    public static final String NUMBER = "number";
    public static final String TIME_INTERVAL_MICROS = "time-interval-micros";
  }

  static final String LOGICAL_TYPE = "logicalType";

  static final Logger LOG = LoggerFactory.getLogger(FormatDatastreamRecordToJson.class);
  static final DateTimeFormatter DEFAULT_DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;
  static final DateTimeFormatter DEFAULT_TIMESTAMP_WITH_TZ_FORMATTER =
      DateTimeFormatter.ISO_OFFSET_DATE_TIME;
  static final DecimalConversion DECIMAL_CONVERSION = new DecimalConversion();
  static final DateConversion DATE_CONVERSION = new DateConversion();
  private String streamName;
  private boolean lowercaseSourceColumns = false;
  private String rowIdColumnName;
  private Map<String, String> renameColumns = new HashMap<String, String>();
  private boolean hashRowId = false;

  private FormatDatastreamRecordToJson() {}

  public static FormatDatastreamRecordToJson create() {
    return new FormatDatastreamRecordToJson();
  }

  public FormatDatastreamRecordToJson withStreamName(String streamName) {
    this.streamName = streamName;
    return this;
  }

  public FormatDatastreamRecordToJson withLowercaseSourceColumns(Boolean lowercaseSourceColumns) {
    this.lowercaseSourceColumns = lowercaseSourceColumns;
    return this;
  }

  /**
   * Set the map of columns values to rename/copy.
   *
   * @param renameColumns The map of columns to new columns to rename/copy.
   */
  public FormatDatastreamRecordToJson withRenameColumnValues(Map<String, String> renameColumns) {
    this.renameColumns = renameColumns;
    return this;
  }

  /** Set the reader to hash Oracle ROWID values into int. */
  public FormatDatastreamRecordToJson withHashRowId(Boolean hashRowId) {
    this.hashRowId = hashRowId;
    return this;
  }

  @Override
  public FailsafeElement<String, String> apply(GenericRecord record) {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode outputObject = mapper.createObjectNode();
    UnifiedTypesFormatter.payloadToJson(getPayload(record), outputObject);
    if (this.lowercaseSourceColumns) {
      outputObject = getLowerCaseObject(outputObject);
    }

    // General DataStream Metadata
    String sourceType = getSourceType(record);

    outputObject.put("_metadata_stream", getStreamName(record));
    outputObject.put("_metadata_timestamp", getSourceTimestamp(record));
    outputObject.put("_metadata_read_timestamp", getMetadataTimestamp(record));
    outputObject.put("_metadata_dataflow_timestamp", getCurrentTimestamp());
    outputObject.put("_metadata_read_method", record.get("read_method").toString());
    outputObject.put("_metadata_source_type", sourceType);

    // Source Specific Metadata
    outputObject.put("_metadata_deleted", getMetadataIsDeleted(record));
    outputObject.put("_metadata_table", getMetadataTable(record));
    outputObject.put("_metadata_change_type", getMetadataChangeType(record));
    outputObject.put("_metadata_primary_keys", getPrimaryKeys(record));
    outputObject.put("_metadata_uuid", getUUID());

    if (sourceType.equals("mysql")) {
      // MySQL Specific Metadata
      outputObject.put("_metadata_schema", getMetadataDatabase(record));
      outputObject.put("_metadata_log_file", getSourceMetadata(record, "log_file"));
      outputObject.put("_metadata_log_position", getSourceMetadata(record, "log_position"));
    } else if (sourceType.equals("postgresql")) {
      // Postgres Specific Metadata
      outputObject.put("_metadata_schema", getMetadataSchema(record));
      outputObject.put("_metadata_lsn", getPostgresLsn(record));
      outputObject.put("_metadata_tx_id", getPostgresTxId(record));
    } else {
      // Oracle Specific Metadata
      outputObject.put("_metadata_schema", getMetadataSchema(record));
      outputObject.put("_metadata_scn", getOracleScn(record));
      outputObject.put("_metadata_ssn", getOracleSsn(record));
      outputObject.put("_metadata_rs_id", getOracleRsId(record));
      outputObject.put("_metadata_tx_id", getOracleTxId(record));

      FormatDatastreamRecord.setOracleRowIdValue(
          outputObject, getOracleRowId(record), this.hashRowId);
    }
    // Rename columns supplied
    FormatDatastreamRecord.applyRenameColumns(outputObject, this.renameColumns);

    // All Raw Metadata
    outputObject.put("_metadata_source", getSourceMetadataJson(record));

    return FailsafeElement.of(outputObject.toString(), outputObject.toString());
  }

  private GenericRecord getPayload(GenericRecord record) {
    return (GenericRecord) record.get("payload");
  }

  private ObjectNode getLowerCaseObject(ObjectNode outputObject) {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode loweredOutputObject = mapper.createObjectNode();

    for (Iterator<String> fieldNames = outputObject.fieldNames(); fieldNames.hasNext(); ) {
      String fieldName = fieldNames.next();
      loweredOutputObject.put(fieldName.toLowerCase(), outputObject.get(fieldName));
    }

    return loweredOutputObject;
  }

  private JsonNode getSourceMetadataJson(GenericRecord record) {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode dataInput;
    try {
      dataInput = mapper.readTree(record.get("source_metadata").toString());
    } catch (IOException e) {
      LOG.error("Issue parsing JSON record. Unable to continue.", e);
      throw new RuntimeException(e);
    }
    return dataInput;
  }

  private String getStreamName(GenericRecord record) {
    if (this.streamName == null) {
      return record.get("stream_name").toString();
    }
    return this.streamName;
  }

  private String getSourceType(GenericRecord record) {
    String sourceType = record.get("read_method").toString().split("-")[0];
    // TODO: consider validating the value is mysql or oracle
    return sourceType;
  }

  private long getMetadataTimestamp(GenericRecord record) {
    long unixTimestampMilli = (long) record.get("read_timestamp");
    return unixTimestampMilli / 1000;
  }

  private long getCurrentTimestamp() {
    return System.currentTimeMillis() / 1000L;
  }

  private long getSourceTimestamp(GenericRecord record) {
    long unixTimestampMilli = (long) record.get("source_timestamp");

    return unixTimestampMilli / 1000;
  }

  private String getSourceMetadata(GenericRecord record, String fieldName) {
    GenericRecord sourceMetadata = (GenericRecord) record.get("source_metadata");
    if (sourceMetadata.getSchema().getField(fieldName) != null
        && sourceMetadata.get(fieldName) != null) {
      return sourceMetadata.get(fieldName).toString();
    }

    return null;
  }

  private String getMetadataSchema(GenericRecord record) {
    return ((GenericRecord) record.get("source_metadata")).get("schema").toString();
  }

  private String getMetadataDatabase(GenericRecord record) {
    return ((GenericRecord) record.get("source_metadata")).get("database").toString();
  }

  private String getMetadataTable(GenericRecord record) {
    return ((GenericRecord) record.get("source_metadata")).get("table").toString();
  }

  private String getMetadataChangeType(GenericRecord record) {
    GenericRecord sourceMetadata = (GenericRecord) record.get("source_metadata");
    if (sourceMetadata.getSchema().getField("change_type") != null
        && sourceMetadata.get("change_type") != null) {
      return sourceMetadata.get("change_type").toString();
    }

    return null;
  }

  private JsonNode getPrimaryKeys(GenericRecord record) {
    GenericRecord sourceMetadata = (GenericRecord) record.get("source_metadata");
    if (sourceMetadata.getSchema().getField("primary_keys") == null
        || sourceMetadata.get("primary_keys") == null) {
      return null;
    }

    JsonNode dataInput = getSourceMetadataJson(record);
    return dataInput.get("primary_keys");
  }

  private String getUUID() {
    return UUID.randomUUID().toString();
  }

  private Boolean getMetadataIsDeleted(GenericRecord record) {
    boolean isDeleted = false;
    GenericRecord sourceMetadata = (GenericRecord) record.get("source_metadata");
    if (sourceMetadata.getSchema().getField("is_deleted") != null
        && sourceMetadata.get("is_deleted") != null) {
      isDeleted = (boolean) sourceMetadata.get("is_deleted");
    }

    return isDeleted;
  }

  private String getOracleRowId(GenericRecord record) {
    GenericRecord sourceMetadata = (GenericRecord) record.get("source_metadata");
    if (sourceMetadata.getSchema().getField("row_id") != null
        && sourceMetadata.get("row_id") != null) {
      return sourceMetadata.get("row_id").toString();
    }

    return null;
  }

  private Long getOracleScn(GenericRecord record) {
    GenericRecord sourceMetadata = (GenericRecord) record.get("source_metadata");
    if (sourceMetadata.getSchema().getField("scn") != null && sourceMetadata.get("scn") != null) {
      return (Long) sourceMetadata.get("scn");
    }

    return null;
  }

  private Long getOracleSsn(GenericRecord record) {
    // oracle sort keys are a list of four values that are provided in this order:
    // [timestamp, scn, rs_id, ssn]
    if (record.hasField("sort_keys")) {
      return (Long) ((GenericData.Array<?>) record.get("sort_keys")).get(3);
    }

    GenericRecord sourceMetadata = (GenericRecord) record.get("source_metadata");
    // oracle sort keys are a list of four values that are provided in this order:
    // [timestamp, scn, rs_id, ssn]
    if (sourceMetadata.getSchema().getField("ssn") != null && sourceMetadata.get("ssn") != null) {
      return (Long) sourceMetadata.get("ssn");
    }

    return null;
  }

  private String getOracleRsId(GenericRecord record) {
    if (record.hasField("sort_keys")) {
      return ((GenericData.Array<?>) record.get("sort_keys")).get(2).toString();
    }

    GenericRecord sourceMetadata = (GenericRecord) record.get("source_metadata");
    if (sourceMetadata.getSchema().getField("rs_id") != null
        && sourceMetadata.get("rs_id") != null) {
      return sourceMetadata.get("rs_id").toString();
    }

    return null;
  }

  private String getOracleTxId(GenericRecord record) {
    GenericRecord sourceMetadata = (GenericRecord) record.get("source_metadata");
    if (sourceMetadata.getSchema().getField("tx_id") != null
        && sourceMetadata.get("tx_id") != null) {
      return sourceMetadata.get("tx_id").toString();
    }

    return null;
  }

  private String getPostgresLsn(GenericRecord record) {
    GenericRecord sourceMetadata = (GenericRecord) record.get("source_metadata");
    if (sourceMetadata.getSchema().getField("lsn") != null && sourceMetadata.get("lsn") != null) {
      return sourceMetadata.get("lsn").toString();
    }

    return null;
  }

  private String getPostgresTxId(GenericRecord record) {
    GenericRecord sourceMetadata = (GenericRecord) record.get("source_metadata");
    if (sourceMetadata.getSchema().getField("tx_id") != null
        && sourceMetadata.get("tx_id") != null) {
      return sourceMetadata.get("tx_id").toString();
    }

    return null;
  }

  static class UnifiedTypesFormatter {
    public static void payloadToJson(GenericRecord payloadRecord, ObjectNode jsonNode) {
      for (Field f : payloadRecord.getSchema().getFields()) {
        putField(f.name(), f.schema(), payloadRecord, jsonNode);
      }
    }

    static void putField(
        String fieldName, Schema fieldSchema, GenericRecord record, ObjectNode jsonObject) {
      // fieldSchema.getLogicalType() returns object of type org.apache.avro.LogicalType,
      // therefore, is null for custom logical types
      if (fieldSchema.getLogicalType() != null) {
        // Logical types should be handled separately.
        handleLogicalFieldType(fieldName, fieldSchema, record, jsonObject);
        return;
      } else if (fieldSchema.getProp(LOGICAL_TYPE) != null) {
        // Handling for custom logical types.
        boolean isSupportedCustomType =
            handleCustomLogicalType(fieldName, fieldSchema, record, jsonObject);
        if (isSupportedCustomType) {
          return;
        }
      }

      switch (fieldSchema.getType()) {
        case BOOLEAN:
          jsonObject.put(fieldName, (Boolean) record.get(fieldName));
          break;
        case BYTES:
          jsonObject.put(fieldName, (byte[]) record.get(fieldName));
          break;
        case FLOAT:
          String value = record.get(fieldName).toString();
          jsonObject.put(fieldName, Double.valueOf(value));
          break;
        case DOUBLE:
          jsonObject.put(fieldName, (Double) record.get(fieldName));
          break;
        case INT:
          jsonObject.put(fieldName, (Integer) record.get(fieldName));
          break;
        case LONG:
          jsonObject.put(fieldName, (Long) record.get(fieldName));
          break;
        case STRING:
          jsonObject.put(fieldName, record.get(fieldName).toString());
          break;
        case RECORD:
          handleDatastreamRecordType(
              fieldName, fieldSchema, (GenericRecord) record.get(fieldName), jsonObject);
          break;
        case NULL:
          // Add key as null
          jsonObject.putNull(fieldName);
          break;
        case UNION:
          List<Schema> types = fieldSchema.getTypes();
          if (types.size() == 2
              && types.stream().anyMatch(s -> s.getType().equals(Schema.Type.NULL))) {
            if (record.get(fieldName) == null) {
              // Add key as null
              jsonObject.putNull(fieldName);
              break; // This element is null.
            }
            Schema actualSchema =
                types.stream().filter(s -> !s.getType().equals(Schema.Type.NULL)).findFirst().get();
            putField(fieldName, actualSchema, record, jsonObject);
            break;
          }
          FormatDatastreamRecordToJson.LOG.error(
              "Unknown field type {} for field {} in record {}.", fieldSchema, fieldName, record);
          break;
        case ARRAY:
        case FIXED:
        default:
          FormatDatastreamRecordToJson.LOG.error(
              "Unknown field type {} for field {} in record {}.", fieldSchema, fieldName, record);
          break;
      }
    }

    static boolean handleCustomLogicalType(
        String fieldName, Schema fieldSchema, GenericRecord element, ObjectNode jsonObject) {
      if (fieldSchema.getProp(LOGICAL_TYPE).equals(CustomAvroTypes.TIME_INTERVAL_MICROS)) {
        Long timeMicrosTotal = (Long) element.get(fieldName);
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
        String timeString =
            (hours < 10) ? String.format("%02d", hours) : String.format("%d", hours);
        timeString += String.format(":%02d:%02d", minutes, seconds);
        if (micros > 0) {
          timeString += String.format(".%d", micros);
        }
        String resultString = isNegative ? "-" + timeString : timeString;
        jsonObject.put(fieldName, resultString);
        return true;
      } else if (fieldSchema.getProp(LOGICAL_TYPE).equals(CustomAvroTypes.NUMBER)) {
        String number = element.get(fieldName).toString();
        jsonObject.put(fieldName, number);
        return true;
      } else if (fieldSchema.getProp(LOGICAL_TYPE).equals(CustomAvroTypes.VARCHAR)) {
        String varcharValue = element.get(fieldName).toString();
        jsonObject.put(fieldName, varcharValue);
        return true;
      }
      return false;
    }

    static void handleLogicalFieldType(
        String fieldName, Schema fieldSchema, GenericRecord element, ObjectNode jsonObject) {
      // TODO(pabloem) Actually test this.
      if (fieldSchema.getLogicalType() instanceof LogicalTypes.Date) {
        LocalDate date =
            DATE_CONVERSION.fromInt(
                (Integer) element.get(fieldName), fieldSchema, fieldSchema.getLogicalType());
        jsonObject.put(fieldName, date.format(DEFAULT_DATE_FORMATTER));
      } else if (fieldSchema.getLogicalType() instanceof LogicalTypes.Decimal) {
        BigDecimal bigDecimal =
            FormatDatastreamRecordToJson.DECIMAL_CONVERSION.fromBytes(
                (ByteBuffer) element.get(fieldName), fieldSchema, fieldSchema.getLogicalType());
        jsonObject.put(fieldName, bigDecimal.toPlainString());
      } else if (fieldSchema.getLogicalType() instanceof LogicalTypes.TimeMicros) {
        Long nanoseconds = (Long) element.get(fieldName) * TimeUnit.MICROSECONDS.toNanos(1);
        Duration duration =
            Duration.ofSeconds(
                TimeUnit.NANOSECONDS.toSeconds(nanoseconds),
                nanoseconds % TimeUnit.SECONDS.toNanos(1));
        jsonObject.put(fieldName, duration.toString());
      } else if (fieldSchema.getLogicalType() instanceof LogicalTypes.TimeMillis) {
        Duration duration = Duration.ofMillis(((Long) element.get(fieldName)));
        jsonObject.put(fieldName, duration.toString());
      } else if (fieldSchema.getLogicalType() instanceof LogicalTypes.TimestampMicros) {
        Long microseconds = (Long) element.get(fieldName);
        Long millis = TimeUnit.MICROSECONDS.toMillis(microseconds);
        Instant instant = Instant.ofEpochMilli(millis);
        // adding the microsecond after it was removed in the millisecond conversion
        instant = instant.plusNanos(microseconds % 1000 * 1000L);
        jsonObject.put(
            fieldName,
            instant.atOffset(ZoneOffset.UTC).format(DEFAULT_TIMESTAMP_WITH_TZ_FORMATTER));
      } else if (fieldSchema.getLogicalType() instanceof LogicalTypes.TimestampMillis) {
        Instant timestamp = Instant.ofEpochMilli(((Long) element.get(fieldName)));
        jsonObject.put(
            fieldName,
            timestamp.atOffset(ZoneOffset.UTC).format(DEFAULT_TIMESTAMP_WITH_TZ_FORMATTER));
      } else {
        LOG.error(
            "Unknown field type {} for field {} in {}. Ignoring it.",
            fieldSchema,
            fieldName,
            element.get(fieldName));
      }
    }

    static void handleDatastreamRecordType(
        String fieldName, Schema fieldSchema, GenericRecord element, ObjectNode jsonObject) {
      switch (fieldSchema.getName()) {
        case "Json":
          jsonObject.put(fieldName, element.toString());
          break;
        case "varchar":
          // TODO(pabloem): Remove this after Datastream rollout N+1 after 2020-10-26
          // Datastream Varchars are represented with their value and length.
          // For us, we only care about values.
          element.get("value");
          jsonObject.put(fieldName, element.get("value").toString());
          break;
        case "calendarDate":
          LocalDate date =
              LocalDate.of(
                  (Integer) element.get("year"),
                  (Integer) element.get("month"),
                  (Integer) element.get("day"));
          jsonObject.put(fieldName, date.format(DEFAULT_DATE_FORMATTER));
          break;
        case "year":
          jsonObject.put(fieldName, (Integer) element.get("year"));
          break;
        case "timestampTz":
          // Timestamp comes in microseconds
          Long nanoseconds = (Long) element.get("timestamp") * TimeUnit.MICROSECONDS.toNanos(1);
          Instant timestamp =
              Instant.ofEpochSecond(
                  TimeUnit.NANOSECONDS.toSeconds(nanoseconds),
                  nanoseconds % TimeUnit.SECONDS.toNanos(1));
          // Offset comes in milliseconds
          ZoneOffset offset =
              ZoneOffset.ofTotalSeconds(((Number) element.get("offset")).intValue() / 1000);
          ZonedDateTime fullDate = timestamp.atOffset(offset).toZonedDateTime();
          // BigQuery only has UTC timestamps so we convert to UTC and adjust
          jsonObject.put(
              fieldName,
              fullDate
                  .withZoneSameInstant(ZoneId.of("UTC"))
                  .format(DEFAULT_TIMESTAMP_WITH_TZ_FORMATTER));
          break;
          /*
           * The `intervalNano` maps to nano second precision interval type used by Cassandra Interval.
           * On spanner this will map to `string` or `Interval` type.
           * This is added here for DQL retrials for sourcedb-to-spanner.
           *
           * TODO(b/383689307):
           * There's a lot of commonality in handling avro types between {@link FormatDatastreamRecordToJson} and {@link com.google.cloud.teleport.v2.spanner.migrations.avro.GenericRecordTypeConvertor}.
           * Adding inter-package dependency might not be the best route, and we might eventually want to build a common package for handling common logic between the two.
           */
        case "intervalNano":
          Period period =
              Period.ZERO
                  .plusYears(getOrDefault(element, "years", 0L))
                  .plusMonths(getOrDefault(element, "months", 0L))
                  .plusDays(getOrDefault(element, "days", 0L));
          /*
           * Convert the period to a ISO-8601 period formatted String, such as P6Y3M1D.
           * A zero period will be represented as zero days, 'P0D'.
           * Refer to javadoc for Period#toString.
           */
          String periodIso8061 = period.toString();
          java.time.Duration duration =
              java.time.Duration.ZERO
                  .plusHours(getOrDefault(element, "hours", 0L))
                  .plusMinutes(getOrDefault(element, "minutes", 0L))
                  .plusSeconds(getOrDefault(element, "seconds", 0L))
                  .plusNanos(getOrDefault(element, "nanos", 0L));
          /*
           * Convert the duration to a ISO-8601 period formatted String, such as  PT8H6M12.345S
           * refer to javadoc for Duration#toString.
           */
          String durationIso8610 = duration.toString();
          // Convert to ISO-8601 period format.
          String convertedIntervalNano;
          if (duration.isZero()) {
            convertedIntervalNano = periodIso8061;
          } else {
            convertedIntervalNano =
                periodIso8061 + StringUtils.removeStartIgnoreCase(durationIso8610, "P");
          }
          jsonObject.put(fieldName, convertedIntervalNano);
          break;
        default:
          LOG.warn(
              "Unknown field type {} for field {} in record {}.", fieldSchema, fieldName, element);
          ObjectMapper mapper = new ObjectMapper();
          JsonNode dataInput;
          try {
            dataInput = mapper.readTree(element.toString());
            jsonObject.put(fieldName, dataInput);
          } catch (IOException e) {
            LOG.error("Issue parsing JSON record. Unable to continue.", e);
            throw new RuntimeException(e);
          }
          break;
      }
    }

    private static <T> T getOrDefault(GenericRecord element, String name, T def) {
      if (element.get(name) == null) {
        return def;
      }
      return (T) element.get(name);
    }
  }
}
