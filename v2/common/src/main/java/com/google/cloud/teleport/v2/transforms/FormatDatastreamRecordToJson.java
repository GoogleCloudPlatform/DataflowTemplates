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
package com.google.cloud.teleport.v2.transforms;

import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.Conversions.DecimalConversion;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.data.TimeConversions.DateConversion;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
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
  }

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

    for (Iterator<String> fieldNames = outputObject.getFieldNames(); fieldNames.hasNext(); ) {
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

  private long getSourceTimestamp(GenericRecord record) {
    long unixTimestampMilli = (long) record.get("source_timestamp");
    long unixTimestampSec = unixTimestampMilli / 1000;

    return unixTimestampSec;
  }

  private String getSourceMetadata(GenericRecord record, String fieldName) {
    if (((GenericRecord) record.get("source_metadata")).get(fieldName) != null) {
      return ((GenericRecord) record.get("source_metadata")).get(fieldName).toString();
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
    if (((GenericRecord) record.get("source_metadata")).get("change_type") != null) {
      return ((GenericRecord) record.get("source_metadata")).get("change_type").toString();
    }

    return null;
  }

  private JsonNode getPrimaryKeys(GenericRecord record) {
    GenericRecord sourceMetadata = (GenericRecord) record.get("source_metadata");
    if (sourceMetadata.get("primary_keys") == null) {
      return null;
    }

    ObjectMapper mapper = new ObjectMapper();
    JsonNode dataInput = getSourceMetadataJson(record);
    return dataInput.get("primary_keys");
  }

  private String getUUID() {
    return UUID.randomUUID().toString();
  }

  private Boolean getMetadataIsDeleted(GenericRecord record) {
    boolean isDeleted = false;
    GenericRecord sourceMetadata = (GenericRecord) record.get("source_metadata");
    if (sourceMetadata.get("is_deleted") != null) {
      isDeleted = (boolean) sourceMetadata.get("is_deleted");
    }

    return isDeleted;
  }

  private String getOracleRowId(GenericRecord record) {
    if (((GenericRecord) record.get("source_metadata")).get("row_id") != null) {
      return ((GenericRecord) record.get("source_metadata")).get("row_id").toString();
    }

    return null;
  }

  private Long getOracleScn(GenericRecord record) {
    if (((GenericRecord) record.get("source_metadata")).get("scn") != null) {
      return (Long) ((GenericRecord) record.get("source_metadata")).get("scn");
    }

    return (Long) null;
  }

  private Long getOracleSsn(GenericRecord record) {
    if (((GenericRecord) record.get("source_metadata")).get("ssn") != null) {
      return (Long) ((GenericRecord) record.get("source_metadata")).get("ssn");
    }

    return (Long) null;
  }

  private String getOracleRsId(GenericRecord record) {
    if (((GenericRecord) record.get("source_metadata")).get("rs_id") != null) {
      return ((GenericRecord) record.get("source_metadata")).get("rs_id").toString();
    }

    return (String) null;
  }

  private String getOracleTxId(GenericRecord record) {
    if (((GenericRecord) record.get("source_metadata")).get("tx_id") != null) {
      return ((GenericRecord) record.get("source_metadata")).get("tx_id").toString();
    }

    return (String) null;
  }

  static class UnifiedTypesFormatter {
    public static void payloadToJson(GenericRecord payloadRecord, ObjectNode jsonNode) {
      for (Field f : payloadRecord.getSchema().getFields()) {
        putField(f.name(), f.schema(), payloadRecord, jsonNode);
      }
    }

    static void putField(
        String fieldName, Schema fieldSchema, GenericRecord record, ObjectNode jsonObject) {
      if (fieldSchema.getLogicalType() != null) {
        // Logical types should be handled separately.
        handleLogicalFieldType(fieldName, fieldSchema, record, jsonObject);
        return;
      }

      switch (fieldSchema.getType()) {
        case BOOLEAN:
          jsonObject.put(fieldName, (Boolean) record.get(fieldName));
          break;
        case BYTES:
          jsonObject.put(fieldName, (byte[]) record.get(fieldName));
          break;
        case FLOAT:
          String value = ((Float) record.get(fieldName)).toString();
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

    static void handleLogicalFieldType(
        String fieldName, Schema fieldSchema, GenericRecord element, ObjectNode jsonObject) {
      // TODO(pabloem) Actually test this.
      if (fieldSchema.getLogicalType() instanceof LogicalTypes.Date) {
        org.joda.time.LocalDate date =
            DATE_CONVERSION.fromInt(
                (Integer) element.get(fieldName), fieldSchema, fieldSchema.getLogicalType());
        LocalDate javaDate =
            LocalDate.of(date.getYear(), date.getMonthOfYear(), date.getDayOfMonth());
        jsonObject.put(fieldName, javaDate.format(DEFAULT_DATE_FORMATTER));
      } else if (fieldSchema.getLogicalType() instanceof LogicalTypes.Decimal) {
        BigDecimal bigDecimal =
            FormatDatastreamRecordToJson.DECIMAL_CONVERSION.fromBytes(
                (ByteBuffer) element.get(fieldName), fieldSchema, fieldSchema.getLogicalType());
        jsonObject.put(fieldName, bigDecimal.toPlainString());
      } else if (fieldSchema.getLogicalType() instanceof LogicalTypes.TimeMicros
          || fieldSchema.getLogicalType() instanceof LogicalTypes.TimeMillis) {
        Integer factor = fieldSchema.getLogicalType() instanceof LogicalTypes.TimeMillis ? 1 : 1000;
        Duration duration = Duration.ofMillis(((Long) element.get(fieldName)) / factor);
        jsonObject.put(fieldName, duration.toString());
      } else if (fieldSchema.getLogicalType() instanceof LogicalTypes.TimestampMicros
          || fieldSchema.getLogicalType() instanceof LogicalTypes.TimestampMillis) {
        Integer factor =
            fieldSchema.getLogicalType() instanceof LogicalTypes.TimestampMillis ? 1 : 1000;
        Instant timestamp = Instant.ofEpochMilli(((Long) element.get(fieldName)) / factor);
        jsonObject.put(
            fieldName,
            timestamp.atOffset(ZoneOffset.UTC).format(DEFAULT_TIMESTAMP_WITH_TZ_FORMATTER));
      } else if (fieldSchema.getLogicalType().getName().equals(CustomAvroTypes.NUMBER)) {
        String number = (String) element.get(fieldName);
        jsonObject.put(fieldName, number);
      } else if (fieldSchema.getLogicalType().getName().equals(CustomAvroTypes.VARCHAR)) {
        String varcharValue = (String) element.get(fieldName);
        jsonObject.put(fieldName, varcharValue);
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
          Instant timestamp = Instant.ofEpochMilli(((Long) element.get("timestamp")) / 1000);
          // Offset comes in milliseconds
          ZoneOffset offset =
              ZoneOffset.ofTotalSeconds(((Number) element.get("offset")).intValue() / 1000);
          ZonedDateTime fullDate = timestamp.atOffset(offset).toZonedDateTime();
          jsonObject.put(fieldName, fullDate.format(DEFAULT_TIMESTAMP_WITH_TZ_FORMATTER));
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
  }
}
