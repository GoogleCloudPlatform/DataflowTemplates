/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.List;
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

  static final Logger LOG = LoggerFactory.getLogger(FormatDatastreamRecordToJson.class);
  static final DateTimeFormatter DEFAULT_DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;
  static final DateTimeFormatter DEFAULT_TIMESTAMP_WITH_TZ_FORMATTER =
      DateTimeFormatter.ISO_OFFSET_DATE_TIME;
  static final DecimalConversion DECIMAL_CONVERSION = new DecimalConversion();
  static final DateConversion DATE_CONVERSION = new DateConversion();
  private String streamName;

  private FormatDatastreamRecordToJson() {}

  public static FormatDatastreamRecordToJson create() {
    return new FormatDatastreamRecordToJson();
  }

  public FormatDatastreamRecordToJson withStreamName(String streamName) {
    this.streamName = streamName;
    return this;
  }

  @Override
  public FailsafeElement<String, String> apply(GenericRecord record) {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode outputObject = mapper.createObjectNode();
    UnifiedTypesFormatter.payloadToJson((GenericRecord) record.get("payload"), outputObject);

    // General DataStream Metadata
    outputObject.put("_metadata_stream", getStreamName(record));
    outputObject.put("_metadata_timestamp", getSourceTimestamp(record));
    outputObject.put("_metadata_read_timestamp", getMetadataTimestamp(record));

    // Source Specific Metadata
    outputObject.put("_metadata_deleted", getMetadataIsDeleted(record));
    outputObject.put("_metadata_schema", getMetadataSchema(record));
    outputObject.put("_metadata_table", getMetadataTable(record));
    outputObject.put("_metadata_change_type", getMetadataChangeType(record));

    // Oracle Specific Metadata
    outputObject.put("_metadata_row_id", getOracleRowId(record));
    outputObject.put("_metadata_scn", getOracleScn(record));
    outputObject.put("_metadata_ssn", getOracleSsn(record));
    outputObject.put("_metadata_rs_id", getOracleRsId(record));
    outputObject.put("_metadata_tx_id", getOracleTxId(record));

    outputObject.put("_metadata_source", getSourceMetadata(record));

    return FailsafeElement.of(outputObject.toString(), outputObject.toString());
  }

  private JsonNode getSourceMetadata(GenericRecord record) {
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

  private long getMetadataTimestamp(GenericRecord record) {
    long unixTimestampMilli = (long) record.get("read_timestamp");
    return unixTimestampMilli / 1000;
  }

  private long getSourceTimestamp(GenericRecord record) {
    long unixTimestampMilli = (long) record.get("source_timestamp");
    long unixTimestampSec = unixTimestampMilli / 1000;

    return unixTimestampSec;
  }

  private String getMetadataSchema(GenericRecord record) {
    return ((GenericRecord) record.get("source_metadata")).get("schema").toString();
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
      for (Field f : payloadRecord
          .getSchema()
          .getFields()) {
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
          jsonObject.put(fieldName, (Float) record.get(fieldName));
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
              fieldName,
              fieldSchema,
              (GenericRecord) record.get(fieldName),
              jsonObject);
          break;
        case NULL:
          // We represent nulls by not adding the key to the JSON object.
          break;
        case UNION:
          List<Schema> types = fieldSchema.getTypes();
          if (types.size() == 2
              && types.stream().anyMatch(s -> s.getType().equals(Schema.Type.NULL))) {
            // We represent nulls by not adding the key to the JSON object.
            if (record.get(fieldName) == null) {
              break;  // This element is null.
            }
            Schema actualSchema = types.stream()
                .filter(s -> !s.getType().equals(Schema.Type.NULL))
                .findFirst().get();
            putField(fieldName, actualSchema, record, jsonObject);
            break;
          }
          FormatDatastreamRecordToJson.LOG.error("Unknown field type {} for field {} in record {}.",
              fieldSchema, fieldName, record);
          break;
        case ARRAY:
        case FIXED:
        default:
          FormatDatastreamRecordToJson.LOG.error("Unknown field type {} for field {} in record {}.",
              fieldSchema, fieldName, record);
          break;
      }
    }

    static void handleLogicalFieldType(
        String fieldName, Schema fieldSchema, GenericRecord element, ObjectNode jsonObject) {
      // TODO(pabloem) Actually test this.
      if (fieldSchema.getLogicalType() instanceof LogicalTypes.Date) {
        org.joda.time.LocalDate date = DATE_CONVERSION.fromInt(
            (Integer) element.get(fieldName), fieldSchema, fieldSchema.getLogicalType());
        LocalDate javaDate = LocalDate.of(
            date.getYear(), date.getMonthOfYear(), date.getDayOfMonth());
        jsonObject.put(
            fieldName,
            javaDate.format(DEFAULT_DATE_FORMATTER));
      } else if (fieldSchema.getLogicalType() instanceof  LogicalTypes.Decimal) {
        BigDecimal bigDecimal = FormatDatastreamRecordToJson.DECIMAL_CONVERSION.fromBytes(
            (ByteBuffer) element.get(fieldName), fieldSchema, fieldSchema.getLogicalType());
        jsonObject.put(
            fieldName,
            bigDecimal.toPlainString());
      } else if (fieldSchema.getLogicalType() instanceof LogicalTypes.TimeMicros
          || fieldSchema.getLogicalType() instanceof LogicalTypes.TimeMillis) {
        Integer factor =
            fieldSchema.getLogicalType() instanceof LogicalTypes.TimeMillis ? 1 : 1000;
        Duration duration = Duration.ofMillis(((Long) element.get(fieldName)) / factor);
        jsonObject.put(
            fieldName,
            duration.toString());
      } else if (fieldSchema.getLogicalType() instanceof LogicalTypes.TimestampMicros
          || fieldSchema.getLogicalType() instanceof LogicalTypes.TimestampMillis) {
        Integer factor =
            fieldSchema.getLogicalType() instanceof LogicalTypes.TimestampMillis ? 1 : 1000;
        Instant timestamp = Instant.ofEpochMilli(((Long) element.get(fieldName)) / factor);
        jsonObject.put(
            fieldName,
            timestamp.atOffset(ZoneOffset.UTC).format(DEFAULT_TIMESTAMP_WITH_TZ_FORMATTER));
      } else {
        LOG.error("Unknown field type {} for field {} in {}. Ignoring it.",
            fieldSchema, fieldName, element.get(fieldName));
      }
    }

    static void handleDatastreamRecordType(
        String fieldName, Schema fieldSchema, GenericRecord element, ObjectNode jsonObject) {
      switch (fieldSchema.getName()) {
        case "Json":
          jsonObject.put(fieldName, element.toString());
          break;
        case "varchar":
          // Datastream Varchars are represented with their value and length.
          // For us, we only care about values.
          element.get("value");
          jsonObject.put(
              fieldName,
              element.get("value").toString());
          break;
        case "calendarDate":
          LocalDate date = LocalDate.of(
              (Integer) element.get("year"),
              (Integer) element.get("month"),
              (Integer) element.get("day"));
          jsonObject.put(
              fieldName,
              date.format(DEFAULT_DATE_FORMATTER));
          break;
        case "year":
          jsonObject.put(
              fieldName,
              (Integer) element.get("year"));
          break;
        case "timestampTz":
          // Timestamp comes in microseconds
          Instant timestamp = Instant.ofEpochMilli(
              ((Long) element.get("timestamp")) / 1000);
          // Offset comes in milliseconds
          ZoneOffset offset = ZoneOffset.ofTotalSeconds(
              ((Long) element.get("offset")).intValue() / 1000);
          ZonedDateTime fullDate = timestamp.atOffset(offset).toZonedDateTime();
          jsonObject.put(
              fieldName,
              fullDate.format(DEFAULT_TIMESTAMP_WITH_TZ_FORMATTER));
          break;
        default:
          LOG.warn("Unknown field type {} for field {} in record {}.",
              fieldSchema, fieldName, element);
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
