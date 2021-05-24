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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Formats a plain datastream-json record coming from Datastream into the full JSON record
 * that we will use downstream.
 */
public final class FormatDatastreamJsonToJson
    extends DoFn<String, FailsafeElement<String, String>> {

  static final Logger LOG = LoggerFactory.getLogger(FormatDatastreamJsonToJson.class);
  private String rowIdColumnName;
  private Map<String, String> hashedColumns = new HashMap<String, String>();
  private boolean lowercaseSourceColumns = false;
  private String streamName;

  private FormatDatastreamJsonToJson() {}

  public static FormatDatastreamJsonToJson create() {
    return new FormatDatastreamJsonToJson();
  }

  public FormatDatastreamJsonToJson withStreamName(String streamName) {
    this.streamName = streamName;
    return this;
  }

  public FormatDatastreamJsonToJson withLowercaseSourceColumns(Boolean lowercaseSourceColumns) {
    this.lowercaseSourceColumns = lowercaseSourceColumns;
    return this;
  }

 /**
   * Add the supplied columnName to the list of column values to be hashed.
   *
   * @param columnName The column name to look for in the data to hash.
   */
  public FormatDatastreamJsonToJson withHashColumnValue(String columnName) {
    this.hashedColumns.put(columnName, columnName);
    return this;
  }

  /**
   * Add the supplied columnName to the map of column values to be hashed.
   * A new column with a hashed value of the first will be created.
   *
   * @param columnName The column name to look for in the data.
   * @param newColumnName The name of the new column created with hashed data.
   */
  public FormatDatastreamJsonToJson withHashColumnValue(
      String columnName, String newColumnName) {
    this.hashedColumns.put(columnName, newColumnName);
    return this;
  }

  /**
   * Set the map of columns values to hash.
   *
   * @param hashedColumns The map of columns to new columns to hash.
   */
  public FormatDatastreamJsonToJson withHashColumnValues(
      Map<String, String> hashedColumns) {
    this.hashedColumns = hashedColumns;
    return this;
  }

  @ProcessElement
  public void processElement(ProcessContext c) {

    JsonNode record = null;

    try {
      record = new ObjectMapper().readTree(c.element());

      // check if payload is null/empty
      // re: b/183584054
      if (record.get("payload") == null) {
        String changeType = getSourceMetadata(record, "change_type");
        if (changeType == null || changeType.toLowerCase() != "delete") {
          // Empty payloads are generally schema files or
          // non-datastream files, they can all be ignored.
          return;
        }
      }
    } catch (IOException e) {
      LOG.error("Issue parsing JSON record: {}", c.element());
      return;
    }

    ObjectMapper mapper = new ObjectMapper();
    ObjectNode outputObject = mapper.createObjectNode();

    // General DataStream Metadata
    String sourceType = getSourceType(record);

    outputObject.put("_metadata_stream", getStreamName(record));
    outputObject.put("_metadata_timestamp", getSourceTimestamp(record));
    outputObject.put("_metadata_read_timestamp", getMetadataTimestamp(record));
    outputObject.put("_metadata_read_method", record.get("read_method").getTextValue());
    outputObject.put("_metadata_source_type", sourceType);

    outputObject.put("_metadata_deleted", getMetadataIsDeleted(record));
    outputObject.put("_metadata_table", getSourceMetadata(record, "table"));
    outputObject.put("_metadata_change_type", getSourceMetadata(record, "change_type"));

    // Source Specific Metadata
    if (sourceType.equals("mysql")) {
      // MySQL Specific Metadata
      outputObject.put("_metadata_schema", getSourceMetadata(record, "database"));
      outputObject.put("_metadata_log_file", getSourceMetadata(record, "log_file"));
      outputObject.put("_metadata_log_position", getSourceMetadataAsLong(record, "log_position"));
    } else {
      // Oracle Specific Metadata
      outputObject.put("_metadata_schema", getSourceMetadata(record, "schema"));
      outputObject.put("_metadata_row_id", getSourceMetadata(record, "row_id"));
      outputObject.put("_metadata_scn", getSourceMetadataAsLong(record, "scn"));
      outputObject.put("_metadata_ssn", getSourceMetadataAsLong(record, "ssn"));
      outputObject.put("_metadata_rs_id", getSourceMetadata(record, "rs_id"));
      outputObject.put("_metadata_tx_id", getSourceMetadata(record, "tx_id"));
    }

    JsonNode payload = record.get("payload");
    if (payload != null) {
      Iterator<String> dataKeys = payload.getFieldNames();

      while (dataKeys.hasNext()) {
        String key = dataKeys.next();

        if (this.lowercaseSourceColumns) {
          outputObject.put(key.toLowerCase(), payload.get(key));
        } else {
          outputObject.put(key, payload.get(key));
        }
      }
      // Hash columns supplied to be hashed
      applyHashToColumns(payload, outputObject);
    }

    // All Raw Metadata
    outputObject.put("_metadata_source", getSourceMetadata(record));

    c.output(FailsafeElement.of(outputObject.toString(), outputObject.toString()));
  }

  private JsonNode getSourceMetadata(JsonNode record) {
    return record.get("source_metadata");
  }

  private String getStreamName(JsonNode record) {
    if (this.streamName == null) {
      return record.get("stream_name").getTextValue();
    }
    return this.streamName;
  }

  private String getSourceType(JsonNode record) {
    String sourceType = record.get("read_method").getTextValue().split("-")[0];
    // TODO: consider validating the value is mysql or oracle
    return sourceType;
  }

  private String getMetadataTimestamp(JsonNode record) {
    if (record.get("read_timestamp").isLong()) {
      long unixTimestampMilli = record.get("read_timestamp").getLongValue();
      long unixTimestampSec = unixTimestampMilli / 1000;

      return String.valueOf(unixTimestampSec);
    }
    String timestamp = record.get("read_timestamp").getTextValue();
    return timestamp;
  }

  private String getSourceTimestamp(JsonNode record) {
    if (record.get("source_timestamp").isLong()) {
      long unixTimestampMilli = record.get("source_timestamp").getLongValue();
      long unixTimestampSec = unixTimestampMilli / 1000;

      return String.valueOf(unixTimestampSec);
    }
    String timestamp = record.get("source_timestamp").getTextValue();
    return timestamp;
  }

  private String getSourceMetadata(JsonNode record, String fieldName) {
    JsonNode md = getSourceMetadata(record);
    if (md == null || md.isNull()) {
      return null;
    }

    JsonNode value = md.get(fieldName);
    if (value == null || value.isNull()) {
      return null;
    }

    return value.getTextValue();
  }

  private Long getSourceMetadataAsLong(JsonNode record, String fieldName) {
    JsonNode md = getSourceMetadata(record);
    if (md == null || md.isNull()) {
      return null;
    }

    JsonNode value = md.get(fieldName);
    if (value == null || value.isNull()) {
      return null;
    }

    return value.getLongValue();
  }

  private Boolean getMetadataIsDeleted(JsonNode record) {
    boolean isDeleted = false;
    JsonNode md = getSourceMetadata(record);
    if (md == null || md.isNull()) {
      return isDeleted;
    }

    JsonNode value = md.get("is_deleted");
    if (value == null || value.isNull()) {
      return isDeleted;
    }

    return value.getBooleanValue();
  }

  private void applyHashToColumns(JsonNode record, ObjectNode outputObject) {
    for (String columnName : this.hashedColumns.keySet()) {
      if (record.get(columnName) != null) {
        // TODO: discuss hash algorithm to use
        String newColumnName = this.hashedColumns.get(columnName);
        int hashedValue = record.get(columnName).getTextValue().hashCode();
        outputObject.put(newColumnName, hashedValue);
      }
    }
  }
}
