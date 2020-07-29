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
import java.util.Iterator;
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

  private static final Logger LOG = LoggerFactory.getLogger(FormatDatastreamRecordToJson.class);
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
    String strRecord = record.toString();
    ObjectMapper mapper = new ObjectMapper();
    JsonNode dataInput;
    try {
      dataInput = mapper.readTree(strRecord);
    } catch (IOException e) {
      LOG.error("Issue parsing JSON record. Unable to continue.", e);
      throw new RuntimeException(e);
    }
    ObjectNode outputObject = mapper.createObjectNode();
    Iterator<String> fieldNames = dataInput.get("payload").getFieldNames();
    while (fieldNames.hasNext()) {
      String fieldName = fieldNames.next();
      ((ObjectNode) outputObject).put(fieldName, dataInput.get("payload").get(fieldName));
    }

    // General DataStream Metadata
    ((ObjectNode) outputObject).put("_metadata_stream", getStreamName(dataInput));
    ((ObjectNode) outputObject).put("_metadata_timestamp", getSourceTimestamp(dataInput));
    ((ObjectNode) outputObject).put("_metadata_read_timestamp", getMetadataTimestamp(dataInput));

    // Source Specific Metadata
    ((ObjectNode) outputObject).put("_metadata_deleted", getMetadataIsDeleted(dataInput));
    ((ObjectNode) outputObject).put("_metadata_schema", getMetadataSchema(dataInput));
    ((ObjectNode) outputObject).put("_metadata_table", getMetadataTable(dataInput));
    ((ObjectNode) outputObject).put("_metadata_change_type", getMetadataChangeType(dataInput));

    // Oracle Specific Metadata
    ((ObjectNode) outputObject).put("_metadata_row_id", getOracleRowId(dataInput));
    ((ObjectNode) outputObject).put("_metadata_source", dataInput.get("source_metadata"));

    return FailsafeElement.of(outputObject.toString(), outputObject.toString());
  }

  private String getStreamName(JsonNode dataInput) {
    if (this.streamName == null) {
      return dataInput.get("stream_name").getTextValue();
    }
    return this.streamName;
  }

  private double getMetadataTimestamp(JsonNode dataInput) {
    double unixTimestampMilli = (double) dataInput.get("read_timestamp").getLongValue();
    return unixTimestampMilli / 1000;
  }

  private double getSourceTimestamp(JsonNode dataInput) {
    double unixTimestampSec;
    if (dataInput.has("source_timestamp")) {
      double unixTimestampMilli = (double) dataInput.get("source_timestamp").getLongValue();
      unixTimestampSec = unixTimestampMilli / 1000;
    } else {
      double unixTimestampMilli = (double) dataInput.get("read_timestamp").getLongValue();
      unixTimestampSec = unixTimestampMilli / 1000;
    }
    return unixTimestampSec;
  }

  private String getMetadataSchema(JsonNode dataInput) {
    return dataInput.get("source_metadata").get("schema").getTextValue();
  }

  private String getMetadataTable(JsonNode dataInput) {
    return dataInput.get("source_metadata").get("table").getTextValue();
  }

  private String getMetadataChangeType(JsonNode dataInput) {
    if (dataInput.get("source_metadata").has("change_type")) {
      return dataInput.get("source_metadata").get("change_type").getTextValue();
    }

    // TODO(dhercher): This should be a backfill insert
    return null;
  }

  private Boolean getMetadataIsDeleted(JsonNode dataInput) {
    // TODO(pabloem): Implement complete calculation for isDeleted.
    Boolean isDeleted = true;
    if (dataInput.has("read_method")
        && dataInput.get("read_method").getTextValue().equals("oracle_dump")) {
      isDeleted = false;
    }

    return isDeleted;
  }

  private String getOracleRowId(JsonNode dataInput) {
    if (dataInput.get("source_metadata").has("row_id")) {
      return dataInput.get("source_metadata").get("row_id").getTextValue();
    }

    return null;
  }
}
