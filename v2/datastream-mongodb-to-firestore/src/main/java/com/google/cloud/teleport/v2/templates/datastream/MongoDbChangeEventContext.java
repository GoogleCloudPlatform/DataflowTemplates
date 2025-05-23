/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.templates.datastream;

import static com.google.cloud.teleport.v2.templates.DataStreamMongoDBToFirestore.MAPPER_IGNORE_FIELDS;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.teleport.v2.transforms.Utils;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.util.Objects;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MongoDB's implementation of ChangeEventContext that provides implementation for handling MongoDB
 * change events.
 */
public class MongoDbChangeEventContext implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(MongoDbChangeEventContext.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static final String TIMESTAMP_COL = "timestamp";
  public static final String SHADOW_DOC_ID_COL = "_metadata_id";
  public static final String DOC_ID_COL = "_id";

  public static final String TIMESTAMP_SECONDS_COL = "seconds";

  public static final String TIMESTAMP_NANOS_COL = "nanos";
  public static final String DATA_COL = "data";
  public static final String OID_FIELD_NAME = "$oid";

  private final JsonNode changeEvent;
  private final String dataCollection;
  private final String shadowCollection;
  private final Object documentId;
  private final Document shadowDocument;
  private final String jsonStringData;
  private final boolean isDeleteEvent;
  private final Document timestampDoc;
  private boolean isDlqReconsumed;
  private int retryCount;

  /** Gets the change type from the event metadata. */
  private String getChangeType(JsonNode changeEvent) {
    if (changeEvent.has(DatastreamConstants.EVENT_CHANGE_TYPE_KEY)) {
      return changeEvent.get(DatastreamConstants.EVENT_CHANGE_TYPE_KEY).asText();
    }
    return DatastreamConstants.EMPTY_EVENT;
  }

  /** Determines if the event is a delete event based on metadata. */
  private boolean isDeleteEvent(JsonNode changeEvent) {
    String changeType = getChangeType(changeEvent);
    return DatastreamConstants.DELETE_EVENT.equalsIgnoreCase(changeType);
  }

  /** Determines if the event is from dlq. */
  private boolean isDlqReconsumed(JsonNode changeEvent) {
    if (changeEvent.has(DatastreamConstants.IS_DLQ_RECONSUMED)) {
      return changeEvent
          .get(DatastreamConstants.IS_DLQ_RECONSUMED)
          .asText()
          .equalsIgnoreCase("true");
    }
    return false;
  }

  public MongoDbChangeEventContext(JsonNode payload, String shadowCollectionPrefix)
      throws JsonProcessingException {
    this.changeEvent =
        payload.has(DatastreamConstants.CHANGE_EVENT)
            ? payload.get(DatastreamConstants.CHANGE_EVENT)
            : payload;

    this.retryCount =
        changeEvent.has(DatastreamConstants.RETRY_COUNT)
            ? changeEvent.get(DatastreamConstants.RETRY_COUNT).asInt()
            : payload.has(DatastreamConstants.RETRY_COUNT)
                ? payload.get(DatastreamConstants.RETRY_COUNT).asInt()
                : 0;

    // Extract collection name from the event
    if (changeEvent.has(DatastreamConstants.EVENT_SOURCE_METADATA)) {
      this.dataCollection =
          changeEvent
              .get(DatastreamConstants.EVENT_SOURCE_METADATA)
              .get(DatastreamConstants.COLLECTION)
              .asText();
    } else {
      // Error will be handled by DLQ from the caller
      throw new IllegalStateException("Invalid event record without _metadata_source.");
    }

    this.shadowCollection = shadowCollectionPrefix + this.dataCollection;

    // Extract document id
    if (changeEvent.has(DatastreamConstants.MONGODB_DOCUMENT_ID)) {
      JsonNode docIdVal =
          OBJECT_MAPPER.readTree(changeEvent.get(DatastreamConstants.MONGODB_DOCUMENT_ID).asText());
      if (docIdVal.isIntegralNumber()) {
        this.documentId = docIdVal.asLong();
        if (this.documentId.equals(0L)) {
          LOG.error("Unsupported _id value of _id {}.", docIdVal);
          throw new IllegalArgumentException("Unsupported _id value.");
        }
      } else if (docIdVal.isTextual()) {
        this.documentId = docIdVal.asText();
      } else if (docIdVal.isObject()
          && docIdVal.has(OID_FIELD_NAME)
          && docIdVal.get(OID_FIELD_NAME).isTextual()) {
        this.documentId = new ObjectId(docIdVal.get(OID_FIELD_NAME).asText());
      } else {
        LOG.error("Unsupported _id type of _id {}.", docIdVal);
        throw new IllegalArgumentException("Unsupported _id type.");
      }
    } else {
      LOG.error("_id not found.");
      throw new IllegalArgumentException("Invalid event record without _id.");
    }

    // Extract timestamp
    if (changeEvent.has(DatastreamConstants.TIMESTAMP_SECONDS)
        && changeEvent.has(DatastreamConstants.TIMESTAMP_NANOS)) {
      JsonNode timestampSeconds = changeEvent.get(DatastreamConstants.TIMESTAMP_SECONDS);
      JsonNode timestampNanos = changeEvent.get(DatastreamConstants.TIMESTAMP_NANOS);
      this.timestampDoc =
          new Document(
              ImmutableMap.of(
                  TIMESTAMP_SECONDS_COL,
                  timestampSeconds.asLong(),
                  TIMESTAMP_NANOS_COL,
                  timestampNanos.asInt()));
    } else {
      LOG.error("sort_keys not found in change event");
      throw new IllegalArgumentException("Invalid event record without sort_keys");
    }

    // Determine event types
    this.isDeleteEvent = isDeleteEvent(changeEvent);

    this.jsonStringData = dataAsJsonString();
    this.shadowDocument = generateShadowDocument();
    this.isDlqReconsumed = isDlqReconsumed(changeEvent);
  }

  /** Creates a shadow document for tracking event ordering. */
  public Document generateShadowDocument() throws JsonProcessingException {
    JsonNode jsonNode = this.getChangeEvent();
    String jsonString = OBJECT_MAPPER.writeValueAsString(jsonNode);
    Document shadowDoc = Document.parse(jsonString);

    // Highlight primary key fields
    shadowDoc.put(SHADOW_DOC_ID_COL, documentId);
    shadowDoc.put(DOC_ID_COL, documentId);

    // Add timestamp information
    shadowDoc.put(TIMESTAMP_COL, this.timestampDoc);

    // Add processed timestamp
    shadowDoc.put("processed_at", System.currentTimeMillis());
    shadowDoc.put("is_from_dlq", isDlqReconsumed);

    Utils.removeTableRowFields(shadowDoc, MAPPER_IGNORE_FIELDS);

    return shadowDoc;
  }

  public String dataAsJsonString() throws JsonProcessingException {
    if (isDeleteEvent) {
      return null;
    }
    JsonNode jsonNode = this.getChangeEvent();
    return OBJECT_MAPPER.writeValueAsString(jsonNode);
  }

  public JsonNode getChangeEvent() {
    return changeEvent;
  }

  public String getDataCollection() {
    return dataCollection;
  }

  public String getShadowCollection() {
    return shadowCollection;
  }

  public Object getDocumentId() {
    return documentId;
  }

  public boolean isDeleteEvent() {
    return isDeleteEvent;
  }

  public Document getShadowDocument() {
    return shadowDocument;
  }

  public String getDataAsJsonString() {
    return jsonStringData;
  }

  public Document getTimestampDoc() {
    return timestampDoc;
  }

  public boolean getIsDlqReconsumed() {
    return isDlqReconsumed;
  }

  public int getRetryCount() {
    return retryCount;
  }

  /**
   * Override toString() to provide a proper JSON representation of this object. This ensures that
   * when the object is serialized to a string, it produces valid JSON.
   */
  @Override
  public String toString() {
    try {
      ObjectNode jsonNode = OBJECT_MAPPER.createObjectNode();

      // Add the original change event JSON
      jsonNode.set("changeEvent", this.changeEvent);

      // Add other important fields
      jsonNode.put("dataCollection", this.dataCollection);
      jsonNode.put("shadowCollection", this.shadowCollection);
      if (this.documentId instanceof ObjectId) {
        ObjectNode objectIdNode = OBJECT_MAPPER.createObjectNode();
        objectIdNode.put(OID_FIELD_NAME, this.documentId.toString());
        jsonNode.put("documentId", objectIdNode);
      } else {
        jsonNode.putPOJO("documentId", this.documentId);
      }
      jsonNode.put("isDeleteEvent", this.isDeleteEvent);

      // Convert timestamp document to JSON
      if (this.timestampDoc != null) {
        ObjectNode timestampNode = OBJECT_MAPPER.createObjectNode();
        timestampNode.put(TIMESTAMP_SECONDS_COL, this.timestampDoc.getLong(TIMESTAMP_SECONDS_COL));
        timestampNode.put(TIMESTAMP_NANOS_COL, this.timestampDoc.getInteger(TIMESTAMP_NANOS_COL));
        jsonNode.set(TIMESTAMP_COL, timestampNode);
      }

      jsonNode.put(DatastreamConstants.IS_DLQ_RECONSUMED, this.isDlqReconsumed);
      jsonNode.put(DatastreamConstants.RETRY_COUNT, this.retryCount);

      return OBJECT_MAPPER.writeValueAsString(jsonNode);
    } catch (JsonProcessingException e) {
      LOG.error("Error serializing MongoDbChangeEventContext to JSON: {}", e.getMessage(), e);
      // Fallback to a simple JSON structure with error information
      try {
        ObjectNode errorNode = OBJECT_MAPPER.createObjectNode();
        errorNode.put("error", "Failed to serialize MongoDbChangeEventContext");
        errorNode.put("errorMessage", e.getMessage());
        errorNode.putPOJO("documentId", this.documentId);
        return OBJECT_MAPPER.writeValueAsString(errorNode);
      } catch (JsonProcessingException ex) {
        // This should never happen with a simple ObjectNode
        LOG.error("Critical error creating error JSON: {}", ex.getMessage(), ex);
        return "{\"error\":\"Failed to serialize error message\"}";
      }
    }
  }

  public boolean equals(Object other) {
    if (other instanceof MongoDbChangeEventContext) {
      return Objects.equals(this.toString(), other.toString());
    }
    return false;
  }
}
