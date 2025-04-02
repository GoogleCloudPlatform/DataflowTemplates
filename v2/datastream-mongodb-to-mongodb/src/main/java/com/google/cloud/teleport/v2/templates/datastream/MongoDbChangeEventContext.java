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

import static com.google.cloud.teleport.v2.templates.DataStreamMongoDBToMongoDB.MAPPER_IGNORE_FIELDS;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.teleport.v2.transforms.Utils;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.util.Objects;
import org.bson.Document;
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

  private final JsonNode changeEvent;
  private final String dataCollection;
  private final String shadowCollection;
  private final Object documentId;
  private final Document shadowDocument;
  private final Document dataDocument;
  private final boolean isDeleteEvent;
  private final Document timestampDoc;

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

  public MongoDbChangeEventContext(JsonNode changeEvent, String shadowCollectionPrefix)
      throws JsonProcessingException {
    this.changeEvent = changeEvent;

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
      JsonNode docIdVal = changeEvent.get(DatastreamConstants.MONGODB_DOCUMENT_ID);
      if (docIdVal.isLong()) {
        this.documentId = docIdVal.asLong();
      } else if (docIdVal.isTextual()) {
        this.documentId = docIdVal.asText();
      } else if (docIdVal.isObject()) {
        // TODO: Better handling of objectId type _id values.
        this.documentId = docIdVal;
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

    this.dataDocument = generateDataDocument();
    this.shadowDocument = generateShadowDocument();
  }

  /** Creates a shadow document for tracking event ordering. */
  public Document generateShadowDocument() throws JsonProcessingException {
    JsonNode jsonNode = this.getChangeEvent();
    String jsonString = OBJECT_MAPPER.writeValueAsString(jsonNode);
    Document shadowDoc = Document.parse(jsonString);

    // Highlight primary key fields
    shadowDoc.put(SHADOW_DOC_ID_COL, documentId);

    // Add timestamp information
    shadowDoc.put(TIMESTAMP_COL, this.timestampDoc);

    // Add processed timestamp
    shadowDoc.put("processed_at", System.currentTimeMillis());

    Utils.removeTableRowFields(shadowDoc, MAPPER_IGNORE_FIELDS);

    return shadowDoc;
  }

  public Document generateDataDocument() throws JsonProcessingException {
    if (isDeleteEvent) {
      return null;
    }
    JsonNode jsonNode = this.getChangeEvent();
    String jsonString = OBJECT_MAPPER.writeValueAsString(jsonNode);
    Document rawDoc = Document.parse(Document.parse(jsonString).get(DATA_COL).toString());
    rawDoc.put(MongoDbChangeEventContext.DOC_ID_COL, documentId);
    return rawDoc;
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

  public Document getDataDocument() {
    return dataDocument;
  }

  public Document getTimestampDoc() {
    return timestampDoc;
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
      jsonNode.putPOJO("documentId", this.documentId);
      jsonNode.put("isDeleteEvent", this.isDeleteEvent);

      // Convert timestamp document to JSON
      if (this.timestampDoc != null) {
        ObjectNode timestampNode = OBJECT_MAPPER.createObjectNode();
        timestampNode.put(TIMESTAMP_SECONDS_COL, this.timestampDoc.getLong(TIMESTAMP_SECONDS_COL));
        timestampNode.put(TIMESTAMP_NANOS_COL, this.timestampDoc.getInteger(TIMESTAMP_NANOS_COL));
        jsonNode.set(TIMESTAMP_COL, timestampNode);
      }

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
