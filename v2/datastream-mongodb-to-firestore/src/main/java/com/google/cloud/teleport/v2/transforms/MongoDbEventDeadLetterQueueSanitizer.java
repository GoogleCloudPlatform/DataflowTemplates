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
package com.google.cloud.teleport.v2.transforms;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.teleport.v2.cdc.dlq.DeadLetterQueueSanitizer;
import com.google.cloud.teleport.v2.templates.datastream.DatastreamConstants;
import com.google.cloud.teleport.v2.templates.datastream.MongoDbChangeEventContext;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The MongoDbEventDeadLetterQueueSanitizer cleans and prepares failed MongoDbEvent to BsonDocument
 * conversion to be stored in a GCS Dead Letter Queue.
 */
public class MongoDbEventDeadLetterQueueSanitizer
    extends DeadLetterQueueSanitizer<
        FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext>, String> {
  private static final Logger LOG =
      LoggerFactory.getLogger(MongoDbEventDeadLetterQueueSanitizer.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Override
  public String getJsonMessage(
      FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext> input) {
    MongoDbChangeEventContext changeEvent = input.getOriginalPayload();
    try {
      // Serialize the change event to JSON
      ObjectNode jsonNode = OBJECT_MAPPER.createObjectNode();

      // Add the original change event JSON
      jsonNode.set("changeEvent", changeEvent.getChangeEvent());

      // Add other important fields
      jsonNode.put("dataCollection", changeEvent.getDataCollection());
      jsonNode.put("shadowCollection", changeEvent.getShadowCollection());
      jsonNode.putPOJO("documentId", changeEvent.getDocumentId());
      jsonNode.put("isDeleteEvent", changeEvent.isDeleteEvent());
      jsonNode.put(DatastreamConstants.IS_DLQ_RECONSUMED, true);
      jsonNode.put(DatastreamConstants.RETRY_COUNT, changeEvent.getRetryCount() + 1);

      return OBJECT_MAPPER.writeValueAsString(jsonNode);
    } catch (JsonProcessingException e) {
      LOG.error("Error serializing MongoDbChangeEventContext to JSON: {}", e.getMessage(), e);
      // Fallback to a simple JSON structure with error information
      try {
        ObjectNode errorNode = OBJECT_MAPPER.createObjectNode();
        errorNode.put("error", "Failed to serialize MongoDbChangeEventContext");
        errorNode.put("errorMessage", e.getMessage());
        errorNode.putPOJO("documentId", changeEvent.getDocumentId());
        return OBJECT_MAPPER.writeValueAsString(errorNode);
      } catch (JsonProcessingException ex) {
        // This should never happen with a simple ObjectNode
        LOG.error("Critical error creating error JSON: {}", ex.getMessage(), ex);
        return "{\"error\":\"Failed to serialize error message\"}";
      }
    }
  }

  @Override
  public String getErrorMessageJson(
      FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext> input) {
    try {
      MongoDbChangeEventContext eventContext = input.getOriginalPayload();
      ObjectNode errorNode = OBJECT_MAPPER.createObjectNode();
      errorNode.put("errorType", input.getErrorMessage());
      errorNode.putPOJO("documentId", eventContext.getDocumentId());
      errorNode.put("collection", eventContext.getDataCollection());
      return OBJECT_MAPPER.writeValueAsString(errorNode);
    } catch (JsonProcessingException e) {
      LOG.error("Error creating error message JSON: {}", e.getMessage(), e);
      return "{\"error\":\"Failed to create error message\"}";
    }
  }
}
