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
package com.google.cloud.teleport.v2.cdc.dlq;

import java.io.IOException;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO when this is a flex template it could incorporate outputs with configurable dest?
/**
 * The DeadLetterQueueSanitizer is an abstract handler to clean and prepare pipeline failures to be
 * stored in a GCS Dead Letter Queue.
 *
 * <p>Extending the DeadLetterQueueSanitizer requires only to implement getJsonMessage() and
 * getErrorMessageJson().
 *
 * <p>NOTE: The input to a Sanitizer is flexible but the output must be a String unless your
 * override formatMessage()
 */
public class DeadLetterQueueSanitizer<InputT, OutputT> extends SimpleFunction<InputT, OutputT> {

  private static final Logger LOG = LoggerFactory.getLogger(DeadLetterQueueSanitizer.class);

  // public DeadLetterQueueSanitizer() {}

  @Override
  public OutputT apply(InputT input) {
    // Extract details required for DLQ Storage
    String rawJson = getJsonMessage(input) == null ? "" : getJsonMessage(input);
    String errorMessageJson = getErrorMessageJson(input) == null ? "" : getErrorMessageJson(input);

    return formatMessage(rawJson, errorMessageJson);
  }

  // NOTE: Override these 2 functions to extend this class
  public String getJsonMessage(InputT input) {
    return "";
  }

  public String getErrorMessageJson(InputT input) {
    return "";
  }

  // NOTE: Only override formatMessage if required or you desire a non-String output
  public OutputT formatMessage(String rawJson, String errorMessageJson) {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode resultNode = mapper.createObjectNode();
    try {
      JsonNode node = mapper.readTree(rawJson);
      resultNode.put("message", node);
    } catch (IOException e) {
      resultNode.put("message", rawJson);
    }
    try {
      JsonNode node = mapper.readTree(errorMessageJson);
      resultNode.put("error_message", node);
    } catch (IOException e) {
      resultNode.put("error_message", errorMessageJson);
    }
    return (OutputT) resultNode.toString();
  }
}
