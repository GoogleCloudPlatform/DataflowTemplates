/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.elasticsearch.transforms;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.teleport.v2.elasticsearch.utils.ElasticsearchUtils;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * The {@link FailedPubsubMessageToPubsubTopicFn} converts Pub/Sub messages which have failed
 * processing into {@link com.google.api.services.pubsub.model.PubsubMessage} objects which can be
 * output to a PubSub topic.
 */
public class FailedPubsubMessageToPubsubTopicFn
    extends DoFn<FailsafeElement<PubsubMessage, String>, PubsubMessage> {

  private static final String ERROR_MESSAGE = "errorMessage";
  private static final String ERROR_STACKTRACE = "stackTrace";
  private static final String ERROR_TIMESTAMP = "timestamp";
  private static final String ERROR_PAYLOAD_STRING = "payloadString";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /** Counter to track total failed messages. */
  private static final Counter ERROR_MESSAGES_COUNTER =
      Metrics.counter(FailedPubsubMessageToPubsubTopicFn.class, "total-failed-messages");

  @ProcessElement
  public void processElement(ProcessContext context) {
    FailsafeElement<PubsubMessage, String> failsafeElement = context.element();
    PubsubMessage pubsubMessage = failsafeElement.getOriginalPayload();
    String message =
        pubsubMessage.getPayload().length > 0
            ? new String(pubsubMessage.getPayload())
            : pubsubMessage.getAttributeMap().toString();

    // Format the timestamp for insertion
    String timestamp;
    try {
      timestamp =
          ElasticsearchUtils.getTimestampFromOriginalPayload(OBJECT_MAPPER.readTree(message));
    } catch (Exception e) {
      timestamp = new java.sql.Timestamp(System.currentTimeMillis()).toInstant().toString();
    }

    // Build the output PubSub message
    ObjectNode outputMessage = OBJECT_MAPPER.createObjectNode();
    outputMessage
        .put(ERROR_TIMESTAMP, timestamp)
        .put(ERROR_MESSAGE, failsafeElement.getErrorMessage())
        .put(ERROR_STACKTRACE, failsafeElement.getStacktrace())
        .put(ERROR_PAYLOAD_STRING, message);

    ERROR_MESSAGES_COUNTER.inc();

    Map<String, String> attributes = new HashMap<>(pubsubMessage.getAttributeMap());
    attributes.put(ERROR_MESSAGE, failsafeElement.getErrorMessage());

    context.output(
        new PubsubMessage(outputMessage.toString().getBytes(StandardCharsets.UTF_8), attributes));
  }
}
