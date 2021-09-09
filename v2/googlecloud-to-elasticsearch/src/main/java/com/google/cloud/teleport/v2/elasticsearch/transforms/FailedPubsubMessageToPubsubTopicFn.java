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
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * The {@link FailedPubsubMessageToPubsubTopicFn} converts PubSub message which have failed processing into
 * {@link com.google.api.services.pubsub.model.PubsubMessage} objects which can be output to a PubSub topic.
 */
public class FailedPubsubMessageToPubsubTopicFn
        extends DoFn<FailsafeElement<PubsubMessage, String>, PubsubMessage> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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
            timestamp = ElasticsearchUtils.getTimestampFromOriginalPayload(OBJECT_MAPPER.readTree(message));
        } catch (Exception e) {
            timestamp = new java.sql.Timestamp(System.currentTimeMillis()).toInstant().toString();
        }

        // Build the output PubSub message
        ObjectNode outputMessage = OBJECT_MAPPER.createObjectNode();
        outputMessage
                .put("@timestamp", timestamp)
                .put("errorMessage", failsafeElement.getErrorMessage())
                .put("stacktrace", failsafeElement.getStacktrace())
                .put("payloadString", message);

        context.output(new PubsubMessage(outputMessage.toString().getBytes(StandardCharsets.UTF_8), null));
    }

    /*private String getOrGenerateTimestamp(JsonNode node) throws NoSuchElementException {
        if(node.has("timestamp")) {
            return node.get("timestamp").asText();
        } else {
            if (node.has("protoPayload")
                    && node.get("protoPayload").has("timestamp")) {
                return node.get("protoPayload").get("timestamp").asText();
            }
        }

        return new java.sql.Timestamp(System.currentTimeMillis()).toInstant().toString();
    }*/

}
