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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.elasticsearch.utils.ElasticsearchUtils;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;


/**
 * The {@link FailedElasticsearchMessageToPubsubTopicFn} converts PubSub message which have failed processing into
 * {@link com.google.api.services.pubsub.model.PubsubMessage} objects which can be output to a PubSub topic.
 */
public class FailedElasticsearchMessageToPubsubTopicFn
        extends DoFn<String, PubsubMessage> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /** Counter to track total failed messages. */
    private static final Counter ERROR_MESSAGES_COUNTER =
            Metrics.counter(FailedElasticsearchMessageToPubsubTopicFn.class, "total-failed-messages");

    @ProcessElement
    public void processElement(ProcessContext context) {
        String input = context.element();

        // Build the output PubSub message
        JsonNode outputMessage = null;
        try {
            outputMessage = OBJECT_MAPPER.readTree(input);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        ERROR_MESSAGES_COUNTER.inc();

        context.output(new PubsubMessage(outputMessage.toString().getBytes(StandardCharsets.UTF_8), null));
    }

}
