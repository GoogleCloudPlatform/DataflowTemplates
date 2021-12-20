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

import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: Jackson2 is adding \n chars into the JSON which is not desired

/**
 * The BigQueryDeadLetterQueueSanitizer cleans and prepares failed BigQuery inserts to be stored in
 * a GCS Dead Letter Queue. NOTE: The input to a Sanitizer is flexible but the output must be a
 * String unless your override formatMessage()
 */
public class PubSubDeadLetterQueueSanitizer
    extends DeadLetterQueueSanitizer<FailsafeElement<PubsubMessage, String>, String> {

  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
  private static final Logger LOG = LoggerFactory.getLogger(PubSubDeadLetterQueueSanitizer.class);

  @Override
  public String getJsonMessage(FailsafeElement<PubsubMessage, String> input) {
    PubsubMessage pubsubMessage = input.getOriginalPayload();
    String message;
    try {
      message =
          pubsubMessage.getPayload().length > 0
              ? new String(pubsubMessage.getPayload())
              : pubsubMessage.getAttributeMap().toString();
    } catch (Exception e) {
      // Ignore exception and print bad format
      message =
          String.format(
              "\"%s\"",
              message =
                  pubsubMessage.getPayload().length > 0
                      ? new String(pubsubMessage.getPayload())
                      : pubsubMessage.getAttributeMap().toString());
    }

    return message;
  }

  @Override
  public String getErrorMessageJson(FailsafeElement<PubsubMessage, String> input) {
    return input.getErrorMessage();
  }
}
