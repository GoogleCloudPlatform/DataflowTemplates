/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.templates.transforms;

import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Different transformations over the processed data in the pipeline. */
public class MessageTransformation {
  private static final Logger LOG = LoggerFactory.getLogger(MessageTransformation.class);

  static String attributeKey;
  static String messageId;
  public static String key;
  static String fieldKey;

  /** Configures Pubsub consumer. */
  public static PubsubIO.@UnknownKeyFor @NonNull @Initialized Read<@UnknownKeyFor @NonNull @Initialized PubsubMessage> readFromPubSub(String inputSubscription) {
    PubsubIO.@UnknownKeyFor @NonNull @Initialized Read<@UnknownKeyFor @NonNull @Initialized PubsubMessage> pubSubRecords = PubsubIO.readMessagesWithAttributesAndMessageId();
    return pubSubRecords.fromSubscription(inputSubscription);
  }

  public static class MessageToRedisString extends DoFn<PubsubMessage, String> {
    @ProcessElement
    public void processElement(
            @Element PubsubMessage pubsubMessage, OutputReceiver<String> receiver) {
      String element = new String(pubsubMessage.getPayload());
      messageId = pubsubMessage.getMessageId();
      LOG.debug("PubSubMessage messageId: " + messageId);
      LOG.debug("PubSubMessage payload: " + element);
      if (pubsubMessage.getAttribute("key") != null) {
        attributeKey = pubsubMessage.getAttribute("key");
        LOG.debug("PubSubMessage attributeKey: " + attributeKey);
        key = attributeKey + ":" + messageId;
      } else {
        attributeKey = "";
        key = messageId;
      }
      receiver.output(element);
    }
  }

  public static class MessageToRedisHash extends DoFn<PubsubMessage, KV<String, KV<String, String>>> {
    @ProcessElement
    public void processElement(
            @Element PubsubMessage pubsubMessage, OutputReceiver<KV<String, KV<String, String>>> receiver) {
      String element = new String(pubsubMessage.getPayload());
      messageId = pubsubMessage.getMessageId();
      LOG.debug("PubSubMessage messageId: " + messageId);
      LOG.debug("PubSubMessage payload: " + element);
      if (pubsubMessage.getAttribute("key") != null) {
        attributeKey = pubsubMessage.getAttribute("key");
        LOG.debug("PubSubMessage attributeKey: " + attributeKey);
        key = attributeKey + ":" + messageId;
        fieldKey = key.substring(0, key.lastIndexOf(":"));
      } else {
        attributeKey = "";
        key = messageId;
        fieldKey = key;
      }
      receiver.output(KV.of(key, KV.of(fieldKey, element)));
    }
  }

  public static class MessageToRedisStreams extends DoFn<PubsubMessage, KV<String, Map<String, String>>> {
    @ProcessElement
    public void processElement(
            @Element PubsubMessage pubsubMessage, OutputReceiver<KV<String, Map<String, String>>> receiver) {
      String element = new String(pubsubMessage.getPayload());
      messageId = pubsubMessage.getMessageId();
      LOG.debug("PubSubMessage messageId: " + messageId);
      LOG.debug("PubSubMessage payload: " + element);
      if (pubsubMessage.getAttribute("key") != null) {
        attributeKey = pubsubMessage.getAttribute("key");
        LOG.debug("PubSubMessage attributeKey: " + attributeKey);
        key = attributeKey + ":" + messageId;
      } else {
        attributeKey = "";
        key = messageId;
      }
      receiver.output(KV.of(key, Map.of(key, element)));
    }
  }
}
