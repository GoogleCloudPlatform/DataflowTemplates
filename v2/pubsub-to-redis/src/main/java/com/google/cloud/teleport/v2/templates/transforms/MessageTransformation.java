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

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
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

  private static final ObjectMapper objectMapper = new ObjectMapper();

  static String attributeKey;
  static String messageId;
  public static String key;
  public static String hashKey;
  public static String hashKeyPrefix = "hash:";
  public static String logKeyPrefix = "log:";
  public static String namespaceDelimiter = ":";
  static String fieldKey = "data";

  /** Configures Pubsub consumer. */
  public static PubsubIO.@UnknownKeyFor @NonNull @Initialized Read<
          @UnknownKeyFor @NonNull @Initialized PubsubMessage>
      readFromPubSub(String inputSubscription) {
    PubsubIO.@UnknownKeyFor @NonNull @Initialized Read<
            @UnknownKeyFor @NonNull @Initialized PubsubMessage>
        pubSubRecords = PubsubIO.readMessagesWithAttributesAndMessageId();
    return pubSubRecords.fromSubscription(inputSubscription);
  }

  public static class MessageToRedisString extends DoFn<PubsubMessage, String> {
    @ProcessElement
    public void processElement(
        @Element PubsubMessage pubsubMessage, OutputReceiver<String> receiver) {
      String element = new String(pubsubMessage.getPayload());
      messageId = pubsubMessage.getMessageId();
      LOG.debug("PubSubMessage messageId: " + messageId);

      if (pubsubMessage.getAttribute("key") != null) {
        attributeKey = pubsubMessage.getAttribute("key");
        LOG.debug("PubSubMessage attributeKey: " + attributeKey);
        key = attributeKey + namespaceDelimiter + messageId;
      } else {
        attributeKey = "";
        key = messageId;
      }
      receiver.output(element);
    }
  }

  public static class MessageToRedisHash
      extends DoFn<PubsubMessage, KV<String, KV<String, String>>> {
    @ProcessElement
    public void processElement(
        @Element PubsubMessage pubsubMessage,
        OutputReceiver<KV<String, KV<String, String>>> receiver) {
      String element = new String(pubsubMessage.getPayload());
      messageId = pubsubMessage.getMessageId();
      LOG.debug("PubSubMessage messageId: " + messageId);

      if (pubsubMessage.getAttribute("key") != null) {
        attributeKey = pubsubMessage.getAttribute("key");
        LOG.debug("PubSubMessage attributeKey: " + attributeKey);
        hashKey = hashKeyPrefix + attributeKey + namespaceDelimiter + messageId;
      } else {
        hashKey = hashKeyPrefix + messageId;
      }
      receiver.output(KV.of(hashKey, KV.of(fieldKey, element)));
    }
  }

  public static class MessageToRedisLogs
      extends DoFn<PubsubMessage, KV<String, KV<String, String>>> {
    public boolean isValidJSON(String json) {
      try {
        objectMapper.readTree(json);
      } catch (JacksonException e) {
        return false;
      }
      return true;
    }

    @ProcessElement
    public void processElement(
        @Element PubsubMessage pubsubMessage,
        OutputReceiver<KV<String, KV<String, String>>> receiver)
        throws JsonProcessingException {
      String element = new String(pubsubMessage.getPayload());
      messageId = pubsubMessage.getMessageId();
      LOG.debug("PubSubMessage messageId: " + messageId);

      if (element.isEmpty() && element.isBlank()) {
        return;
      }

      objectMapper.enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
      if (!isValidJSON(element)) {
        return;
      }

      Map<String, Object> elementMapData = objectMapper.readValue(element, Map.class);

      if (elementMapData.containsKey("timestamp")) {
        LocalDateTime localDateTime =
            LocalDateTime.parse(
                String.valueOf(elementMapData.get("timestamp")), DateTimeFormatter.ISO_DATE_TIME);
        ZonedDateTime zonedDateTime = ZonedDateTime.of(localDateTime, ZoneId.systemDefault());
        long timestampValueAsLong = zonedDateTime.toInstant().toEpochMilli();
        elementMapData.put("timestampAsLong", String.valueOf(timestampValueAsLong));
        hashKey = logKeyPrefix + timestampValueAsLong + namespaceDelimiter + messageId;
      } else {
        hashKey = logKeyPrefix + messageId;
      }
      elementMapData.forEach(
          (k, v) -> receiver.output(KV.of(hashKey, KV.of(k, String.valueOf(v)))));
    }
  }

  public static class MessageToRedisStreams
      extends DoFn<PubsubMessage, KV<String, Map<String, String>>> {
    @ProcessElement
    public void processElement(
        @Element PubsubMessage pubsubMessage,
        OutputReceiver<KV<String, Map<String, String>>> receiver) {
      String element = new String(pubsubMessage.getPayload());
      messageId = pubsubMessage.getMessageId();
      LOG.debug("PubSubMessage messageId: " + messageId);
      if (pubsubMessage.getAttribute("key") != null) {
        attributeKey = pubsubMessage.getAttribute("key");
        LOG.debug("PubSubMessage attributeKey: " + attributeKey);
        key = attributeKey + namespaceDelimiter + messageId;
      } else {
        attributeKey = "";
        key = messageId;
      }
      receiver.output(KV.of(key, Map.of(key, element)));
    }
  }
}
