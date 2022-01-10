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
package com.google.cloud.teleport.templates;

import static com.google.common.truth.Truth.assertThat;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;

/** Unit tests for {@link PubSubToSplunk}. */
public class PubsubToSplunkTest {

  private static final Gson GSON = new Gson();

  /** Tests whether a Pub/Sub message with a {@link String} payload is parsed correctly. */
  @Test
  public void testStringPubsubMessage() {
    String payload = "payload";
    Map<String, String> attributes = ImmutableMap.of("key", "value");

    PubsubMessage pubsubMessage = new PubsubMessage(payload.getBytes(), attributes);

    String formattedPubsubMessage = PubSubToSplunk.formatPubsubMessage(pubsubMessage);

    assertThat(formattedPubsubMessage).contains(PubSubToSplunk.PUBSUB_MESSAGE_DATA_FIELD);
    assertThat(formattedPubsubMessage).contains(PubSubToSplunk.PUBSUB_MESSAGE_ATTRIBUTE_FIELD);

    JsonObject messageJson = GSON.fromJson(formattedPubsubMessage, JsonObject.class);
    assertThat(messageJson.get(PubSubToSplunk.PUBSUB_MESSAGE_DATA_FIELD).getAsString())
        .isEqualTo(payload);
  }

  /** Tests whether a Pub/Sub message with JSON payload is parsed correctly. */
  @Test
  public void testJsonPubsubMessage() {
    String payload =
        "{\"type\":\"object\",\"properties\":{\"id\":{\"@type\":\"string\",\"ipsum\":\"id\"}}}";
    Map<String, String> attributes = ImmutableMap.of("key", "value");

    PubsubMessage pubsubMessage = new PubsubMessage(payload.getBytes(), attributes);

    String formattedPubsubMessage = PubSubToSplunk.formatPubsubMessage(pubsubMessage);

    assertThat(formattedPubsubMessage).contains(PubSubToSplunk.PUBSUB_MESSAGE_DATA_FIELD);
    assertThat(formattedPubsubMessage).contains(PubSubToSplunk.PUBSUB_MESSAGE_ATTRIBUTE_FIELD);

    JsonObject messageJson = GSON.fromJson(formattedPubsubMessage, JsonObject.class);
    assertThat(messageJson.getAsJsonObject(PubSubToSplunk.PUBSUB_MESSAGE_DATA_FIELD).toString())
        .isEqualTo(payload);
  }

  /** Tests whether a Pub/Sub message with complex attributes is parsed correctly. */
  @Test
  public void testPubsubMessageWithComplexAttributes() {
    String payload = "payload";
    Map<String, String> attributes =
        ImmutableMap.of("logging.googleapis.com/timestamp", "2021-03-17T17:14:23.270642Z");

    PubsubMessage pubsubMessage = new PubsubMessage(payload.getBytes(), attributes);

    String formattedPubsubMessage = PubSubToSplunk.formatPubsubMessage(pubsubMessage);

    assertThat(formattedPubsubMessage).contains(PubSubToSplunk.PUBSUB_MESSAGE_DATA_FIELD);
    assertThat(formattedPubsubMessage).contains(PubSubToSplunk.PUBSUB_MESSAGE_ATTRIBUTE_FIELD);

    JsonObject messageJson = GSON.fromJson(formattedPubsubMessage, JsonObject.class);
    JsonObject attributesJson =
        messageJson.getAsJsonObject(PubSubToSplunk.PUBSUB_MESSAGE_ATTRIBUTE_FIELD);

    for (String key : attributes.keySet()) {
      assertThat(attributesJson.get(key).getAsString()).isEqualTo(attributes.get(key));
    }
  }
}
