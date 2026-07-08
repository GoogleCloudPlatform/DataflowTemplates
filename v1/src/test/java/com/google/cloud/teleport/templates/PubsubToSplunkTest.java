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
import static org.junit.Assert.assertEquals;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
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

  /** Tests extraction of original payload from HEC-formatted JSON. */
  @Test
  public void testExtractOriginalPayloadFromHec_withStringEvent() {
    String hecPayload = "{\"event\":\"simple text message\",\"time\":1634567890}";

    String extracted = PubSubToSplunk.extractOriginalPayloadFromHec(hecPayload);

    assertEquals("simple text message", extracted);
  }

  /** Tests extraction when event is a JSON object. */
  @Test
  public void testExtractOriginalPayloadFromHec_withJsonEvent() {
    String hecPayload =
        "{\"event\":{\"userId\":123,\"action\":\"login\"},\"time\":1634567890,\"host\":\"web-01\"}";

    String extracted = PubSubToSplunk.extractOriginalPayloadFromHec(hecPayload);

    // The extracted event should be the JSON object as a string
    JsonObject extractedJson = GSON.fromJson(extracted, JsonObject.class);
    assertThat(extractedJson.get("userId").getAsInt()).isEqualTo(123);
    assertThat(extractedJson.get("action").getAsString()).isEqualTo("login");
  }

  /** Tests extraction when payload is already unwrapped (not HEC format). */
  @Test
  public void testExtractOriginalPayloadFromHec_alreadyUnwrapped() {
    String originalPayload = "{\"userId\":123,\"action\":\"login\"}";

    String extracted = PubSubToSplunk.extractOriginalPayloadFromHec(originalPayload);

    // Should return as-is since no "event" field
    assertEquals(originalPayload, extracted);
  }

  /** Tests extraction with plain text (not JSON). */
  @Test
  public void testExtractOriginalPayloadFromHec_plainText() {
    String plainText = "This is just plain text, not JSON";

    String extracted = PubSubToSplunk.extractOriginalPayloadFromHec(plainText);

    // Should return as-is when parsing fails
    assertEquals(plainText, extracted);
  }

  /** Tests extraction with complex HEC format including all metadata fields. */
  @Test
  public void testExtractOriginalPayloadFromHec_fullHecFormat() {
    String fullHecPayload =
        "{\"event\":{\"log\":\"Application started\",\"level\":\"INFO\"},"
            + "\"time\":1634567890,"
            + "\"host\":\"app-server-01\","
            + "\"source\":\"application\","
            + "\"sourcetype\":\"json\","
            + "\"index\":\"main\","
            + "\"fields\":{\"environment\":\"production\"}}";

    String extracted = PubSubToSplunk.extractOriginalPayloadFromHec(fullHecPayload);

    // Should extract just the event field
    JsonObject extractedJson = GSON.fromJson(extracted, JsonObject.class);
    assertThat(extractedJson.get("log").getAsString()).isEqualTo("Application started");
    assertThat(extractedJson.get("level").getAsString()).isEqualTo("INFO");

    // Verify other HEC fields are NOT in the extracted payload
    assertThat(extractedJson.has("time")).isFalse();
    assertThat(extractedJson.has("host")).isFalse();
  }
}
