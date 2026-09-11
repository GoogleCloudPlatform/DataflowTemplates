/*
 * Copyright (C) 2026 Google LLC
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.teleport.v2.templates.datastream.MongoDbChangeEventContext;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.util.CoderUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link MongoDbChangeEventContextCoder}. */
@RunWith(JUnit4.class)
public class MongoDbChangeEventContextCoderTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String SHADOW_PREFIX = "shadow_";

  private final MongoDbChangeEventContextCoder coder = MongoDbChangeEventContextCoder.of();

  private JsonNode insertEventJson;
  private JsonNode updateEventJson;
  private JsonNode deleteEventJson;
  private JsonNode readBackfillEventJson;

  @Before
  public void setUp() throws Exception {
    insertEventJson =
        OBJECT_MAPPER.readTree(
            """
            {
              "_metadata_source": {
                "collection": "users"
              },
              "_id": "{\\\"$oid\\\": \\\"645c9a7e7b8b1a0e9c0f8b3a\\\"}",
              "data": {
                "name": "Alice",
                "age": 30
              },
              "_metadata_timestamp_seconds": 1683782270,
              "_metadata_timestamp_nanos": 123456789,
              "op": "i"
            }\
            """);

    updateEventJson =
        OBJECT_MAPPER.readTree(
            """
            {
              "_metadata_source": {
                "collection": "users"
              },
              "_id": "{\\\"$oid\\\": \\\"645c9a7e7b8b1a0e9c0f8b3a\\\"}",
              "data": {
                "name": "Alice Smith",
                "age": 31
              },
              "_metadata_timestamp_seconds": 1683782275,
              "_metadata_timestamp_nanos": 200,
              "op": "u",
              "_metadata_change_type": "UPDATE"
            }\
            """);

    deleteEventJson =
        OBJECT_MAPPER.readTree(
            """
            {
              "_metadata_source": {
                "collection": "users"
              },
              "_id": "{\\\"$oid\\\": \\\"645c9a7e7b8b1a0e9c0f8b3a\\\"}",
              "_metadata_timestamp_seconds": 1683782280,
              "_metadata_timestamp_nanos": 300,
              "op": "d",
              "_metadata_change_type": "DELETE"
            }\
            """);

    readBackfillEventJson =
        OBJECT_MAPPER.readTree(
            """
            {
              "_metadata_source": {
                "collection": "users"
              },
              "_id": "{\\\"$oid\\\": \\\"645c9a7e7b8b1a0e9c0f8b3a\\\"}",
              "data": {
                "name": "Alice",
                "age": 30
              },
              "_metadata_timestamp_seconds": 1683782260,
              "_metadata_timestamp_nanos": 999999000,
              "op": "r",
              "_metadata_change_type": "READ",
              "_metadata_read_method": "backfill"
            }\
            """);
  }

  @Test
  public void testEncodeDecodeRoundTrip_insertEvent() throws Exception {
    MongoDbChangeEventContext context =
        new MongoDbChangeEventContext(insertEventJson, SHADOW_PREFIX);
    MongoDbChangeEventContext decoded = CoderUtils.clone(coder, context);

    assertEquals(context, decoded);
    assertEquals("users", decoded.getDataCollection());
    assertEquals("shadow_users", decoded.getShadowCollection());
    assertEquals(SHADOW_PREFIX, decoded.getShadowCollectionPrefix());
    assertTrue(decoded.getDocumentId() instanceof ObjectId);
    assertEquals("645c9a7e7b8b1a0e9c0f8b3a", decoded.getDocumentId().toString());
    assertEquals(1683782270L, decoded.getTimestampSeconds());
    assertEquals(123456789L, decoded.getTimestampSubSeconds());
    assertFalse(decoded.isDeleteEvent());
    assertNotNull(decoded.getDataAsJsonString());
    assertFalse(decoded.getIsDlqReconsumed());
    assertEquals(0, decoded.getRetryCount());
    assertEquals(context.getChangeEvent(), decoded.getChangeEvent());
    assertEquals(context.getOriginalChangeEvent(), decoded.getOriginalChangeEvent());
    CoderProperties.coderDecodeEncodeEqual(coder, context);
  }

  @Test
  public void testEncodeDecodeRoundTrip_updateEvent() throws Exception {
    MongoDbChangeEventContext context =
        new MongoDbChangeEventContext(updateEventJson, SHADOW_PREFIX);
    MongoDbChangeEventContext decoded = CoderUtils.clone(coder, context);

    assertEquals(context, decoded);
    assertTrue(decoded.isUpdateEvent());
    assertFalse(decoded.isDeleteEvent());
    assertEquals(1683782275L, decoded.getTimestampSeconds());
    assertEquals(200L, decoded.getTimestampSubSeconds());
    CoderProperties.coderDecodeEncodeEqual(coder, context);
  }

  @Test
  public void testEncodeDecodeRoundTrip_deleteEvent() throws Exception {
    MongoDbChangeEventContext context =
        new MongoDbChangeEventContext(deleteEventJson, SHADOW_PREFIX);
    MongoDbChangeEventContext decoded = CoderUtils.clone(coder, context);

    assertEquals(context, decoded);
    assertTrue(decoded.isDeleteEvent());
    assertNull(decoded.getDataAsJsonString());
    assertEquals(1683782280L, decoded.getTimestampSeconds());
    CoderProperties.coderDecodeEncodeEqual(coder, context);
  }

  @Test
  public void testEncodeDecodeRoundTrip_readBackfillEvent() throws Exception {
    MongoDbChangeEventContext context =
        new MongoDbChangeEventContext(readBackfillEventJson, SHADOW_PREFIX);
    MongoDbChangeEventContext decoded = CoderUtils.clone(coder, context);

    assertEquals(context, decoded);
    assertTrue(decoded.isBackfillEvent());
    assertFalse(decoded.isCdcEvent());
    assertEquals(999999000L, decoded.getTimestampSubSeconds());
    CoderProperties.coderDecodeEncodeEqual(coder, context);
  }

  @Test
  public void testEncodeDecodeRoundTrip_udfPayloadPreservation() throws Exception {
    JsonNode originalEvent = insertEventJson.deepCopy();
    JsonNode modifiedEvent = insertEventJson.deepCopy();
    ((ObjectNode) modifiedEvent.get("data")).put("transformedByUdf", true);

    MongoDbChangeEventContext context =
        new MongoDbChangeEventContext(modifiedEvent, originalEvent, SHADOW_PREFIX);
    MongoDbChangeEventContext decoded = CoderUtils.clone(coder, context);

    assertEquals(context, decoded);
    // Verify modified event has the UDF-added field
    assertTrue(decoded.getChangeEvent().get("data").has("transformedByUdf"));
    assertTrue(decoded.getChangeEvent().get("data").get("transformedByUdf").asBoolean());

    // Verify original event DOES NOT have the UDF-added field (original is preserved)
    assertFalse(decoded.getOriginalChangeEvent().get("data").has("transformedByUdf"));
    CoderProperties.coderDecodeEncodeEqual(coder, context);
  }

  @Test
  public void testEncodeDecodeRoundTrip_dlqReconsumedWithRetries() throws Exception {
    String dlqPayload =
        """
        {
          "_metadata_source": {
            "collection": "orders"
          },
          "_id": "\\\"order_123\\\"",
          "data": {
            "amount": 99.99
          },
          "_metadata_timestamp_seconds": 1683782270,
          "_metadata_timestamp_nanos": 100,
          "isDlqReconsumed": "true",
          "_metadata_retry_count": 3
        }\
        """;
    MongoDbChangeEventContext context =
        new MongoDbChangeEventContext(OBJECT_MAPPER.readTree(dlqPayload), SHADOW_PREFIX);
    MongoDbChangeEventContext decoded = CoderUtils.clone(coder, context);

    assertEquals(context, decoded);
    assertTrue(decoded.getIsDlqReconsumed());
    assertEquals(3, decoded.getRetryCount());
    CoderProperties.coderDecodeEncodeEqual(coder, context);
  }

  @Test
  public void testEncodeDecodeRoundTrip_variousDocumentIdTypes() throws Exception {
    // String doc ID
    String stringIdPayload =
        """
        {
          "_metadata_source": {"collection": "c1"},
          "_id": "\\\"str_id_val\\\"",
          "_metadata_timestamp_seconds": 1000,
          "_metadata_timestamp_nanos": 1,
          "data": {}
        }\
        """;
    MongoDbChangeEventContext strContext =
        new MongoDbChangeEventContext(OBJECT_MAPPER.readTree(stringIdPayload), SHADOW_PREFIX);
    assertEquals("str_id_val", CoderUtils.clone(coder, strContext).getDocumentId());

    // Long doc ID
    String longIdPayload =
        """
        {
          "_metadata_source": {"collection": "c2"},
          "_id": 9223372036854775806,
          "_metadata_timestamp_seconds": 1000,
          "_metadata_timestamp_nanos": 1,
          "data": {}
        }\
        """;
    MongoDbChangeEventContext longContext =
        new MongoDbChangeEventContext(OBJECT_MAPPER.readTree(longIdPayload), SHADOW_PREFIX);
    assertEquals(9223372036854775806L, CoderUtils.clone(coder, longContext).getDocumentId());

    // Integer doc ID
    String intIdPayload =
        """
        {
          "_metadata_source": {"collection": "c3"},
          "_id": 42,
          "_metadata_timestamp_seconds": 1000,
          "_metadata_timestamp_nanos": 1,
          "data": {}
        }\
        """;
    MongoDbChangeEventContext intContext =
        new MongoDbChangeEventContext(OBJECT_MAPPER.readTree(intIdPayload), SHADOW_PREFIX);
    assertEquals(42, CoderUtils.clone(coder, intContext).getDocumentId());

    // Double doc ID
    String doubleIdPayload =
        """
        {
          "_metadata_source": {"collection": "c4"},
          "_id": "123.456",
          "_metadata_timestamp_seconds": 1000,
          "_metadata_timestamp_nanos": 1,
          "data": {}
        }\
        """;
    MongoDbChangeEventContext doubleContext =
        new MongoDbChangeEventContext(OBJECT_MAPPER.readTree(doubleIdPayload), SHADOW_PREFIX);
    assertEquals(123.456, (Double) CoderUtils.clone(coder, doubleContext).getDocumentId(), 0.001);

    // Composite Document (Map) doc ID
    String compositeIdPayload =
        """
        {
          "_metadata_source": {"collection": "c5"},
          "_id": "{\\\"tenant\\\": \\\"acme\\\", \\\"uid\\\": 100}",
          "_metadata_timestamp_seconds": 1000,
          "_metadata_timestamp_nanos": 1,
          "data": {}
        }\
        """;
    MongoDbChangeEventContext compContext =
        new MongoDbChangeEventContext(OBJECT_MAPPER.readTree(compositeIdPayload), SHADOW_PREFIX);
    Object docId = CoderUtils.clone(coder, compContext).getDocumentId();
    assertTrue(docId instanceof Document);
    assertEquals("acme", ((Document) docId).get("tenant"));
    assertEquals(100, ((Document) docId).get("uid"));
  }

  @Test
  public void testNullValueEncoding() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    coder.encode(null, out);
    byte[] bytes = out.toByteArray();

    assertEquals(1, bytes.length);

    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    MongoDbChangeEventContext decoded = coder.decode(in);
    assertNull(decoded);
  }

  @Test
  public void testVerifyDeterministic() throws Exception {
    coder.verifyDeterministic();

    MongoDbChangeEventContext context1 =
        new MongoDbChangeEventContext(insertEventJson, SHADOW_PREFIX);
    MongoDbChangeEventContext context2 =
        new MongoDbChangeEventContext(insertEventJson, SHADOW_PREFIX);
    CoderProperties.coderDeterministic(coder, context1, context2);
  }
}
