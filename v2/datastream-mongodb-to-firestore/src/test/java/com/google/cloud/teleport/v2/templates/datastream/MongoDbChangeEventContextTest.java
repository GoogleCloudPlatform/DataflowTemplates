/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.templates.datastream;

import static com.google.cloud.teleport.v2.templates.datastream.MongoDbChangeEventContext.DOC_ID_COL;
import static com.google.cloud.teleport.v2.templates.datastream.MongoDbChangeEventContext.SHADOW_DOC_ID_COL;
import static com.google.cloud.teleport.v2.templates.datastream.MongoDbChangeEventContext.TIMESTAMP_COL;
import static com.google.cloud.teleport.v2.templates.datastream.MongoDbChangeEventContext.TIMESTAMP_NANOS_COL;
import static com.google.cloud.teleport.v2.templates.datastream.MongoDbChangeEventContext.TIMESTAMP_SECONDS_COL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.transforms.Utils;
import com.google.common.collect.ImmutableMap;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link MongoDbChangeEventContext}. */
@RunWith(JUnit4.class)
public class MongoDbChangeEventContextTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String SHADOW_PREFIX = "shadow_";

  private JsonNode insertEvent;
  private JsonNode deleteEvent;
  private JsonNode updateEvent;
  private JsonNode invalidEventMissingMetadata;
  private JsonNode invalidEventMissingId;
  private JsonNode invalidEventUnsupportedIdType;
  private JsonNode invalidEventMissingTimestamp;

  @Before
  public void setUp() throws JsonProcessingException {

    insertEvent =
        OBJECT_MAPPER.readTree(
            """
                {
                  "_metadata_source": {
                    "collection": "test_collection"
                  },
                  "_id": "{\\\"$oid\\\": \\\"645c9a7e7b8b1a0e9c0f8b3a\\\"}",
                  "data": {
                    "field1": "value1",
                    "field2": 123
                  },
                  "_metadata_timestamp_seconds": 1683782270,
                  "_metadata_timestamp_nanos": 123456789,
                  "op": "i"
                }""");

    deleteEvent =
        OBJECT_MAPPER.readTree(
            """
                {
                  "_metadata_source": {
                    "collection": "test_collection"
                  },
                  "_id": "{\\\"$oid\\\": \\\"645c9a7e7b8b1a0e9c0f8b3a\\\"}",
                  "_metadata_timestamp_seconds": 1683782270,
                  "_metadata_timestamp_nanos": 123456789,
                  "op": "d",
                  "_metadata_change_type": "DELETE"
                }""");

    updateEvent =
        OBJECT_MAPPER.readTree(
            """
                {
                  "_metadata_source": {
                    "collection": "test_collection"
                  },
                  "_id": "{\\\"$oid\\\": \\\"645c9a7e7b8b1a0e9c0f8b3a\\\"}",
                  "data": {
                    "field1": "updated_value",
                    "field2": 456
                  },
                  "_metadata_timestamp_seconds": 1683782270,
                  "_metadata_timestamp_nanos": 123456789,
                  "op": "u"
                }""");

    invalidEventMissingMetadata = OBJECT_MAPPER.readTree("{}");

    invalidEventMissingId =
        OBJECT_MAPPER.readTree(
            """
                {
                  "_metadata_source": {
                    "collection": "test_collection"
                  },
                  "_metadata_timestamp_seconds": 1683782270,
                  "_metadata_timestamp_nanos": 123456789
                }""");

    invalidEventUnsupportedIdType =
        OBJECT_MAPPER.readTree(
            """
                {
                  "_metadata_source": {
                    "collection": "test_collection"
                  },
                  "_id": "[1, 2, 3]",
                  "_metadata_timestamp_seconds": 1683782270,
                  "_metadata_timestamp_nanos": 123456789
                }""");

    invalidEventMissingTimestamp =
        OBJECT_MAPPER.readTree(
            """
                {
                  "_metadata_source": {
                    "collection": "test_collection"
                  },
                  "_id": "{\\\"$oid\\\": \\\"645c9a7e7b8b1a0e9c0f8b3a\\\"}"
                }""");
  }

  @Test
  public void testConstructorInsertEvent() throws JsonProcessingException {
    MongoDbChangeEventContext context = new MongoDbChangeEventContext(insertEvent, SHADOW_PREFIX);
    assertEquals("test_collection", context.getDataCollection());
    assertEquals("shadow_test_collection", context.getShadowCollection());
    assertNotNull(context.getDocumentId());
    assertFalse(context.isDeleteEvent());
    assertNotNull(context.getTimestampDoc());
    assertNotNull(context.getDataAsJsonString());
    assertNotNull(context.getShadowDocument());

    Document expectedTimestamp =
        new Document(
            ImmutableMap.of(TIMESTAMP_SECONDS_COL, 1683782270L, TIMESTAMP_NANOS_COL, 123456789));
    assertEquals(expectedTimestamp, context.getTimestampDoc());
    assertTrue(
        Utils.jsonToDocument(context.getDataAsJsonString(), context.getDocumentId())
            .containsKey(DOC_ID_COL));
  }

  @Test
  public void testConstructorDeleteEvent() throws JsonProcessingException {
    MongoDbChangeEventContext context = new MongoDbChangeEventContext(deleteEvent, SHADOW_PREFIX);
    assertEquals("test_collection", context.getDataCollection());
    assertEquals("shadow_test_collection", context.getShadowCollection());
    assertNotNull(context.getDocumentId());
    assertTrue(context.isDeleteEvent());
    assertNotNull(context.getTimestampDoc());
    assertNull(context.getDataAsJsonString());
    assertNotNull(context.getShadowDocument());

    Document expectedTimestamp =
        new Document(
            ImmutableMap.of(TIMESTAMP_SECONDS_COL, 1683782270L, TIMESTAMP_NANOS_COL, 123456789));
    assertEquals(expectedTimestamp, context.getTimestampDoc());
  }

  @Test
  public void testConstructorUpdateEvent() throws JsonProcessingException {
    MongoDbChangeEventContext context = new MongoDbChangeEventContext(updateEvent, SHADOW_PREFIX);
    assertEquals("test_collection", context.getDataCollection());
    assertEquals("shadow_test_collection", context.getShadowCollection());
    assertNotNull(context.getDocumentId());
    assertFalse(context.isDeleteEvent());
    assertNotNull(context.getTimestampDoc());
    assertNotNull(context.getDataAsJsonString());
    assertNotNull(context.getShadowDocument());

    Document expectedTimestamp =
        new Document(
            ImmutableMap.of(TIMESTAMP_SECONDS_COL, 1683782270L, TIMESTAMP_NANOS_COL, 123456789));
    assertEquals(expectedTimestamp, context.getTimestampDoc());
    assertTrue(
        Utils.jsonToDocument(context.getDataAsJsonString(), context.getDocumentId())
            .containsKey(DOC_ID_COL));
  }

  @Test(expected = IllegalStateException.class)
  public void testConstructorInvalidEventMissingMetadata() throws JsonProcessingException {
    new MongoDbChangeEventContext(invalidEventMissingMetadata, SHADOW_PREFIX);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructorInvalidEventMissingId() throws JsonProcessingException {
    new MongoDbChangeEventContext(invalidEventMissingId, SHADOW_PREFIX);
  }

  @Test
  public void testConstructorObjectId() throws JsonProcessingException {
    JsonNode eventWithObjectId =
        OBJECT_MAPPER.readTree(
            """
                {
                  "_metadata_source": {
                    "collection": "test_collection"
                  },
                  "_id": "{ \\\"$oid\\\": \\\"645c9a7e7b8b1a0e9c0f8b3a\\\"}",
                  "data": {
                    "field1": "updated_value",
                    "field2": 456
                  },
                  "_metadata_timestamp_seconds": 1683782270,
                  "_metadata_timestamp_nanos": 123456789
                }""");
    MongoDbChangeEventContext context =
        new MongoDbChangeEventContext(eventWithObjectId, SHADOW_PREFIX);
    assertNotNull(context.getDocumentId());
    assertTrue(context.getDocumentId() instanceof ObjectId);
    assertEquals("645c9a7e7b8b1a0e9c0f8b3a", context.getDocumentId().toString());
  }

  @Test
  public void testConstructorLongId() throws JsonProcessingException {
    JsonNode eventWithLongId =
        OBJECT_MAPPER.readTree(
            """
                {
                  "_metadata_source": {
                    "collection": "test_collection"
                  },
                  "_id": 9223372036854775806,
                  "data": {
                    "field1": "updated_value",
                    "field2": 456
                  },
                  "_metadata_timestamp_seconds": 1683782270,
                  "_metadata_timestamp_nanos": 123456789
                }""");
    MongoDbChangeEventContext context =
        new MongoDbChangeEventContext(eventWithLongId, SHADOW_PREFIX);
    assertNotNull(context.getDocumentId());
    assertTrue(context.getDocumentId() instanceof Long);
    assertEquals(9223372036854775806L, context.getDocumentId());
  }

  @Test
  public void testConstructorStringId() throws JsonProcessingException {
    JsonNode eventWithStringId =
        OBJECT_MAPPER.readTree(
            """
                {
                  "_metadata_source": {
                    "collection": "test_collection"
                  },
                  "_id": "\\\"test_id\\\"",
                  "data": {
                    "field1": "updated_value",
                    "field2": 456
                  },
                  "_metadata_timestamp_seconds": 1683782270,
                  "_metadata_timestamp_nanos": 123456789
                }""");
    MongoDbChangeEventContext context =
        new MongoDbChangeEventContext(eventWithStringId, SHADOW_PREFIX);
    assertNotNull(context.getDocumentId());
    assertTrue(context.getDocumentId() instanceof String);
    assertEquals("test_id", context.getDocumentId());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructorInvalidEventUnsupportedIdType() throws JsonProcessingException {
    new MongoDbChangeEventContext(invalidEventUnsupportedIdType, SHADOW_PREFIX);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructorInvalidEventMissingTimestamp() throws JsonProcessingException {
    new MongoDbChangeEventContext(invalidEventMissingTimestamp, SHADOW_PREFIX);
  }

  @Test
  public void testGenerateShadowDocument() throws JsonProcessingException {
    MongoDbChangeEventContext context = new MongoDbChangeEventContext(insertEvent, SHADOW_PREFIX);
    Document shadowDocument = context.generateShadowDocument();
    assertNotNull(shadowDocument);
    assertTrue(shadowDocument.containsKey(SHADOW_DOC_ID_COL));
    assertTrue(shadowDocument.containsKey(TIMESTAMP_COL));
    assertTrue(shadowDocument.containsKey("processed_at"));
    assertFalse(shadowDocument.containsKey("_metadata_source"));
    assertTrue(shadowDocument.containsKey("_id"));
    assertFalse(shadowDocument.containsKey("data"));
    assertFalse(shadowDocument.containsKey("_metadata_timestamp_seconds"));
    assertFalse(shadowDocument.containsKey("_metadata_timestamp_nanos"));
    assertTrue(shadowDocument.containsKey("op"));
  }

  @Test
  public void testGenerateDataDocumentInsert() throws JsonProcessingException {
    MongoDbChangeEventContext context = new MongoDbChangeEventContext(insertEvent, SHADOW_PREFIX);
    String dataString = context.dataAsJsonString();
    Document dataDocument = Utils.jsonToDocument(dataString, context.getDocumentId());
    assertNotNull(dataDocument);
    assertTrue(dataDocument.containsKey(DOC_ID_COL));
    assertTrue(dataDocument.containsKey("field1"));
    assertTrue(dataDocument.containsKey("field2"));
  }

  @Test
  public void testGenerateDataDocumentDelete() throws JsonProcessingException {
    MongoDbChangeEventContext context = new MongoDbChangeEventContext(deleteEvent, SHADOW_PREFIX);
    String dataString = context.dataAsJsonString();
    assertNull(dataString);
  }

  @Test
  public void testGetChangeEvent() throws JsonProcessingException {
    MongoDbChangeEventContext context = new MongoDbChangeEventContext(insertEvent, SHADOW_PREFIX);
    assertEquals(insertEvent, context.getChangeEvent());
  }

  @Test
  public void testGetDataCollection() throws JsonProcessingException {
    MongoDbChangeEventContext context = new MongoDbChangeEventContext(insertEvent, SHADOW_PREFIX);
    assertEquals("test_collection", context.getDataCollection());
  }

  @Test
  public void testGetShadowCollection() throws JsonProcessingException {
    MongoDbChangeEventContext context = new MongoDbChangeEventContext(insertEvent, SHADOW_PREFIX);
    assertEquals("shadow_test_collection", context.getShadowCollection());
  }

  @Test
  public void testGetDocumentId() throws JsonProcessingException {
    MongoDbChangeEventContext context = new MongoDbChangeEventContext(insertEvent, SHADOW_PREFIX);
    assertNotNull(context.getDocumentId());
  }

  @Test
  public void testIsDeleteEvent() throws JsonProcessingException {
    MongoDbChangeEventContext insertContext =
        new MongoDbChangeEventContext(insertEvent, SHADOW_PREFIX);
    assertFalse(insertContext.isDeleteEvent());

    MongoDbChangeEventContext deleteContext =
        new MongoDbChangeEventContext(deleteEvent, SHADOW_PREFIX);
    assertTrue(deleteContext.isDeleteEvent());
  }

  @Test
  public void testGetShadowDocument() throws JsonProcessingException {
    MongoDbChangeEventContext context = new MongoDbChangeEventContext(insertEvent, SHADOW_PREFIX);
    assertNotNull(context.getShadowDocument());
  }

  @Test
  public void testGetDataDocumentInsertEvent() throws JsonProcessingException {
    MongoDbChangeEventContext context = new MongoDbChangeEventContext(insertEvent, SHADOW_PREFIX);
    assertNotNull(context.getDataAsJsonString());
  }

  @Test
  public void testGetDataDocumentDeleteEvent() throws JsonProcessingException {
    MongoDbChangeEventContext context = new MongoDbChangeEventContext(deleteEvent, SHADOW_PREFIX);
    assertNull(context.getDataAsJsonString());
  }

  @Test
  public void testGetTimestampDoc() throws JsonProcessingException {
    MongoDbChangeEventContext context = new MongoDbChangeEventContext(insertEvent, SHADOW_PREFIX);
    assertNotNull(context.getTimestampDoc());
    assertEquals(1683782270L, context.getTimestampDoc().getLong(TIMESTAMP_SECONDS_COL).longValue());
    assertEquals(123456789, context.getTimestampDoc().getInteger(TIMESTAMP_NANOS_COL).intValue());
  }

  @Test
  public void testToString() throws JsonProcessingException {
    MongoDbChangeEventContext context = new MongoDbChangeEventContext(insertEvent, SHADOW_PREFIX);
    String jsonString = context.toString();

    String expectedJson =
        "{\"changeEvent\":{\"_metadata_source\":{\"collection\":\"test_collection\"},\"_id\":\"{\\\"$oid\\\": \\\"645c9a7e7b8b1a0e9c0f8b3a\\\"}\",\"data\":{\"field1\":\"value1\",\"field2\":123},\"_metadata_timestamp_seconds\":1683782270,\"_metadata_timestamp_nanos\":123456789,\"op\":\"i\"},"
            + "\"dataCollection\":\"test_collection\","
            + "\"shadowCollection\":\"shadow_test_collection\","
            + "\"documentId\":{\"$oid\":\"645c9a7e7b8b1a0e9c0f8b3a\"},"
            + "\"isDeleteEvent\":false,"
            + "\"timestamp\":{\"seconds\":1683782270,\"nanos\":123456789},"
            + "\"isDlqReconsumed\":false,"
            + "\"_metadata_retry_count\":0}";

    assertEquals(jsonString, expectedJson);
  }
}
