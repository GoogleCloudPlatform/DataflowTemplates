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
package com.google.cloud.teleport.v2.transforms;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.templates.datastream.MongoDbChangeEventContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link MongoDbEventDeadLetterQueueSanitizer}. */
@RunWith(JUnit4.class)
public class MongoDbEventDeadLetterQueueSanitizerTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private MongoDbEventDeadLetterQueueSanitizer sanitizer;
  private MongoDbChangeEventContext mockContext;
  private JsonNode mockChangeEvent;

  @Before
  public void setUp() throws Exception {
    sanitizer = new MongoDbEventDeadLetterQueueSanitizer();
    mockContext = mock(MongoDbChangeEventContext.class);
    mockChangeEvent = OBJECT_MAPPER.readTree("{\"key\": \"value\"}");

    when(mockContext.getChangeEvent()).thenReturn(mockChangeEvent);
    when(mockContext.getDataCollection()).thenReturn("test_collection");
    when(mockContext.getShadowCollection()).thenReturn("shadow_test_collection");
    when(mockContext.getDocumentId()).thenReturn("test_id");
    when(mockContext.isDeleteEvent()).thenReturn(false);
  }

  @Test
  public void testGetJsonMessageSuccess() throws Exception {
    String expectedJson =
        "{\"changeEvent\":{\"key\":\"value\"},"
            + "\"dataCollection\":\"test_collection\","
            + "\"shadowCollection\":\"shadow_test_collection\","
            + "\"documentId\":\"test_id\","
            + "\"isDeleteEvent\":false}";
    assertEquals(expectedJson, sanitizer.getJsonMessage(mockContext));
  }

  @Test
  public void testGetJsonMessageEmptyChangeEvent() throws Exception {
    // Simulate JsonProcessingException by making getChangeEvent return null
    when(mockContext.getChangeEvent()).thenReturn(null);

    String expectedJson =
        "{\"changeEvent\":null,"
            + "\"dataCollection\":\"test_collection\","
            + "\"shadowCollection\":\"shadow_test_collection\","
            + "\"documentId\":\"test_id\","
            + "\"isDeleteEvent\":false}";
    assertEquals(expectedJson, sanitizer.getJsonMessage(mockContext));
  }

  @Test
  public void testGetErrorMessageJsonSuccess() throws Exception {
    String expectedJson =
        "{\"errorType\":\"MongoDbChangeEventContext processing error\","
            + "\"documentId\":\"test_id\","
            + "\"collection\":\"test_collection\"}";
    assertEquals(expectedJson, sanitizer.getErrorMessageJson(mockContext));
  }

  @Test
  public void testGetErrorMessageJsonJsonProcessingException() throws Exception {
    // Simulate JsonProcessingException by making getDataCollection return null
    when(mockContext.getDataCollection()).thenReturn(null);

    assertEquals(
        "{\"errorType\":\"MongoDbChangeEventContext processing error\",\"documentId\":\"test_id\",\"collection\":null}",
        sanitizer.getErrorMessageJson(mockContext));
  }

  @Test
  public void testGetJsonMessageWithComplexChangeEvent() throws Exception {
    JsonNode complexChangeEvent =
        OBJECT_MAPPER.readTree(
            "{\"_id\": {\"$oid\": \"645c9a7e7b8b1a0e9c0f8b3a\"}, \"data\": {\"field1\": \"value1\", \"field2\": 123}}");
    when(mockContext.getChangeEvent()).thenReturn(complexChangeEvent);

    String expectedJson =
        "{\"changeEvent\":{\"_id\":{\"$oid\":\"645c9a7e7b8b1a0e9c0f8b3a\"},\"data\":{\"field1\":\"value1\",\"field2\":123}},"
            + "\"dataCollection\":\"test_collection\","
            + "\"shadowCollection\":\"shadow_test_collection\","
            + "\"documentId\":\"test_id\","
            + "\"isDeleteEvent\":false}";
    assertEquals(expectedJson, sanitizer.getJsonMessage(mockContext));
  }

  @Test
  public void testGetErrorMessageJsonWithDifferentValues() throws Exception {
    when(mockContext.getDocumentId()).thenReturn(123L);
    when(mockContext.getDataCollection()).thenReturn("another_collection");

    String expectedJson =
        "{\"errorType\":\"MongoDbChangeEventContext processing error\","
            + "\"documentId\":123,"
            + "\"collection\":\"another_collection\"}";
    assertEquals(expectedJson, sanitizer.getErrorMessageJson(mockContext));
  }
}
