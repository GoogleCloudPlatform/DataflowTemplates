/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.templates.SpannerTransactionWriterDoFn;
import com.google.cloud.teleport.v2.templates.datastream.InvalidChangeEventException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.json.JSONObject;
import org.junit.Test;

public class SpannerTransactionWriterTest {
  static JSONObject getTestChangeEvent() {
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("first_name", "A");
    changeEvent.put("last_name", "B");
    changeEvent.put("age", 10L);
    changeEvent.put("bool_column", false);
    changeEvent.put("double_column", 1.3D);
    changeEvent.put("byte_column", new byte[] {0x00});
    return changeEvent;
  }

  public static JsonNode parseChangeEvent(String json) {
    try {
      ObjectMapper mapper = new ObjectMapper();
      mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
      return mapper.readTree(json);
    } catch (IOException e) {
      // No action. Return null.
    }
    return null;
  }

  @Test
  public void testConvertJsonNodeToMap() throws InvalidChangeEventException {
    JSONObject changeEvent = getTestChangeEvent();
    JsonNode ce = parseChangeEvent(changeEvent.toString());
    SpannerTransactionWriterDoFn spannerTransactionWriterDoFn =
        new SpannerTransactionWriterDoFn(
            SpannerConfig.create(), null, null, null, "shadow_", "mysql", true, "", "", "");
    Map<String, Object> result = spannerTransactionWriterDoFn.convertJsonNodeToMap(ce);
    Map<String, Object> expectedMap = new HashMap<>();
    expectedMap.put("first_name", "A");
    expectedMap.put("last_name", "B");
    expectedMap.put("age", 10L);
    expectedMap.put("bool_column", false);
    expectedMap.put("double_column", 1.3D);
    expectedMap.put("byte_column", new byte[] {0x00});
    assertEquals(expectedMap.get("first_name"), result.get("first_name"));
    assertEquals(expectedMap.get("age"), result.get("age"));
    assertEquals(expectedMap.get("last_name"), result.get("last_name"));
    assertEquals(expectedMap.get("bool_column"), result.get("bool_column"));
    assertEquals(expectedMap.get("double_column"), result.get("double_column"));
    assertArrayEquals((byte[]) expectedMap.get("byte_column"), (byte[]) result.get("byte_column"));
  }

  @Test(expected = InvalidChangeEventException.class)
  public void testConvertEmptyJsonNodeToMap() throws InvalidChangeEventException {
    JsonNode ce = parseChangeEvent("");
    SpannerTransactionWriterDoFn spannerTransactionWriterDoFn =
        new SpannerTransactionWriterDoFn(
            SpannerConfig.create(), null, null, null, "shadow_", "mysql", true, "", "", "");
    Map<String, Object> result = spannerTransactionWriterDoFn.convertJsonNodeToMap(ce);
  }

  @Test
  public void testTransformChangeEventViaAdvancedTransformation() {
    JSONObject changeEvent = getTestChangeEvent();
    JsonNode ce = parseChangeEvent(changeEvent.toString());

    Map<String, Object> spannerRecord = new HashMap<>();
    spannerRecord.put("first_name", "new_value");
    spannerRecord.put("age", 20);
    spannerRecord.put("bool_column", true);
    spannerRecord.put("double_column", 1.4D);
    spannerRecord.put("byte_column", new byte[] {0x01});
    SpannerTransactionWriterDoFn spannerTransactionWriterDoFn =
        new SpannerTransactionWriterDoFn(
            SpannerConfig.create(), null, null, null, "shadow_", "mysql", true, "", "", "");
    JsonNode result =
        spannerTransactionWriterDoFn.transformChangeEventViaAdvancedTransformation(
            ce, spannerRecord);

    // Verify the result
    JSONObject changeEventNew = new JSONObject();
    changeEventNew.put("first_name", "new_value");
    changeEventNew.put("age", 20);
    changeEventNew.put("last_name", "B");
    changeEventNew.put("bool_column", true);
    changeEventNew.put("double_column", 1.4D);
    changeEventNew.put("byte_column", "AQ=="); // Base64 encoded value for 0x01
    JsonNode expectedEvent = parseChangeEvent(changeEventNew.toString());
    assertEquals(expectedEvent.get("first_name"), result.get("first_name"));
    assertEquals(expectedEvent.get("age"), result.get("age"));
    assertEquals(expectedEvent.get("last_name"), result.get("last_name"));
    assertEquals(expectedEvent.get("bool_column"), result.get("bool_column"));
    assertEquals(
        expectedEvent.get("double_column").asDouble(), result.get("double_column").asDouble(), 0);
    assertEquals(expectedEvent.get("byte_column").asText(), result.get("byte_column").asText());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTransformChangeEventViaAdvancedTransformationUnsupportedType() {
    JSONObject changeEvent = getTestChangeEvent();
    JsonNode ce = parseChangeEvent(changeEvent.toString());

    Map<String, Object> spannerRecord = new HashMap<>();
    Map<String, String> mapEntry = new HashMap<>();
    mapEntry.put("abc", "A");
    spannerRecord.put("first_name", "new_value");
    spannerRecord.put("age", 20);
    spannerRecord.put("object_type", mapEntry);
    SpannerTransactionWriterDoFn spannerTransactionWriterDoFn =
        new SpannerTransactionWriterDoFn(
            SpannerConfig.create(), null, null, null, "shadow_", "mysql", true, "", "", "");
    JsonNode result =
        spannerTransactionWriterDoFn.transformChangeEventViaAdvancedTransformation(
            ce, spannerRecord);
  }
}
