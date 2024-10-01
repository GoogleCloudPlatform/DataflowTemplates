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
package com.google.cloud.teleport.v2.spanner.migrations.convertors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.spanner.exceptions.InvalidTransformationException;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.InvalidChangeEventException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.json.JSONObject;
import org.junit.Test;

public class ChangeEventToMapConvertorTest {

  static JSONObject getTestChangeEvent() {
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("first_name", "A");
    changeEvent.put("last_name", "B");
    changeEvent.put("age", 10L);
    changeEvent.put("bool_column", false);
    changeEvent.put("double_column", 1.3D);
    changeEvent.put("byte_column", new byte[] {0x00});
    JSONObject nestedObject = new JSONObject();
    nestedObject.put("nested_key", "nested_value");
    changeEvent.put("nested_object", nestedObject);
    changeEvent.put("null_column", JSONObject.NULL);
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
    Map<String, Object> result = ChangeEventToMapConvertor.convertChangeEventToMap(ce);
    Map<String, Object> expectedMap = new HashMap<>();
    expectedMap.put("first_name", "A");
    expectedMap.put("last_name", "B");
    expectedMap.put("age", 10L);
    expectedMap.put("bool_column", false);
    expectedMap.put("double_column", 1.3D);
    expectedMap.put("byte_column", new byte[] {0x00});
    Map<String, Object> expectedNestedMap = new HashMap<>();
    expectedNestedMap.put("nested_key", "nested_value");
    expectedMap.put("nested_object", expectedNestedMap);
    expectedMap.put("null_column", null);
    assertEquals(expectedMap.get("first_name"), result.get("first_name"));
    assertEquals(expectedMap.get("age"), result.get("age"));
    assertEquals(expectedMap.get("last_name"), result.get("last_name"));
    assertEquals(expectedMap.get("bool_column"), result.get("bool_column"));
    assertEquals(expectedMap.get("double_column"), result.get("double_column"));
    assertArrayEquals((byte[]) expectedMap.get("byte_column"), (byte[]) result.get("byte_column"));
    assertEquals(expectedNestedMap, result.get("nested_object"));
    assertEquals(expectedMap.get("null_column"), result.get("null_column"));
  }

  @Test(expected = InvalidChangeEventException.class)
  public void testConvertEmptyJsonNodeToMap() throws InvalidChangeEventException {
    JsonNode ce = parseChangeEvent("");
    Map<String, Object> result = ChangeEventToMapConvertor.convertChangeEventToMap(ce);
  }

  @Test
  public void testTransformChangeEventViaCustomTransformation()
      throws InvalidTransformationException {
    JSONObject changeEvent = getTestChangeEvent();
    JsonNode ce = parseChangeEvent(changeEvent.toString());

    Map<String, Object> spannerRecord = new HashMap<>();
    spannerRecord.put("first_name", "new_value");
    spannerRecord.put("age", 20);
    spannerRecord.put("bool_column", true);
    spannerRecord.put("double_column", 1.4D);
    spannerRecord.put("byte_column", new byte[] {0x01});
    JsonNode result =
        ChangeEventToMapConvertor.transformChangeEventViaCustomTransformation(ce, spannerRecord);

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

  @Test(expected = InvalidTransformationException.class)
  public void testTransformChangeEventViaCustomTransformationUnsupportedType()
      throws InvalidTransformationException {
    JSONObject changeEvent = getTestChangeEvent();
    JsonNode ce = parseChangeEvent(changeEvent.toString());

    Map<String, Object> spannerRecord = new HashMap<>();
    Map<String, String> mapEntry = new HashMap<>();
    mapEntry.put("abc", "A");
    spannerRecord.put("first_name", "new_value");
    spannerRecord.put("age", 20);
    spannerRecord.put("object_type", mapEntry);
    JsonNode result =
        ChangeEventToMapConvertor.transformChangeEventViaCustomTransformation(ce, spannerRecord);
  }

  @Test
  public void testCombineJsonObjects() {
    JSONObject keysJson = new JSONObject();
    keysJson.put("key1", "value1");

    JSONObject newValuesJson = new JSONObject();
    newValuesJson.put("key2", "value2");
    newValuesJson.put("key3", "value3");

    Map<String, Object> combinedMap =
        ChangeEventToMapConvertor.combineJsonObjects(keysJson, newValuesJson);

    assertEquals(3, combinedMap.size());
    assertEquals("value1", combinedMap.get("key1"));
    assertEquals("value2", combinedMap.get("key2"));
    assertEquals("value3", combinedMap.get("key3"));
  }

  @Test
  public void testUpdateJsonWithMap() {
    JSONObject keysJson = new JSONObject();
    keysJson.put("key1", "oldValue1");
    keysJson.put("key2", "oldValue2");
    keysJson.put("key4", "oldValue4");

    JSONObject newValuesJson = new JSONObject();
    newValuesJson.put("key2", "oldValue2");
    newValuesJson.put("key3", "oldValue3");

    Map<String, Object> map =
        Map.of(
            "key1", "newValue1",
            "key2", "newValue2",
            "key3", "newValue3");

    ChangeEventToMapConvertor.updateJsonWithMap(map, keysJson, newValuesJson);

    assertEquals("newValue1", keysJson.getString("key1"));
    assertEquals("newValue2", keysJson.getString("key2"));
    assertEquals("oldValue4", keysJson.getString("key4"));

    assertEquals("newValue2", newValuesJson.getString("key2"));
    assertEquals("newValue3", newValuesJson.getString("key3"));
  }
}
