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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.teleport.v2.spanner.exceptions.InvalidTransformationException;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.InvalidChangeEventException;
import com.google.cloud.teleport.v2.spanner.migrations.utils.ChangeEventUtils;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.json.JSONObject;

public class ChangeEventToMapConvertor {
  public static Map<String, Object> convertChangeEventToMap(JsonNode changeEvent)
      throws InvalidChangeEventException {
    Map<String, Object> sourceRecord = new HashMap<>();
    List<String> changeEventColumns = ChangeEventUtils.getEventColumnKeys(changeEvent);
    for (String key : changeEventColumns) {
      JsonNode value = changeEvent.get(key);
      if (value.isObject()) {
        sourceRecord.put(key, convertChangeEventToMap(value));
      } else if (value.isArray()) {
        if (value.size() > 0 && value.get(0).isIntegralNumber()) {
          byte[] byteArray = new byte[value.size()];
          for (int i = 0; i < value.size(); i++) {
            byteArray[i] = (byte) value.get(i).intValue();
          }
          sourceRecord.put(key, byteArray);
        } else {
          throw new InvalidChangeEventException("Invalid byte array value for column: " + key);
        }
      } else if (value.isTextual()) {
        sourceRecord.put(key, value.asText());
      } else if (value.isBoolean()) {
        sourceRecord.put(key, value.asBoolean());
      } else if (value.isDouble() || value.isFloat() || value.isFloatingPointNumber()) {
        sourceRecord.put(key, value.asDouble());
      } else if (value.isLong() || value.isInt() || value.isIntegralNumber()) {
        sourceRecord.put(key, value.asLong());
      } else if (value.isNull()) {
        sourceRecord.put(key, null);
      } else {
        throw new InvalidChangeEventException("Invalid datatype for column: " + key);
      }
    }
    return sourceRecord;
  }

  public static JsonNode transformChangeEventViaCustomTransformation(
      JsonNode changeEvent, Map<String, Object> spannerRecord)
      throws InvalidTransformationException {
    for (Map.Entry<String, Object> entry : spannerRecord.entrySet()) {
      String columnName = entry.getKey();
      Object columnValue = entry.getValue();

      if (columnValue instanceof Boolean) {
        ((ObjectNode) changeEvent).put(columnName, (Boolean) columnValue);
      } else if (columnValue instanceof Long) {
        ((ObjectNode) changeEvent).put(columnName, (Long) columnValue);
      } else if (columnValue instanceof byte[]) {
        ((ObjectNode) changeEvent).put(columnName, (byte[]) columnValue);
      } else if (columnValue instanceof Double) {
        ((ObjectNode) changeEvent).put(columnName, (Double) columnValue);
      } else if (columnValue instanceof Integer) {
        ((ObjectNode) changeEvent).put(columnName, (Integer) columnValue);
      } else if (columnValue instanceof String) {
        ((ObjectNode) changeEvent).put(columnName, (String) columnValue);
      } else {
        throw new InvalidTransformationException(
            "Column name(" + columnName + ") has unsupported column value(" + columnValue + ")");
      }
    }
    return changeEvent;
  }

  public static Map<String, Object> combineJsonObjects(
      JSONObject keysJson, JSONObject newValuesJson) {
    Map<String, Object> combinedMap = new HashMap<>();
    addJsonToMap(newValuesJson, combinedMap);
    addJsonToMap(keysJson, combinedMap);
    return combinedMap;
  }

  private static void addJsonToMap(JSONObject jsonObject, Map<String, Object> map) {
    Iterator<String> keys = jsonObject.keys();
    while (keys.hasNext()) {
      String key = keys.next();
      map.put(key, jsonObject.get(key));
    }
  }

  public static void updateJsonWithMap(
      Map<String, Object> map, JSONObject keysJson, JSONObject newValuesJson) {
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();

      if (keysJson.has(key)) {
        keysJson.put(key, value);
      }

      if (newValuesJson.has(key)) {
        newValuesJson.put(key, value);
      }
    }
  }
}
