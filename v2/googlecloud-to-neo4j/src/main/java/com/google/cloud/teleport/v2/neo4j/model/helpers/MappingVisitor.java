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
package com.google.cloud.teleport.v2.neo4j.model.helpers;

import org.json.JSONArray;
import org.json.JSONObject;

class MappingVisitor {
  public static void visit(Object mappings, MappingListener listener) {
    if (mappings instanceof JSONArray) {
      var array = (JSONArray) mappings;
      visitMappingArray(array, listener);
    } else if (mappings instanceof JSONObject) {
      var object = (JSONObject) mappings;
      visitMappingObject(object, listener);
    } else if (mappings instanceof String) {
      String string = (String) mappings;
      visitMappingString(string, listener);
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported type for mappings, expected array, object or string, got: %s",
              mappings.getClass()));
    }
  }

  private static void visitMappingArray(JSONArray allMappings, MappingListener listener) {
    listener.enterArray();
    int length = allMappings.length();
    for (int i = 0; i < length; i++) {
      Object mappings = allMappings.get(i);
      if (mappings instanceof JSONObject) {
        JSONObject object = (JSONObject) mappings;
        visitMappingObject(object, listener);
      } else if (mappings instanceof String) {
        visitMappingString((String) mappings, listener);
      } else {
        throw new IllegalArgumentException(
            String.format(
                "Unsupported type for mapping, expected object or string, got: %s",
                mappings.getClass()));
      }
    }
    listener.exitArray();
  }

  private static void visitMappingObject(JSONObject mappings, MappingListener listener) {
    listener.enterObject();
    mappings
        .keys()
        .forEachRemaining(
            (field) -> {
              listener.enterObjectEntry(field, mappings.getString(field));
            });
    listener.exitObject();
  }

  private static void visitMappingString(String property, MappingListener listener) {
    listener.enterString(property);
    listener.exitString();
  }
}
