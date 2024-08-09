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

import java.util.function.Supplier;
import org.json.JSONObject;

class JsonObjects {

  public static String getStringOrNull(JSONObject object, String property) {
    return getStringOrDefault(object, property, null);
  }

  public static String getStringOrDefault(JSONObject object, String property, String defaultValue) {
    return getOrDefault(object, property, () -> object.getString(property), defaultValue);
  }

  public static boolean getBooleanOrDefault(
      JSONObject object, String property, boolean defaultValue) {
    return getOrDefault(object, property, () -> object.getBoolean(property), defaultValue);
  }

  public static Integer getIntegerOrNull(JSONObject object, String property) {
    return getIntegerOrDefault(object, property, null);
  }

  private static Integer getIntegerOrDefault(
      JSONObject object, String property, Integer defaultValue) {
    return getOrDefault(object, property, () -> object.getInt(property), defaultValue);
  }

  private static <T> T getOrDefault(
      JSONObject object, String property, Supplier<T> valueSupplier, T defaultValue) {
    if (isAbsentOrNull(object, property)) {
      return defaultValue;
    }
    return valueSupplier.get();
  }

  private static boolean isAbsentOrNull(JSONObject object, String property) {
    return !object.has(property) || object.isNull(property);
  }
}
