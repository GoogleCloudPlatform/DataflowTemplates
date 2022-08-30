/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.v2.utils;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type.Code;
import com.google.cloud.spanner.Type.StructField;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.util.List;

/** Utilities for Spanner templates. */
public final class SpannerUtils {

  private SpannerUtils() {}

  /**
   * Converts a Spanner Struct to a Json Object .
   *
   * @param struct The {@link Struct} object to convert.
   * @return JsonObject representation of Spanner Struct.
   */
  public static JsonObject convertStructToJson(Struct struct) {
    JsonObject jsonObject = new JsonObject();
    List<StructField> structFields = struct.getType().getStructFields();

    for (StructField field : structFields) {
      switch (field.getType().getCode()) {
        case BOOL:
          if (!struct.getValue(field.getName()).isNull()) {
            jsonObject.addProperty(field.getName(), struct.getBoolean(field.getName()));
          }
          break;
        case INT64:
          if (!struct.getValue(field.getName()).isNull()) {
            jsonObject.addProperty(field.getName(), struct.getLong(field.getName()));
          }
          break;
        case FLOAT64:
          if (!struct.getValue(field.getName()).isNull()) {
            jsonObject.addProperty(field.getName(), struct.getDouble(field.getName()));
          }
          break;
        case STRING:
        case PG_NUMERIC:
          if (!struct.getValue(field.getName()).isNull()) {
            jsonObject.addProperty(field.getName(), struct.getString(field.getName()));
          }
          break;
        case BYTES:
          if (!struct.getValue(field.getName()).isNull()) {
            jsonObject.addProperty(
                field.getName(), struct.getBytes(field.getName()).toStringUtf8());
          }
          break;
        case DATE:
          if (!struct.getValue(field.getName()).isNull()) {
            jsonObject.addProperty(field.getName(), struct.getDate(field.getName()).toString());
          }
          break;
        case TIMESTAMP:
          if (!struct.getValue(field.getName()).isNull()) {
            jsonObject.addProperty(
                field.getName(), struct.getTimestamp(field.getName()).toString());
          }
          break;
        case ARRAY:
          if (!struct.getValue(field.getName()).isNull()) {
            jsonObject.add(field.getName(), convertArrayToJsonArray(struct, field.getName()));
          }
          break;
        case STRUCT:
          if (!struct.getValue(field.getName()).isNull()) {
            jsonObject.add(field.getName(), convertStructToJson(struct.getStruct(field.getName())));
          }
          break;
        default:
          throw new RuntimeException("Unsupported type: " + field.getType());
      }
    }
    return jsonObject;
  }

  /**
   * Converts an array from a struct to a json array .
   *
   * @param struct The {@link Struct} object to get array from.
   * @param columnName The name of column where the array is.
   * @return JSONArray representation of the {@link Struct} array.
   */
  private static JsonArray convertArrayToJsonArray(Struct struct, String columnName) {
    Code code = struct.getColumnType(columnName).getArrayElementType().getCode();
    JsonArray jsonArray = new JsonArray();
    switch (code) {
      case BOOL:
        struct.getBooleanList(columnName).forEach(jsonArray::add);
        break;
      case INT64:
        struct.getLongList(columnName).forEach(jsonArray::add);
        break;
      case FLOAT64:
        struct.getDoubleList(columnName).forEach(jsonArray::add);
        break;
      case STRING:
      case PG_NUMERIC:
        struct.getStringList(columnName).forEach(jsonArray::add);
        break;
      case BYTES:
        struct.getBytesList(columnName).stream()
            .map(ByteArray::toStringUtf8)
            .forEach(jsonArray::add);
        break;
      case DATE:
        struct.getDateList(columnName).stream().map(Date::toString).forEach(jsonArray::add);
        break;
      case TIMESTAMP:
        struct.getTimestampList(columnName).stream()
            .map(Timestamp::toString)
            .forEach(jsonArray::add);
        break;
      case STRUCT:
        struct.getStructList(columnName).stream()
            .map(SpannerUtils::convertStructToJson)
            .forEach(jsonArray::add);
        break;
      default:
        throw new RuntimeException("Unsupported type: " + code);
    }
    return jsonArray;
  }
}
