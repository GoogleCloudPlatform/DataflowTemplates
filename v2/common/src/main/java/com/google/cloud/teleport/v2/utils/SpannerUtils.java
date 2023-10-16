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
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.teleport.v2.values.SpannerSchema;
import com.google.common.base.Strings;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.List;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.util.StreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility methods for Spanner. */
public class SpannerUtils {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerUtils.class);

  /**
   * Converts a Spanner Struct to a Json Object.
   *
   * @param struct The {@link Struct} object to convert.
   * @return JsonObject representation of Spanner Struct.
   */
  public static JsonObject convertStructToJson(Struct struct) {
    JsonObject jsonObject = new JsonObject();
    List<Type.StructField> structFields = struct.getType().getStructFields();

    for (Type.StructField field : structFields) {
      switch (field.getType().getCode()) {
        case BOOL:
          jsonObject.addProperty(field.getName(), struct.getBoolean(field.getName()));
          break;
        case INT64:
          jsonObject.addProperty(field.getName(), struct.getLong(field.getName()));
          break;
        case FLOAT64:
          jsonObject.addProperty(field.getName(), struct.getDouble(field.getName()));
          break;
        case STRING:
        case PG_NUMERIC:
          jsonObject.addProperty(field.getName(), struct.getString(field.getName()));
          break;
        case BYTES:
          jsonObject.addProperty(field.getName(), struct.getBytes(field.getName()).toStringUtf8());
          break;
        case DATE:
          jsonObject.addProperty(field.getName(), struct.getDate(field.getName()).toString());
          break;
        case TIMESTAMP:
          jsonObject.addProperty(field.getName(), struct.getTimestamp(field.getName()).toString());
          break;
        case ARRAY:
          jsonObject.add(field.getName(), convertArrayToJsonArray(struct, field.getName()));
          break;
        case STRUCT:
          jsonObject.add(field.getName(), convertStructToJson(struct.getStruct(field.getName())));
          break;
        default:
          throw new RuntimeException("Unsupported type: " + field.getType());
      }
    }
    return jsonObject;
  }

  /**
   * Converts an array from a struct to a json array.
   *
   * @param struct The {@link Struct} object to get array from.
   * @param columnName The name of column where the array is.
   * @return JSONArray representation of the {@link Struct} array.
   */
  private static JsonArray convertArrayToJsonArray(Struct struct, String columnName) {
    Type.Code code = struct.getColumnType(columnName).getArrayElementType().getCode();
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

  /**
   * Helper method to read schema file and convert content to SpannerSchema object.
   *
   * @param schemaFilename GCS location of the schema file
   * @return parsed SpannerSchema object
   */
  public static SpannerSchema getSpannerSchemaFromFile(ValueProvider<String> schemaFilename) {
    if (schemaFilename == null) {
      throw new RuntimeException("No schema file provided!");
    }
    try {
      ReadableByteChannel readableByteChannel =
          FileSystems.open(FileSystems.matchNewResource(schemaFilename.get(), false));
      String schemaString =
          new String(
              StreamUtils.getBytesWithoutClosing(Channels.newInputStream(readableByteChannel)));

      SpannerSchema spannerSchema = new SpannerSchema(schemaString);

      return spannerSchema;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /*
   * Query Cloud Spanner to get the table's schema
   */
  public static SpannerSchema getSpannerSchemaFromSpanner(
      String projectId,
      String spannerInstanceName,
      String spannerDatabaseName,
      String spannerTableName) {
    String schemaQuery =
        "SELECT column_name,spanner_type FROM "
            + "information_schema.columns WHERE table_catalog = '' AND table_schema = '' "
            + "AND table_name = @tableName";

    SpannerSchema spannerSchema = new SpannerSchema();
    SpannerOptions options = SpannerOptions.newBuilder().setProjectId(projectId).build();
    Spanner spanner = options.getService();

    try {
      DatabaseId db =
          DatabaseId.of(options.getProjectId(), spannerInstanceName, spannerDatabaseName);
      DatabaseClient dbClient = spanner.getDatabaseClient(db);
      ResultSet resultSet =
          dbClient
              .singleUse()
              .executeQuery(
                  Statement.newBuilder(schemaQuery).bind("tableName").to(spannerTableName).build());
      if (resultSet == null) {
        throw new RuntimeException("Could not get result of Cloud Spanner table schema!");
      }
      while (resultSet.next()) {
        String columnName = resultSet.getString(0);
        String columnType = resultSet.getString(1);

        if (Strings.isNullOrEmpty(columnName) || Strings.isNullOrEmpty(columnType)) {
          throw new RuntimeException("Could not find valid column name or type. They are null.");
        }
        spannerSchema.addEntry(columnName, SpannerSchema.parseSpannerDataType(columnType));
      }
    } finally {
      spanner.close();
    }

    LOG.info("[GetSpannerSchema] Closed Cloud Spanner DB client");
    return spannerSchema;
  }
}
