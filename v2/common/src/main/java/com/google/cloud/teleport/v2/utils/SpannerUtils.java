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

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.teleport.v2.values.SpannerSchema;
import com.google.common.base.Strings;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.util.StreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility methods for Spanner. */
public class SpannerUtils {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerUtils.class);

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
