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
package com.google.cloud.teleport.v2.transforms;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Mutation.WriteBuilder;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.utils.SpannerUtils;
import com.google.cloud.teleport.v2.values.SpannerSchema;
import com.google.cloud.teleport.v2.values.SpannerSchema.SpannerDataTypes;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.transforms.DoFn;
import org.json.JSONObject;

/** Cloud Spanner transformation helper that converts each row to Mutation object. */
public class JsonStringToMutationFn extends DoFn<String, Mutation> {

  private final String projectId;
  private final String spannerTableName;
  private final String spannerDBName;
  private final String spannerInstanceName;

  private SpannerSchema spannerSchema = null;
  private List<String> spannerSchemaColumns = null;

  public JsonStringToMutationFn(
      String projectId, String spannerInstanceName, String spannerDBName, String spannerTableName) {
    this.spannerTableName = spannerTableName;
    this.spannerDBName = spannerDBName;
    this.spannerInstanceName = spannerInstanceName;
    this.projectId = projectId;
  }

  @Setup
  public void setup() {
    this.spannerSchema =
        SpannerUtils.getSpannerSchemaFromSpanner(
            this.projectId, spannerInstanceName, spannerDBName, spannerTableName);
    this.spannerSchemaColumns = spannerSchema.getColumnList();
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws IOException {
    JsonObject row = JsonParser.parseString(Objects.requireNonNull(c.element())).getAsJsonObject();
    WriteBuilder writeBuilder = Mutation.newInsertOrUpdateBuilder(spannerTableName);
    c.output(parseRow(writeBuilder, row, spannerSchema, spannerSchemaColumns));
  }

  /**
   * Create a Mutation object based on the row and schema provided.
   *
   * @param builder Mutation builder to construct object
   * @param row parsed JSONObject containing row data
   * @param spannerSchema schema with column name and column data type
   * @return build Mutation object for the row
   */
  @VisibleForTesting
  Mutation parseRow(
      WriteBuilder builder,
      JsonObject row,
      SpannerSchema spannerSchema,
      List<String> spannerSchemaColumns)
      throws IllegalArgumentException {
    if (row.size() != spannerSchemaColumns.size()) {
      throw new RuntimeException(
          "Parsed row has different number of column than schema. Row content: " + row);
    }

    // Extract cell by cell and construct Mutation object
    for (int i = 0; i < row.size(); i++) {
      String columnName = spannerSchemaColumns.get(i);
      SpannerDataTypes columnType = spannerSchema.getColumnType(columnName);
      JsonElement cellValue = row.get(columnName);

      if (cellValue == JSONObject.NULL) {
        builder.set(columnName).to(Value.string(null));
      } else {
        // TODO: make the tests below match Spanner's SQL literal rules wherever possible,
        // in terms of how input is accepted, and throw exceptions on invalid input.
        switch (columnType) {
          case BOOL:
            builder.set(columnName).to(cellValue.getAsBoolean());
            break;
          case INT64:
            builder.set(columnName).to(cellValue.getAsInt());
            break;
          case STRING:
            builder.set(columnName).to(cellValue.getAsString());
            break;
          case FLOAT64:
            builder.set(columnName).to(cellValue.getAsFloat());
            break;
          case DATE:
            // This requires date type in format of 'YYYY-[M]M-[D]D'
            builder
                .set(columnName)
                .to(com.google.cloud.Date.parseDate(cellValue.getAsString().trim()));
            break;
          case TIMESTAMP:
            builder
                .set(columnName)
                .to(
                    com.google.cloud.Timestamp.parseTimestamp(
                        cellValue.getAsString().replaceAll("\"", "").trim()));
            break;
          default:
            throw new IllegalArgumentException(
                "Unrecognized column data type: " + spannerSchema.getColumnType(columnName));
        }
      }
    }

    return builder.build();
  }
}
