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
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.io.gcp.spanner.SpannerSchema;
import org.apache.beam.sdk.io.gcp.spanner.SpannerSchema.Column;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.json.JSONObject;

/** Cloud Spanner transformation helper that convert each row to Mutation object. */
public class JsonStringToMutationFn extends DoFn<String, Mutation> {
  private final String spannerTableName;
  private final PCollectionView<SpannerSchema> spannerSchemaView;

  public JsonStringToMutationFn(
      String spannerTableName, PCollectionView<SpannerSchema> spannerSchemaView) {
    this.spannerTableName = spannerTableName;
    this.spannerSchemaView = spannerSchemaView;
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws IOException {
    SpannerSchema spannerSchema = c.sideInput(spannerSchemaView);
    List<Column> spannerSchemaColumns = spannerSchema.getColumns(spannerTableName);
    JsonObject row =
        JsonParser.parseString(Objects.requireNonNull(c.element()).toLowerCase()).getAsJsonObject();
    WriteBuilder writeBuilder = Mutation.newInsertOrUpdateBuilder(spannerTableName);
    c.output(parseRow(writeBuilder, row, spannerSchemaColumns));
  }

  /**
   * Create a Mutation object based on the row and schema provided.
   *
   * @param builder Mutation builder to construct object
   * @param row parsed JSONObject containing row data
   * @return build Mutation object for the row
   */
  @VisibleForTesting
  Mutation parseRow(WriteBuilder builder, JsonObject row, List<Column> spannerSchemaColumns)
      throws IllegalArgumentException {
    if (row.size() != spannerSchemaColumns.size()) {
      throw new RuntimeException(
          "Parsed row has different number of column than schema. Row content: " + row);
    }

    // Extract cell by cell and construct Mutation object
    for (int i = 0; i < row.size(); i++) {
      Column spannerColumn = spannerSchemaColumns.get(i);
      String columnName = spannerColumn.getName();
      Type columnType = spannerColumn.getType();
      JsonElement cellValue = row.get(columnName);

      if (cellValue == JSONObject.NULL) {
        builder.set(columnName).to(Value.string(null));
      } else {
        // TODO: make the tests below match Spanner's SQL literal rules wherever possible,
        // in terms of how input is accepted, and throw exceptions on invalid input.
        switch (columnType.getCode()) {
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
            throw new IllegalArgumentException("Unrecognized column data type: " + columnType);
        }
      }
    }

    return builder.build();
  }
}
