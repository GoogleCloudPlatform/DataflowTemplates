/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.sqllauncher;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.services.bigquery.model.TableReference;
import org.apache.beam.sdk.extensions.sql.meta.provider.datacatalog.DataCatalogTableProvider;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * Options to configure and create a BigQuery Table output.
 *
 * <p>Parses JSON of the form:
 *
 * <pre>
 *   {
 *    "type": "bigquery",
 *    "table": {"projectId": "..", "datasetId": "..", "tableId": ".."},
 *    "writeDisposition": "WRITE_EMPTY|WRITE_TRUNCATE|WRITE_APPEND"
 *   }
 * </pre>
 *
 * <p>{@link #applyTransform(PCollection, DataflowSqlLauncherOptions)} applies a {@link Write
 * BigQueryIO.Write} transform to the given {@link PCollection}.
 */
class BigQuerySinkDefinition extends SinkDefinition {
  private final TableReference table;
  private final WriteDisposition writeDisposition;

  @JsonCreator
  public BigQuerySinkDefinition(
      @JsonProperty(value = "table", required = true) TableReference table,
      @JsonProperty(value = "writeDisposition", required = true)
          WriteDisposition writeDisposition) {
    this.table = table;
    this.writeDisposition = writeDisposition;
  }

  public TableReference getTable() {
    return table;
  }

  public WriteDisposition getWriteDisposition() {
    return writeDisposition;
  }

  @Override
  String getTableName() {
    return String.format(
        "bigquery.table.`%s`.`%s`.`%s`",
        table.getProjectId(), table.getDatasetId(), table.getTableId());
  }

  @Override
  protected PTransform<PCollection<Row>, WriteResult> createTransform(
      PCollection<Row> queryResult,
      DataflowSqlLauncherOptions options,
      DataCatalogTableProvider tableProvider) {
    if (table == null) {
      throw new InvalidSinkException("No table defined in BigQuery sink definition.");
    }

    Write<Row> bigQuerySink =
        BigQueryIO.<Row>write()
            .withSchema(BigQueryUtils.toTableSchema(queryResult.getSchema()))
            .withFormatFunction(BigQueryUtils.toTableRow())
            .withWriteDisposition(writeDisposition)
            .to(table);

    if (options.getDryRun()) {
      bigQuerySink = bigQuerySink.withoutValidation();
    }

    return bigQuerySink;
  }
}
