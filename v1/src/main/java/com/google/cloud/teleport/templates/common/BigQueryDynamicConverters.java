/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.templates.common;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.TableId;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class {@link BigQueryDynamicConverters}. */
public class BigQueryDynamicConverters {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryDynamicConverters.class);

  /**
   * Section 1: Transform PCollection&lt;TableRow&gt; into PCollection&lt;KV&lt;TableId,
   * TableRow&gt;gt; with table state added.
   */
  public static PTransform<PCollection<TableRow>, PCollection<KV<TableId, TableRow>>>
      extractTableRowDestination(
          ValueProvider<String> projectId,
          ValueProvider<String> datasetTemplate,
          ValueProvider<String> tableTemplate) {
    return new ExtractTableRowDestination(projectId, datasetTemplate, tableTemplate);
  }

  public static PTransform<PCollection<TableRow>, PCollection<KV<TableId, TableRow>>>
      extractTableRowDestination(
          ValueProvider<String> datasetTemplate, ValueProvider<String> tableTemplate) {
    return new ExtractTableRowDestination(datasetTemplate, tableTemplate);
  }

  /** Converts UTF8 encoded Json records to TableRow records. */
  private static class ExtractTableRowDestination
      extends PTransform<PCollection<TableRow>, PCollection<KV<TableId, TableRow>>> {

    private ValueProvider<String> projectId;
    private ValueProvider<String> datasetTemplate;
    private ValueProvider<String> tableTemplate;

    public ExtractTableRowDestination(
        ValueProvider<String> datasetTemplate, ValueProvider<String> tableTemplate) {
      this.datasetTemplate = datasetTemplate;
      this.tableTemplate = tableTemplate;
    }

    public ExtractTableRowDestination(
        ValueProvider<String> projectId,
        ValueProvider<String> datasetTemplate,
        ValueProvider<String> tableTemplate) {
      this.projectId = projectId;
      this.datasetTemplate = datasetTemplate;
      this.tableTemplate = tableTemplate;
    }

    @Override
    public PCollection<KV<TableId, TableRow>> expand(PCollection<TableRow> tableRowPCollection) {
      return tableRowPCollection.apply(
          "TableRowExtractDestination",
          MapElements.via(
              new SimpleFunction<TableRow, KV<TableId, TableRow>>() {
                @Override
                public KV<TableId, TableRow> apply(TableRow row) {
                  TableId tableId = getTableId(row);
                  TableRow resultTableRow = cleanTableRow(row.clone());

                  return KV.of(tableId, resultTableRow);
                }
              }));
    }

    public TableId getTableId(TableRow input) {
      String datasetName = BigQueryConverters.formatStringTemplate(datasetTemplate.get(), input);
      String tableName = BigQueryConverters.formatStringTemplate(tableTemplate.get(), input);

      if (projectId == null) {
        return TableId.of(datasetName, tableName);
      } else {
        return TableId.of(projectId.get(), datasetName, tableName);
      }
    }

    public TableRow cleanTableRow(TableRow row) {
      // Remove Table fields as we don't need them in the table
      // TODO: how should I know to remove these?  Maybe leave them?
      // row.remove(datasetField.get());
      // row.remove(tableField.get());

      return row;
    }
  }

  /* Section 2: Dynamic Destination Logic to be used in BigQueryIO. */
  public static DynamicDestinations<KV<TableId, TableRow>, KV<TableId, TableRow>>
      bigQueryDynamicDestination() {
    return new BigQueryDynamicDestination();
  }

  /**
   * Class {@link BigQueryDynamicDestination} Class BigQueryDynamicDestination loads into BigQuery
   * tables in a dynamic fashion. The destination table is based on the TableId supplied by previous
   * steps.
   */
  public static class BigQueryDynamicDestination
      extends DynamicDestinations<KV<TableId, TableRow>, KV<TableId, TableRow>> {

    // Instead of the above we will assume the fields to use are hardcoded
    public BigQueryDynamicDestination() {}

    @Override
    public KV<TableId, TableRow> getDestination(
        ValueInSingleWindow<KV<TableId, TableRow>> element) {
      // Value is formatted as needed in ExtractTableRowDestination
      return element.getValue();
    }

    @Override
    public TableDestination getTable(KV<TableId, TableRow> destination) {
      TableId tableId = destination.getKey();
      // TODO String.format("%s:%s.%s", projectId.get(), datasetName.get(), key) if project id is
      // req
      String tableName = String.format("%s.%s", tableId.getDataset(), tableId.getTable());
      TableDestination dest =
          new TableDestination(tableName, "Name of table pulled from data fields");

      return dest;
    }

    @Override
    public TableSchema getSchema(KV<TableId, TableRow> destination) {

      TableRow bqRow = destination.getValue();
      TableSchema schema = new TableSchema();
      List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>();
      for (String field : bqRow.keySet()) {
        /**  currently all BQ data types are set to String */
        // Why do we use checkHeaderName here and not elsewhere, TODO if we add this back in
        // fields.add(new TableFieldSchema().setName(checkHeaderName(header)).setType("STRING"));
        fields.add(new TableFieldSchema().setName(field).setType("STRING"));
      }

      schema.setFields(fields);
      return schema;
    }
  }
}
