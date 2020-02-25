/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.teleport.templates.common;

import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.TableId;
import java.io.*;
import java.util.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

public class BigQueryDynamicConverters {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryDynamicConverters.class);

  /**
   * Section 1: Transform PCollection<TableRow> into PCollection<KV<TableId, TableRow>> with table
   * state added
   */
  public static PTransform<PCollection<TableRow>, PCollection<KV<TableId, TableRow>>>
      extractTableRowDestination(
          ValueProvider<String> datasetField,
          ValueProvider<String> datasetName,
          ValueProvider<String> tableField) {
    return new ExtractTableRowDestination(datasetField, datasetName, tableField);
    }

    /** Converts UTF8 encoded Json records to TableRow records. */
    private static class ExtractTableRowDestination
        extends PTransform<PCollection<TableRow>, PCollection<KV<TableId, TableRow>>> {

        private ValueProvider<String> datasetField;
    private ValueProvider<String> datasetName;
        private ValueProvider<String> tableField;

    // Instead of the above we will assume the fields to use are hardcoded
    public ExtractTableRowDestination(
        ValueProvider<String> datasetField,
        ValueProvider<String> datasetName,
        ValueProvider<String> tableField) {
            this.datasetField = datasetField;
      this.datasetName = datasetName;
            this.tableField = tableField;
        }

        @Override
        public PCollection<KV<TableId, TableRow>> expand(PCollection<TableRow> tableRowPCollection) {
            return tableRowPCollection.apply(
                "TableRowExtractDestination",
                MapElements.via(
                    new SimpleFunction<TableRow, KV<TableId, TableRow>>() {
                    @Override
                    public KV<TableId, TableRow> apply(TableRow row) {
                        TableId tableId = getDestinationTableId(row);
                        TableRow resultTableRow = cleanTableRow(row.clone());

                        return KV.of(tableId, resultTableRow);
                    }
            }));
        }

        public TableId getDestinationTableId(TableRow element) {
      // Grab the Dataset and Table Name to use
      // TODO: CHECK THAT ONLY DATASET_NAME OR DATASET_FIELD HAVE BEEN PROVIDED.
      String dataset;
      if (datasetName.isAccessible() && datasetName.get() != null && !datasetName.get().isEmpty()) {
        dataset = datasetName.get();
      } else {
        dataset = element.get(datasetField.get()).toString();
      }
            String table = element.get(tableField.get()).toString();

            TableId tableId = TableId.of(dataset, table);
            return tableId;
        }

        public TableRow cleanTableRow(TableRow row) {
            // Remove Table fields as we don't need them in the table
            row.remove(datasetField.get());
            row.remove(tableField.get());

            return row;
        }
    }

    /** Section 2: Dynamic Destination Logic to be used in BigQueryIO */
    public static DynamicDestinations<KV<TableId, TableRow>, KV<TableId, TableRow>> bigQueryDynamicDestination() {
        return new BigQueryDynamicDestination();
    }

    public static class BigQueryDynamicDestination
      extends DynamicDestinations<KV<TableId, TableRow>, KV<TableId, TableRow>> {

        // Instead of the above we will assume the fields to use are hardcoded
        public BigQueryDynamicDestination() {
        }

        @Override
        public KV<TableId, TableRow> getDestination(ValueInSingleWindow<KV<TableId, TableRow>> element) {
            // Value is formatted as needed in ExtractTableRowDestination
            return element.getValue();
        }

        @Override
        public TableDestination getTable(KV<TableId, TableRow> destination) {
            TableId tableId = destination.getKey();
            // TODO String.format("%s:%s.%s", projectId.get(), datasetName.get(), key) if project id is req
            String tableName = String.format("%s.%s", tableId.getDataset(), tableId.getTable());
            TableDestination dest =
                new TableDestination(tableName, "Name of table pulled from datafields");

            return dest;
        }

        @Override
        public TableSchema getSchema(KV<TableId, TableRow> destination) {

            TableRow bqRow = destination.getValue();
            TableSchema schema = new TableSchema();
            List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>();
            List<TableCell> cells = bqRow.getF();
            for (int i = 0; i < cells.size(); i++) {
                Map<String, Object> object = cells.get(i);
                String header = object.keySet().iterator().next();
                /** currently all BQ data types are set to String */
                // Why do we use checkHeaderName here and not elsewhere, TODO if we add this back in
                // fields.add(new TableFieldSchema().setName(checkHeaderName(header)).setType("STRING"));
                fields.add(new TableFieldSchema().setName(header).setType("STRING"));
            }

            schema.setFields(fields);
            return schema;
        }
    }
}
