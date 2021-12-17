/*
 * Copyright (C) 2019 Google LLC
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
package com.google.cloud.dataflow.cdc.applier;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements the {@link DynamicDestinations} interface to control writing to BigQuery
 * tables with changelogs.
 *
 * <p>It controls the tasks of identifying the changelog table for each row representing a change,
 * and providing the schema for that table.
 */
class ChangelogTableDynamicDestinations extends DynamicDestinations<TableRow, String> {

  private static final Logger LOG =
      LoggerFactory.getLogger(ChangelogTableDynamicDestinations.class);

  final String changeLogDataset;
  final String gcpProjectId;
  final PCollectionView<Map<String, KV<Schema, Schema>>> schemaMapView;

  ChangelogTableDynamicDestinations(
      String changeLogDataset,
      String gcpProjectId,
      PCollectionView<Map<String, KV<Schema, Schema>>> schemaMapView) {
    this.changeLogDataset = changeLogDataset;
    this.gcpProjectId = gcpProjectId;
    this.schemaMapView = schemaMapView;
  }

  /**
   * Convert a CDC source-provided table name (i.e. "${instance}.${database}.${table}") into the
   * table name to add into BigQuery (i.e. "${table}"), or changelog table name (i.e.
   * "${table}_changelog").
   *
   * @param sourceTable the fully qualified table name coming from MySQL.
   * @param isChangelogTable tells whether the Table name is a Change Log table, or a plain table.
   * @return
   */
  public static String getBigQueryTableName(String sourceTable, Boolean isChangelogTable) {
    String[] tableNameComponents = sourceTable.split("\\.");
    LOG.debug("Source table: {}. After split: {}", sourceTable, tableNameComponents);
    if (isChangelogTable) {
      return String.format("%s_changelog", tableNameComponents[2]);
    } else {
      return tableNameComponents[2];
    }
  }

  @Override
  public String getDestination(ValueInSingleWindow<TableRow> rowInfo) {
    Object o = rowInfo.getValue().get("tableName");
    assert o instanceof String;

    // The input targetTable comes from Debezium as "${instance}.${database}.${table}".
    // We extract the table name, and append "_changelog" to it: "${table}_changelog".
    String targetTable = (String) o;
    return targetTable;
  }

  @Override
  public TableDestination getTable(String targetTable) {
    String changelogTableName = getBigQueryTableName(targetTable, true);

    TableReference tableRef =
        new TableReference()
            .setTableId(changelogTableName)
            .setDatasetId(changeLogDataset)
            .setProjectId(gcpProjectId);
    String description = String.format("Changelog Table for {}", targetTable);

    return new TableDestination(tableRef, description);
  }

  @Override
  public TableSchema getSchema(String targetTable) {
    Map<String, KV<Schema, Schema>> schemaMap = this.sideInput(schemaMapView);
    KV<Schema, Schema> keyAndValueSchemas = schemaMap.get(targetTable);

    TableFieldSchema rowSchema =
        new TableFieldSchema()
            .setName("fullRecord")
            .setType("RECORD")
            .setMode("NULLABLE") // This field is null for deletions
            .setFields(BigQueryUtils.toTableSchema(keyAndValueSchemas.getValue()).getFields());

    TableFieldSchema pkSchema =
        new TableFieldSchema()
            .setName("primaryKey")
            .setType("RECORD")
            .setFields(BigQueryUtils.toTableSchema(keyAndValueSchemas.getKey()).getFields());

    TableSchema changelogTableSchema =
        new TableSchema()
            .setFields(
                Arrays.asList(
                    rowSchema,
                    pkSchema,
                    new TableFieldSchema().setName("operation").setType("STRING"),
                    new TableFieldSchema().setName("timestampMs").setType("INT64"),
                    new TableFieldSchema().setName("tableName").setType("STRING")));

    return changelogTableSchema;
  }

  @Override
  public List<PCollectionView<?>> getSideInputs() {
    return Arrays.asList(schemaMapView);
  }
}
