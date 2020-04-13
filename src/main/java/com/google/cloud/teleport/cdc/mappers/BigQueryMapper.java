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

package com.google.cloud.teleport.cdc.mappers;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// import org.apache.avro.Schema; // if needed we need to figure out the duplicate here

// TODO: Class comments are required

/**
 * BigQueryMapper is intended to be easily extensible to enable BigQuery schema management during
 * pipeline execution.  New fields and tables will be automatically added to BigQuery when they are
 * detected and before data causes BQ load failures.
 *
 * The BigQueryMapper can be easily extended by overriding: - public TableId getTableId(InputT
 * input) - public TableRow getTableRow(InputT input) - public OutputT getOutputObject(InputT input)
 * - public Map<String, LegacySQLTypeName> getInputSchema(InputT input)
 */
public class BigQueryMapper<InputT, OutputT>
    extends PTransform<PCollection<InputT>, PCollection<OutputT>> {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryMapper.class);
  private BigQuery bigquery;
  private Map<String, Table> tables = new HashMap<String, Table>();
  private Map<String, LegacySQLTypeName> defaultSchema;
  private boolean dayPartitioning = false;
  private final ValueProvider<String> projectIdProvider;

  public BigQueryMapper(String projectId) {
    this.projectIdProvider = StaticValueProvider.of(projectId);
  }

  public BigQueryMapper(ValueProvider<String> projectId) {
    this.projectIdProvider = projectId;
  }

  public TableId getTableId(InputT input) {
    return null;
  }

  public TableRow getTableRow(InputT input) {
    return null;
  }

  public OutputT getOutputObject(InputT input) {
    return null;
  }

  /* Return a HashMap with the Column->Column Type Mapping required from the source
      Implementing getInputSchema will allow the mapper class to support your desired format
  */
  public Map<String, LegacySQLTypeName> getInputSchema(InputT input) {
    return new HashMap<String, LegacySQLTypeName>();
  }

  public String getProjectId() {
    return this.projectIdProvider.get();
  }

  public BigQueryMapper<InputT, OutputT> withDefaultSchema(
      Map<String, LegacySQLTypeName> defaultSchema) {
    this.defaultSchema = defaultSchema;
    return this;
  }

  public BigQueryMapper<InputT, OutputT> withDayPartitioning(boolean dayPartitioning) {
    this.dayPartitioning = dayPartitioning;
    return this;
  }

  /* Return the combination of any schema returned via
      implementing getInputSchema (for complex and dynamic cases)
      and submitting a static default schema.
  */
  private Map<String, LegacySQLTypeName> getObjectSchema(InputT input) {
    Map<String, LegacySQLTypeName> inputSchema = getInputSchema(input);
    if (this.defaultSchema != null) {
      inputSchema.putAll(this.defaultSchema);
    }

    return inputSchema;
  }

  @Override
  public PCollection<OutputT> expand(PCollection<InputT> tableKVPCollection) {
    return tableKVPCollection.apply(
        "TableRowExtractDestination",
        MapElements.via(
            new SimpleFunction<InputT, OutputT>() {
              @Override
              public OutputT apply(InputT input) {
                /*
                    We run validation against every event to ensure all columns
                    exist in source.
                    If a column is in the event and not in BigQuery,
                    the column is added to the table before the event can continue.
                */
                TableId tableId = getTableId(input);
                TableRow row = getTableRow(input);
                Map<String, LegacySQLTypeName> inputSchema = getObjectSchema(input);
                // TODO the Dynamic converter needs to use the tableId object rather than a string

                updateTableIfRequired(tableId, row, inputSchema);
                return getOutputObject(input);
                // return KV.of(tableId, row);
              }
            }));
  }

  private void updateTableIfRequired(TableId tableId, TableRow row,
      Map<String, LegacySQLTypeName> inputSchema) {
    // Ensure Instance of BigQuery Exists
    if (this.bigquery == null) {
      this.bigquery =
          BigQueryOptions.newBuilder()
              .setProjectId(getProjectId())
              .build()
              .getService();
    }

    // Get BigQuery Table for Given Row
    Table table = getBigQueryTable(tableId);

    // Validate Table Schema
    FieldList tableFields = table.getDefinition().getSchema().getFields();

    Set<String> rowKeys = row.keySet();
    Boolean tableWasUpdated = false;
    List<Field> newFieldList = new ArrayList<Field>();
    for (String rowKey : rowKeys) {
      // Check if rowKey (column from data) is in the BQ Table
      try {
        Field tableField = tableFields.get(rowKey);
      } catch (IllegalArgumentException e) {
        tableWasUpdated = addNewTableField(tableId, row, rowKey, newFieldList, inputSchema);
      }
    }

    if (tableWasUpdated) {
      LOG.info("Updating Table");
      updateBigQueryTable(tableId, table, tableFields, newFieldList);
    }
  }

  private Table getBigQueryTable(TableId tableId) {
    String tableName = tableId.toString();
    Table table = tables.get(tableName);

    // Checks that table existed in tables map
    // If not pull table from API
    // TODO we need logic to invalidate table caches
    if (table == null) {
      LOG.info("Pulling Table from API");
      table = bigquery.getTable(tableId);
    }
    // Check that table exists, if not create empty table
    // the empty table will have columns automapped during updateBigQueryTable()
    if (table == null) {
      table = createBigQueryTable(tableId);
    }
    tables.put(tableName, table);

    return table;
  }

  private Table createBigQueryTable(TableId tableId) {
    // Create Blank BigQuery Table
    LOG.info(String.format("Creating Table: %s", tableId.toString()));

    List<Field> fieldList = new ArrayList<Field>();
    Schema schema = Schema.of(fieldList);

    StandardTableDefinition.Builder tableDefinitionBuilder =
        StandardTableDefinition.newBuilder().setSchema(schema);
    if (dayPartitioning) {
      tableDefinitionBuilder.setTimePartitioning(
          TimePartitioning.newBuilder(TimePartitioning.Type.DAY).build());
    }
    TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinitionBuilder.build()).build();
    Table table = bigquery.create(tableInfo);

    return table;
  }

  /* Update BigQuery Table Object Supplied */
  private void updateBigQueryTable(
      TableId tableId, Table table, FieldList tableFields, List<Field> newFieldList) {
    // Table Name to Use for Cache
    String tableName = tableId.toString();

    // Add all current columns to the list
    List<Field> fieldList = new ArrayList<Field>();
    for (Field field : tableFields) {
      fieldList.add(field);
    }
    // Add all new columns to the list
    // TODO use guava to use joiner on multi-thread multi line logging
    LOG.info(tableName);
    LOG.info("Mapping New Columns:");
    for (Field field : newFieldList) {
      fieldList.add(field);
      LOG.info(field.toString());
    }

    Schema newSchema = Schema.of(fieldList);
    Table updatedTable =
        table.toBuilder().setDefinition(StandardTableDefinition.of(newSchema)).build().update();

    tables.put(tableName, updatedTable);
  }

  private Boolean addNewTableField(TableId tableId, TableRow row, String rowKey,
      List<Field> newFieldList, Map<String, LegacySQLTypeName> inputSchema) {
    // Call Get Schema and Extract New Field Type
    Field newField;

    if (inputSchema.containsKey(rowKey)) {
      newField = Field.of(rowKey, inputSchema.get(rowKey));
    } else {
      newField = Field.of(rowKey, LegacySQLTypeName.STRING);
    }

    newFieldList.add(newField);

    // Currently we always add new fields for each call
    // TODO: add an option to ignore new field and why boolean?
    return true;
  }
}
