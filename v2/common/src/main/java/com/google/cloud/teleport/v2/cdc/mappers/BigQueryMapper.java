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

package com.google.cloud.teleport.v2.cdc.mappers;

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
import com.google.cloud.teleport.v2.transforms.BigQueryConverters;
import com.google.cloud.teleport.v2.utils.CacheUtils.BigQueryTableCache;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BigQueryMapper is intended to be easily extensible to enable BigQuery schema management during
 * pipeline execution.  New fields and tables will be automatically added to BigQuery when they are
 * detected and before data causes BQ load failures.
 *
 * The BigQueryMapper can be easily extended by overriding: - public TableId getTableId(InputT
 * input) - public TableRow getTableRow(InputT input) - public OutputT getOutputObject(InputT input)
 * - public Map<String, LegacySQLTypeName> getInputSchema(TableId tableId, TableRow row)
 */
public class BigQueryMapper<InputT, OutputT>
    extends PTransform<PCollection<InputT>, PCollection<OutputT>> {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryMapper.class);
  private BigQuery bigquery;
  private BigQueryTableRowCleaner bqTableRowCleaner;
  private boolean dayPartitioning = false;
  private Map<String, LegacySQLTypeName> defaultSchema;
  private Set<String> ignoreFields = new HashSet<String>();
  private int mapperRetries = 5;
  private final String projectId;
  private BigQueryTableCache tableCache;

  public BigQueryMapper(String projectId) {
    this.projectId = projectId;
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
  public Map<String, LegacySQLTypeName> getInputSchema(TableId tableId, TableRow row) {
    return new HashMap<String, LegacySQLTypeName>();
  }

  public void setMapperRetries(int retries) {
    this.mapperRetries = retries;
  }

  public int getMapperRetries() {
    return this.mapperRetries;
  }

  public String getProjectId() {
    return this.projectId;
  }

  public BigQueryMapper<InputT, OutputT> withDefaultSchema(
      Map<String, LegacySQLTypeName> defaultSchema) {
    this.defaultSchema = defaultSchema;
    return this;
  }

  /**
   * The function {@link withDefaultSchemaFromGCS} reads a BigQuery Schema file
   * from GCS and stores it to be used in the Mapper schema logic.
   *
   * @param filePath path to file in GCS
   */
  public BigQueryMapper<InputT, OutputT> withDefaultSchemaFromGCS(
      String filePath) {
    if (filePath == null) {
      return this;
    }
    // TODO: A supplier that reloads the GCS file regularly would allow
    // a user to change the file w/o tearing down the pipeline.
    String schemaStr = SchemaUtils.getGcsFileAsString(filePath);
    List<Field> schemaFields = BigQueryConverters.SchemaUtils.schemaFromString(schemaStr);

    Map<String, LegacySQLTypeName> schema = new HashMap<String, LegacySQLTypeName>();
    for (Field field : schemaFields) {
      schema.put(field.getName(), field.getType());
    }

    this.defaultSchema = schema;
    return this;
  }

  /**
   * The function {@link withDayPartitioning} sets a boolean value dictating if
   * day partitioning shoud be applied to new tables created in BigQuery.
   *
   * @param dayPartitioning A boolean value if day partitioning should be applied.
   */
  public BigQueryMapper<InputT, OutputT> withDayPartitioning(boolean dayPartitioning) {
    this.dayPartitioning = dayPartitioning;
    return this;
  }

  /**
   * The function {@link withIgnoreFields} sets a list of fields to be ignored
   * when mapping new columns to a BigQuery Table.
   *
   * @param fields A Set of fields to ignore.
   */
  public BigQueryMapper<InputT, OutputT> withIgnoreFields(Set fields) {
    this.ignoreFields = fields;

    return this;
  }

  /* Return the combination of any schema returned via
      implementing getInputSchema (for complex and dynamic cases)
      and submitting a static default schema.
  */
  private Map<String, LegacySQLTypeName> getObjectSchema(TableId tableId, TableRow row) {
    Map<String, LegacySQLTypeName> inputSchema = getInputSchema(tableId, row);
    if (this.defaultSchema != null) {
      inputSchema.putAll(this.defaultSchema);
    }

    return inputSchema;
  }

  /** Sets all objects needed during mapper execution. */
  public void setUp() {
    if (this.bqTableRowCleaner == null) {
      this.bqTableRowCleaner = BigQueryTableRowCleaner.getBigQueryTableRowCleaner();
    }
    if (this.bigquery == null) {
      this.bigquery =
          BigQueryOptions.newBuilder().setProjectId(getProjectId()).build().getService();
    }
    if (this.tableCache == null) {
      this.tableCache = new BigQueryTableCache(this.bigquery);
    }
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
                setUp();
                TableId tableId = getTableId(input);
                TableRow row = getTableRow(input);
                int retries = getMapperRetries();

                applyMapperToTableRow(tableId, row, retries);
                return getOutputObject(input);
              }
            })
        );
  }

  /**
   * Sets the {@code BigQueryTableRowCleaner} used in BigQuery TableRow cleanup.
   *
   * @param cleaner a BigQueryTableRowCleaner object to use in cleanup.
   */
  public void setBigQueryTableRowCleaner(BigQueryTableRowCleaner cleaner) {
    this.bqTableRowCleaner = cleaner;
  }

  /**
   * Returns {@code TableRow} after cleaning each field according to
   * the data type found in BigQuery.
   *
   * @param tableId a TableId referencing the BigQuery table to be loaded to.
   * @param row a TableRow with the raw data to be loaded into BigQuery.
   */
  public TableRow getCleanedTableRow(TableId tableId, TableRow row) {
    TableRow cleanRow = row.clone();

    Table table = this.tableCache.get(tableId);
    FieldList tableFields = table.getDefinition().getSchema().getFields();

    Set<String> rowKeys = cleanRow.keySet();
    for (String rowKey : rowKeys) {
      if (this.ignoreFields.contains(rowKey)) {
        continue;
      }
      this.bqTableRowCleaner.cleanTableRowField(cleanRow, tableFields, rowKey);
    }

    return cleanRow;
  }

  /**
   * Extracts and applies new column information to BigQuery by comparing the TableRow against the
   * BigQuery Table. Retries the supplied number of times before failing.
   *
   * @param tableId a TableId referencing the BigQuery table to be loaded to.
   * @param row a TableRow with the raw data to be loaded into BigQuery.
   * @param inputSchema The source schema lookup to be used in mapping.
   * @param retries Number of remaining retries before error is raised.
   */
  private void applyMapperToTableRow(
      TableId tableId, TableRow row, int retries) {
    try {
      updateTableIfRequired(tableId, row);
    } catch (Exception e) {
      if (retries > 0) {
        LOG.info("Mapper Retry {} Remaining: {}", String.valueOf(retries), e.toString());
        try {
          Thread.sleep(2000);
        } catch (InterruptedException i) {
          throw e;
        }
        applyMapperToTableRow(tableId, row, retries - 1);
      } else {
        LOG.info("Mapper Retries Exceeded: {}", e.toString());
        throw e;
      }
    }
  }

  /**
   * Extracts and applies new column information to BigQuery by comparing the TableRow against the
   * BigQuery Table.
   *
   * @param tableId a TableId referencing the BigQuery table to be loaded to.
   * @param row a TableRow with the raw data to be loaded into BigQuery.
   * @param inputSchema The source schema lookup to be used in mapping.
   */
  private void updateTableIfRequired(
      TableId tableId, TableRow row) {
    Table table = getOrCreateBigQueryTable(tableId);
    FieldList tableFields = table.getDefinition().getSchema().getFields();
    Map<String, LegacySQLTypeName> inputSchema = new HashMap<String, LegacySQLTypeName>();


    Set<String> rowKeys = row.keySet();
    Boolean tableWasUpdated = false;
    List<Field> newFieldList = new ArrayList<Field>();
    for (String rowKey : rowKeys) {
      if (this.ignoreFields.contains(rowKey)) {
        continue;
      }
      // Check if rowKey (column from data) is in the BQ Table
      try {
        Field tableField = tableFields.get(rowKey);
      } catch (IllegalArgumentException e) {
        if (inputSchema.isEmpty()) {
          inputSchema = getObjectSchema(tableId, row);
        }
        tableWasUpdated = addNewTableField(tableId, row, rowKey, newFieldList, inputSchema);
      }
    }

    if (tableWasUpdated) {
      LOG.info("Updating Table");
      updateBigQueryTable(tableId, table, tableFields, newFieldList);
    }
  }

  /**
   * Returns {@code Table} which was either extracted from the cache or created.
   *
   * @param tableId a TableId referencing the BigQuery table being requested.
   */
  private Table getOrCreateBigQueryTable(TableId tableId) {
    Table table = this.tableCache.get(tableId);

    // Check that table exists, if not create empty table
    // the empty table will have columns automapped during updateBigQueryTable()
    if (table == null) {
      LOG.info("Creating Table: {}", tableId.toString());
      table = createBigQueryTable(tableId);
      table = this.tableCache.reset(tableId);
    }

    return table;
  }

  /**
   * Returns {@code Table} after creating the table with no columns in BigQuery.
   *
   * @param tableId a TableId referencing the BigQuery table being requested.
   */
  private Table createBigQueryTable(TableId tableId) {
    // Create Blank BigQuery Table
    List<Field> fieldList = new ArrayList<Field>();
    Schema schema = Schema.of(fieldList);

    StandardTableDefinition.Builder tableDefinitionBuilder =
        StandardTableDefinition.newBuilder().setSchema(schema);
    if (dayPartitioning) {
      tableDefinitionBuilder.setTimePartitioning(
          TimePartitioning.newBuilder(TimePartitioning.Type.DAY).build());
    }
    LOG.info("Creating BQ Table {} with  schema {}", tableId, schema);
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
    LOG.info("Updated Table");

    this.tableCache.reset(tableId);
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
