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
package com.google.cloud.teleport.v2.cdc.mappers;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters;
import com.google.cloud.teleport.v2.utils.BigQueryTableCache;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BigQueryMapper is intended to be easily extensible to enable BigQuery schema management during
 * pipeline execution. New fields and tables will be automatically added to BigQuery when they are
 * detected and before data causes BQ load failures.
 *
 * <p>The BigQueryMapper can be easily extended by overriding: - public TableId getTableId(InputT
 * input) - public TableRow getTableRow(InputT input) - public OutputT getOutputObject(InputT input)
 * - public Map<String, StandardSQLTypeName> getInputSchema(TableId tableId, TableRow row)
 */
public class BigQueryMapper<InputT, OutputT>
    extends PTransform<PCollection<InputT>, PCollection<OutputT>> {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryMapper.class);
  private BigQuery bigquery;
  private BigQueryTableRowCleaner bqTableRowCleaner;
  private boolean dayPartitioning = false;
  private Map<String, StandardSQLTypeName> defaultSchema;
  private Set<String> ignoreFields = new HashSet<String>();
  private int mapperRetries = 5;
  private final String projectId;
  private static BigQueryTableCache tableCache;
  private static final Cache<String, TableId> tableLockMap =
      CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).<String, TableId>build();

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
  public Map<String, StandardSQLTypeName> getInputSchema(TableId tableId, TableRow row) {
    return new HashMap<String, StandardSQLTypeName>();
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
      Map<String, StandardSQLTypeName> defaultSchema) {
    this.defaultSchema = defaultSchema;
    return this;
  }

  /**
   * The function {@link withDefaultSchemaFromGCS} reads a BigQuery Schema file from GCS and stores
   * it to be used in the Mapper schema logic.
   *
   * @param filePath path to file in GCS
   */
  public BigQueryMapper<InputT, OutputT> withDefaultSchemaFromGCS(String filePath) {
    if (filePath == null) {
      return this;
    }
    // TODO: A supplier that reloads the GCS file regularly would allow
    // a user to change the file w/o tearing down the pipeline.
    String schemaStr = SchemaUtils.getGcsFileAsString(filePath);
    List<Field> schemaFields = BigQueryConverters.SchemaUtils.schemaFromString(schemaStr);

    Map<String, StandardSQLTypeName> schema = new HashMap<String, StandardSQLTypeName>();
    for (Field field : schemaFields) {
      schema.put(field.getName(), field.getType().getStandardType());
    }

    this.defaultSchema = schema;
    return this;
  }

  /**
   * The function {@link withDayPartitioning} sets a boolean value dictating if day partitioning
   * shoud be applied to new tables created in BigQuery.
   *
   * @param dayPartitioning A boolean value if day partitioning should be applied.
   */
  public BigQueryMapper<InputT, OutputT> withDayPartitioning(boolean dayPartitioning) {
    this.dayPartitioning = dayPartitioning;
    return this;
  }

  /**
   * The function {@link withIgnoreFields} sets a list of fields to be ignored when mapping new
   * columns to a BigQuery Table.
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
  private Map<String, StandardSQLTypeName> getObjectSchema(TableId tableId, TableRow row) {
    Map<String, StandardSQLTypeName> inputSchema = getInputSchema(tableId, row);
    if (this.defaultSchema != null) {
      inputSchema.putAll(this.defaultSchema);
    }

    return inputSchema;
  }

  private synchronized void setUpTableCache() {
    if (this.tableCache == null) {
      this.tableCache =
          (BigQueryTableCache)
              new BigQueryTableCache(bigquery)
                  .withCacheResetTimeUnitValue(60)
                  .withCacheNumRetries(3);
    }
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
      setUpTableCache();
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
            }));
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
   * Returns {@code TableRow} after cleaning each field according to the data type found in
   * BigQuery.
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
  private void applyMapperToTableRow(TableId tableId, TableRow row, int retries) {
    try {
      updateTableIfRequired(tableId, row);
    } catch (Exception e) {
      if (retries > 0) {
        try {
          int sleepSecs = (getMapperRetries() - retries + 1) * 5;
          LOG.info(
              "Mapper Retry {} Remaining: {}: {}",
              String.valueOf(retries),
              tableId.toString(),
              e.toString());
          Thread.sleep(sleepSecs);
          applyMapperToTableRow(tableId, row, retries - 1);
        } catch (InterruptedException i) {
          LOG.info("Mapper Retries Interrupted: {}: {}", tableId.toString(), e.toString());
          throw e;
        }
      } else {
        LOG.info("Mapper Retries Exceeded: {}: {}", tableId.toString(), e.toString());
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
  private void updateTableIfRequired(TableId tableId, TableRow row) {
    Table table = this.tableCache.getOrCreateBigQueryTable(tableId, this.dayPartitioning);

    Map<String, StandardSQLTypeName> inputSchema = new HashMap<String, StandardSQLTypeName>();
    List<Field> newFieldList = getNewTableFields(row, table, inputSchema, this.ignoreFields);

    if (newFieldList.size() > 0) {
      LOG.info("Updating Table: {}", tableId.toString());
      updateBigQueryTable(tableId, row, this.ignoreFields);
    }
  }

  private static TableId getTableLock(TableId tableId) {
    TableId tableLock = tableLockMap.getIfPresent(tableId.toString());
    if (tableLock != null) {
      return tableLock;
    }

    synchronized (tableLockMap) {
      tableLock = tableLockMap.getIfPresent(tableId.toString());
      if (tableLock != null) {
        return tableLock;
      }

      tableLockMap.put(tableId.toString(), tableId);
      return tableId;
    }
  }

  /* Update BigQuery Table Object Supplied */
  private void updateBigQueryTable(TableId tableId, TableRow row, Set<String> ignoreFields) {
    TableId tableLock = getTableLock(tableId);
    synchronized (tableLock) {
      Table table = this.tableCache.get(tableId);
      Map<String, StandardSQLTypeName> inputSchema = getObjectSchema(tableId, row);

      List<Field> newFieldList = getNewTableFields(row, table, inputSchema, ignoreFields);
      if (newFieldList.size() > 0) {
        // Add all current columns to the list
        List<Field> fieldList = new ArrayList<Field>();
        for (Field field : table.getDefinition().getSchema().getFields()) {
          fieldList.add(field);
        }

        // Add all new columns to the list
        LOG.info("Mapping New Columns for: {} -> {}", tableId.toString(), newFieldList.toString());
        for (Field field : newFieldList) {
          fieldList.add(field);
        }

        Schema newSchema = Schema.of(fieldList);
        Table updatedTable =
            table.toBuilder().setDefinition(StandardTableDefinition.of(newSchema)).build().update();
        LOG.info("Updated Table: {}", tableId.toString());

        this.tableCache.reset(tableId, table);
      }
    }
  }

  public static List<Field> getNewTableFields(
      TableRow row,
      Table table,
      Map<String, StandardSQLTypeName> inputSchema,
      Set<String> ignoreFields) {
    List<Field> newFieldList = new ArrayList<Field>();
    FieldList tableFields = table.getDefinition().getSchema().getFields();
    Set<String> rowKeys = row.keySet();

    for (String rowKey : rowKeys) {
      if (ignoreFields.contains(rowKey)) {
        continue;
      }
      try {
        Field tableField = tableFields.get(rowKey);
      } catch (IllegalArgumentException e) {
        addNewTableField(rowKey, newFieldList, inputSchema);
      }
    }

    return newFieldList;
  }

  public static void addNewTableField(
      String rowKey, List<Field> newFieldList, Map<String, StandardSQLTypeName> inputSchema) {
    Field newField;
    if (inputSchema.containsKey(rowKey)) {
      newField = Field.of(rowKey, inputSchema.get(rowKey));
    } else {
      newField = Field.of(rowKey, StandardSQLTypeName.STRING);
    }

    newFieldList.add(newField);
  }
}
