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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
 * - public Map<String, LegacySQLTypeName> getInputSchema(InputT input)
 */
public class BigQueryMapper<InputT, OutputT>
    extends PTransform<PCollection<InputT>, PCollection<OutputT>> {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryMapper.class);
  private BigQuery bigquery;
  private Map<String, Table> tables = new HashMap<String, Table>();
  private Map<String, LegacySQLTypeName> defaultSchema;
  private boolean dayPartitioning = false;
  private final String projectId;
  private BigQueryTableRowCleaner bqTableRowCleaner;

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
  public Map<String, LegacySQLTypeName> getInputSchema(InputT input) {
    return new HashMap<String, LegacySQLTypeName>();
  }

  public String getProjectId() {
    return this.projectId;
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

                updateTableIfRequired(tableId, row, inputSchema);
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
   * Returns {@code BigQueryTableRowCleaner} to clean TableRow data after BigQuery mapping.
   */
  public BigQueryTableRowCleaner getBigQueryTableRowCleaner() {
    if (this.bqTableRowCleaner == null) {
      this.bqTableRowCleaner = BigQueryTableRowCleaner.getBigQueryTableRowCleaner();
    }

    return this.bqTableRowCleaner;
  }

  /**
   * Returns {@code TableRow} after cleaning each field according to
   * the data type found in BigQuery.
   *
   * @param tableId a TableId referencing the BigQuery table to be loaded to.
   * @param row a TableRow with the raw data to be loaded into BigQuery.
   */
  public TableRow getCleanedTableRow(TableId tableId, TableRow row) {
    BigQueryTableRowCleaner bqCleaner = getBigQueryTableRowCleaner();
    TableRow cleanRow = row.clone();

    Table table = getCachedTable(tableId);
    FieldList tableFields = table.getDefinition().getSchema().getFields();

    Set<String> rowKeys = cleanRow.keySet();
    for (String rowKey : rowKeys) {
      bqCleaner.cleanTableRowField(cleanRow, tableFields, rowKey);
    }

    return cleanRow;
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

      if (recursiveFieldCheck(rowKey, row.get(rowKey), tableFields)) {
        tableWasUpdated = addNewTableField(tableId, row, rowKey, newFieldList, inputSchema);
        LOG.info("table was updated: {}", tableWasUpdated.toString());
      }
    }

    if (tableWasUpdated) {
      LOG.info("Updating Table");
      updateBigQueryTable(tableId, table, tableFields, newFieldList);
    }
  }

  private Boolean recursiveFieldCheck(String rowKey, Object rowValue, FieldList tableFields) {
    Boolean updateField = false;
    try {
      Field tableField = tableFields.get(rowKey);
      if (rowValue instanceof List) {
        //unpack first element so we can check if it is also an object
        List cellList = (List) rowValue;
        if (!cellList.isEmpty()) {
          rowValue = cellList.get(0);
        }
      }
      if (getSQLType(rowValue).equals(LegacySQLTypeName.RECORD)) {
        LOG.info("recursing field check into object: {}", rowKey);
        HashMap cellMap = (HashMap) rowValue;
        Set<String> objKeys = cellMap.keySet();
        for (String objKey : objKeys) {
          if (recursiveFieldCheck(objKey, cellMap.get(objKey), tableField.getSubFields())) {
            LOG.info("found nested field mismatch on {}", objKey);
            updateField = true;
          }
        }
      }
    } catch (IllegalArgumentException e) {
      updateField = true;
      LOG.info("caught exception on {}", rowKey);
    }
    return updateField;
  }

  /**
   * Returns {@code Table} from BigQuery or cache referenced by the supplied TableId
   * the data type found in BigQuery.
   *
   * @param tableId a TableId referencing the BigQuery table to be loaded to.
   */
  private Table getCachedTable(TableId tableId) {
    String tableName = tableId.toString();
    Table table = tables.get(tableName);

    return table;
  }

  /**
   * Sets the BigQuery {@code Table} in local cache.
   * the data type found in BigQuery.
   *
   * @param tableId a TableId referencing the BigQuery table to be loaded to.
   * @param table a Table referencing the BigQuery table to be cached.
   */
  private void setCachedTable(TableId tableId, Table table) {
    String tableName = tableId.toString();
    tables.put(tableName, table);
  }

  private Table getBigQueryTable(TableId tableId) {
    Table table = getCachedTable(tableId);

    // Checks that table existed in tables map
    // If not pull table from API
    // TODO: we need logic to invalidate table caches?
    if (table == null) {
      LOG.info("Pulling Table from API: {}", tableId.toString());
      table = bigquery.getTable(tableId);
    }
    // Check that table exists, if not create empty table
    // the empty table will have columns automapped during updateBigQueryTable()
    if (table == null) {
      LOG.info("Creating Table: {}", tableId.toString());
      table = createBigQueryTable(tableId);
    }
    setCachedTable(tableId, table);

    return table;
  }

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
      // if we update a nested field, the parent is already in the list
      // but there can only be one
      for (Field existingField : tableFields) {
        if (existingField.getName().equals(field.getName())) {
          fieldList.remove(existingField);
        }
      }
      fieldList.add(field);
      LOG.info(field.toString());
    }

    Schema newSchema = Schema.of(fieldList);
    Table updatedTable =
        table.toBuilder().setDefinition(StandardTableDefinition.of(newSchema)).build().update();

    tables.put(tableName, updatedTable);
  }

  private FieldList createNestedFields(LinkedHashMap obj) {
    List<Field> fl = new ArrayList();
    Field.Mode fieldMode = Field.Mode.NULLABLE;
    LegacySQLTypeName sqlType;

    Set<String> objKeys = obj.keySet();

    for (String objKey : objKeys) {
     Object cellItem = obj.get(objKey);
     LOG.info("objKey: {}", objKey.toString());
     LOG.info("objValue: {}", cellItem.toString());

     Class cls = cellItem.getClass();
     LOG.info("the type of cellItem is: {}", cls.getName());

    if (cellItem instanceof List)
    {
      LOG.debug("Setting field mode to REPEATED");
      fieldMode = Field.Mode.REPEATED;
      //unpack first element so we can check if it is also an object
      List cellList = (List) cellItem;
      if (!cellList.isEmpty()) {
        cellItem = cellList.get(0);
      }
    }

    sqlType = getSQLType(cellItem);

    if (sqlType.equals(LegacySQLTypeName.RECORD)) {
      LOG.info("We have found an object within an object!");
      LinkedHashMap lhm = (LinkedHashMap) cellItem;
      FieldList sfl = createNestedFields(lhm);
      LOG.info(sfl.toString());
      fl.add(Field.newBuilder(objKey, sqlType, sfl).setMode(fieldMode).build());
    } else {
    fl.add(Field.newBuilder(objKey, sqlType).build());
    }
  }

    return FieldList.of(fl);
  }

  private LegacySQLTypeName getSQLType(Object item) {
    if (item instanceof Integer) {
      return LegacySQLTypeName.INTEGER;
    } else if (item instanceof Boolean) {
      return LegacySQLTypeName.BOOLEAN;
    } else if (item instanceof Double) {
      return LegacySQLTypeName.FLOAT;
    } else if (item instanceof LinkedHashMap) {
      return LegacySQLTypeName.RECORD;
    } else {
      // we could try to parse out a date before defaulting to string
      return LegacySQLTypeName.STRING;
    }
  }

  private Boolean addNewTableField(TableId tableId, TableRow row, String rowKey,
      List<Field> newFieldList, Map<String, LegacySQLTypeName> inputSchema) {
    // Call Get Schema and Extract New Field Type
    Field newField;
    Field.Mode fieldMode = Field.Mode.NULLABLE;
    LegacySQLTypeName sqlType = LegacySQLTypeName.STRING;
    Object cellItem = row.get(rowKey);

    // Set data type if a schema is provided
    if (inputSchema.containsKey(rowKey)) {
      sqlType = inputSchema.get(rowKey);
    } else {
        sqlType = getSQLType(cellItem);
    }

    // Test cell for List type and set field mode
    if (cellItem instanceof List) {
      LOG.debug("Setting field mode to REPEATED");
      fieldMode = Field.Mode.REPEATED;
      //unpack first element so we can check if it is also an object
      List cellList = (List) cellItem;
      if (!cellList.isEmpty()) {
        cellItem = cellList.get(0);
      }
    }

    // Test cell for object and construct subFieldList
    if (sqlType.equals(LegacySQLTypeName.RECORD)) {
      LOG.info("We have found an object!");
      LinkedHashMap lhm = (LinkedHashMap) cellItem;
      FieldList sfl = createNestedFields(lhm);
      LOG.info(sfl.toString());
      newField = Field.newBuilder(rowKey, sqlType, sfl).setMode(fieldMode).build();
    } else {
      newField = Field.newBuilder(rowKey, sqlType).setMode(fieldMode).build();
    }

    newFieldList.add(newField);
    LOG.info("newFieldList: {}", newFieldList.toString());

    // Currently we always add new fields for each call
    // TODO: add an option to ignore new field and why boolean?
    return true;
  }
}
