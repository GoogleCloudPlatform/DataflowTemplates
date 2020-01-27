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

import com.google.cloud.teleport.templates.common.BigQueryConverters;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.generic.GenericRecord;
// import org.apache.avro.Schema; // if needed we need to figure out the duplicate here
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.options.ValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: Class comments are required
/*
 *
 * BigQueryMappers contains different versions of a generic BigQueryMapper class
 *    The BigQueryMapper can be easily extended by overriding:
 *    - public TableId getTableId(InputT input)
 *    - public TableRow getTableRow(InputT input)
 *    - public OutputT getOutputObject(InputT input)
 *    - public Map<String, LegacySQLTypeName> getInputSchema(InputT input)
 *
 * 
 * BigQueryMapper Versions can be used via helper functions
 *  buildBigQueryTableMapper(ValueProvider<String> datasetProvider, ValueProvider<String> tableNameProvider)
 *    - This expects table name to be provided and handles schema changes for a TableRow object
 *
 *  buildBigQueryDynamicTableMapper
 *    - Expect to process a KV<TableId, TableRow> and the class will manage the schema
 *    - for each table supplied in the stream
 *
 *  buildBigQueryGenericRecordMapper
 *    - TODO
 *
 */
public class BigQueryMappers {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryMappers.class);

  /*** Section 1: Functions to build Mapper Class for each different required input ***/
  /* Build Static TableRow BigQuery Mapper */
  public static PTransform<PCollection<TableRow>, PCollection<TableRow>>
          buildBigQueryTableMapper(ValueProvider<String> datasetProvider, ValueProvider<String> tableNameProvider) {
    return new BigQueryTableMapper(datasetProvider, tableNameProvider);
  }

  /* Build Dynamic TableRow BigQuery Mapper */
  public static PTransform<PCollection<KV<TableId, TableRow>>, PCollection<KV<TableId, TableRow>>>
          buildBigQueryDynamicTableMapper() {
    return new BigQueryDynamicTableMapper();
  }

  /* Build Static GenericRecord BigQuery Mapper */
  public static PTransform<PCollection<GenericRecord>, PCollection<KV<TableId, TableRow>>>
          buildBigQueryGenericRecordMapper(ValueProvider<String> datasetProvider, ValueProvider<String> tableNameProvider) {
    return new BigQueryGenericRecordMapper(datasetProvider, tableNameProvider);
  }

  /*** Section 2: Extended Mapper Classes implemented for different input types ***/
  /* Dynamic TableRow BigQuery Mapper */
  public static class BigQueryTableMapper
      extends BigQueryMapper<TableRow, TableRow> {

    private ValueProvider<String> datasetProvider;
    private ValueProvider<String> tableNameProvider;

    public BigQueryTableMapper(ValueProvider<String> datasetProvider, ValueProvider<String> tableNameProvider) {
      super();

      this.datasetProvider = datasetProvider;
      this.tableNameProvider = tableNameProvider;
    }

    @Override
    public TableId getTableId(TableRow input) {
      return TableId.of(datasetProvider.get(), tableNameProvider.get());
    }
    @Override
    public TableRow getTableRow(TableRow input) {
      return input;
    }
    @Override
    public TableRow getOutputObject(TableRow input) {
      return input;
    }
    /* Return a HashMap with the Column->Column Type Mapping required from the source 
        Implementing getSchema will allow the mapper class to support your desired format
    */
    @Override
    public Map<String, LegacySQLTypeName> getInputSchema(TableRow input) {
      return new HashMap<String, LegacySQLTypeName>();
    }
  }

  /* Dynamic TableRow BigQuery Mapper */
  public static class BigQueryDynamicTableMapper
      extends BigQueryMapper<KV<TableId, TableRow>, KV<TableId, TableRow>> {

    @Override
    public TableId getTableId(KV<TableId, TableRow> input) {
      return input.getKey();
    }
    @Override
    public TableRow getTableRow(KV<TableId, TableRow> input) {
      return input.getValue();
    }
    @Override
    public KV<TableId, TableRow> getOutputObject(KV<TableId, TableRow> input) {
      return input;
    }
    /* Return a HashMap with the Column->Column Type Mapping required from the source 
        Implementing getSchema will allow the mapper class to support your desired format
    */
    @Override
    public Map<String, LegacySQLTypeName> getInputSchema(KV<TableId, TableRow> input) {
      return new HashMap<String, LegacySQLTypeName>();
    }
  }

  /* Static GenericRecord BigQuery Mapper */
  public static class BigQueryGenericRecordMapper
      extends BigQueryMapper<GenericRecord, KV<TableId, TableRow>> {

    private ValueProvider<String> datasetProvider;
    private ValueProvider<String> tableNameProvider;

    public BigQueryGenericRecordMapper(ValueProvider<String> datasetProvider, ValueProvider<String> tableNameProvider) {
      super();

      this.datasetProvider = datasetProvider;
      this.tableNameProvider = tableNameProvider;
    }

    @Override
    public TableId getTableId(GenericRecord input) {
      return TableId.of(datasetProvider.get(), tableNameProvider.get());
    }
    @Override
    public TableRow getTableRow(GenericRecord input) {
      String inputString = input.toString();
      return BigQueryConverters.convertJsonToTableRow(inputString);
    }
    @Override
    public KV<TableId, TableRow> getOutputObject(GenericRecord input) {
      TableId tableId = getTableId(input);
      TableRow tableRow = getTableRow(input);

      return KV.of(tableId, tableRow);
    }
    /* Return a HashMap with the Column->Column Type Mapping required from the source 
        Implementing getSchema will allow the mapper class to support your desired format
    */
    @Override
    public Map<String, LegacySQLTypeName> getInputSchema(GenericRecord input) {
      return new HashMap<String, LegacySQLTypeName>();
    }
  }  

  /*** Section 3: Generalized Parent Class to Enable Easy Extension ***/
  public static class BigQueryMapper<InputT, OutputT>
      extends PTransform<PCollection<InputT>, PCollection<OutputT>> {

    private BigQuery bigquery;
    private Map<String, Table> tables = new HashMap<String, Table>();

    public BigQueryMapper() {}

    public TableId getTableId(InputT input) {return null;}
    public TableRow getTableRow(InputT input) {return null;}
    public OutputT getOutputObject(InputT input) {return null;}

    /* Return a HashMap with the Column->Column Type Mapping required from the source 
        Implementing getSchema will allow the mapper class to support your desired format
    */
    public Map<String, LegacySQLTypeName> getInputSchema(InputT input) {return null;}

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
                  Map<String, LegacySQLTypeName> inputSchema = getInputSchema(input);
                  // TODO the Dynamic converter needs to use the tableId object rather than a string

                  updateTableIfRequired(tableId, row, inputSchema);
                  return getOutputObject(input);
                  // return KV.of(tableId, row);
                }
              }));
    }

    private void updateTableIfRequired(TableId tableId, TableRow row, Map<String, LegacySQLTypeName> inputSchema) {
      // Ensure Instance of BigQuery Exists
      if (this.bigquery == null) {
        this.bigquery =
            BigQueryOptions.newBuilder()
                .setProjectId("alooma-pipeline-2019-05-22-1") //TODO should not be hardcoded
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
      TableDefinition tableDefinition = StandardTableDefinition.of(schema);
      TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
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
}