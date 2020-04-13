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

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.cdc.mappers.BigQueryMapper;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// import org.apache.avro.Schema; // if needed we need to figure out the duplicate here


/**
 * BigQueryMappers contains different versions of a generic BigQueryMapper class.
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
 */
public class BigQueryMappers {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryMappers.class);
  private static ValueProvider<String> projectId;

  public BigQueryMappers(ValueProvider<String> projectId) {
    this.projectId = projectId;
  }

  /*** Section 1: Functions to build Mapper Class for each different required input ***/
  /* Build Static TableRow BigQuery Mapper */
  public static BigQueryMapper<TableRow, KV<TableId, TableRow>>
          buildBigQueryTableMapper(ValueProvider<String> datasetProvider, ValueProvider<String> tableNameProvider) {
    return new BigQueryTableMapper(datasetProvider, tableNameProvider, projectId);
  }

  /* Build Dynamic TableRow BigQuery Mapper */
  // public static PTransform<PCollection<KV<TableId, TableRow>>, PCollection<KV<TableId, TableRow>>>
  public static BigQueryMapper<KV<TableId, TableRow>, KV<TableId, TableRow>>
          buildBigQueryDynamicTableMapper() {
    return new BigQueryDynamicTableMapper(projectId);
  }

  /* Build Static GenericRecord BigQuery Mapper */
  public static BigQueryMapper<GenericRecord, KV<TableId, TableRow>>
      buildBigQueryGenericRecordMapper(
          ValueProvider<String> datasetProvider, ValueProvider<String> tableNameProvider) {
    return new BigQueryGenericRecordMapper(datasetProvider, tableNameProvider, projectId);
  }

  /**
   * Section 2: Extended Mapper Classes implemented for different input types.
   */
  public static class BigQueryTableMapper
      extends BigQueryMapper<TableRow, KV<TableId, TableRow>> {

    private ValueProvider<String> datasetProvider;
    private ValueProvider<String> tableNameProvider;

    public BigQueryTableMapper(
        ValueProvider<String> datasetProvider,
        ValueProvider<String> tableNameProvider,
        ValueProvider<String> projectId) {
      super(projectId);

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
    public KV<TableId, TableRow> getOutputObject(TableRow input) {
      TableId tableId = getTableId(input);
      TableRow tableRow = getTableRow(input);

      return KV.of(tableId, tableRow);
    }
    /* Return a HashMap with the Column->Column Type Mapping required from the source
        Implementing getSchema will allow the mapper class to support your desired format
    */
    @Override
    public Map<String, LegacySQLTypeName> getInputSchema(TableRow input) {
      return new HashMap<String, LegacySQLTypeName>();
    }
  }

  /** Dynamic TableRow BigQuery Mapper.
   */
  public static class BigQueryDynamicTableMapper
      extends BigQueryMapper<KV<TableId, TableRow>, KV<TableId, TableRow>> {

    private BigQueryDynamicTableMapper(ValueProvider<String> projectId) {
      super(projectId);
    }

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

  /**
   * Static GenericRecord BigQuery Mapper.
   */
  public static class BigQueryGenericRecordMapper
      extends BigQueryMapper<GenericRecord, KV<TableId, TableRow>> {

    private ValueProvider<String> datasetProvider;
    private ValueProvider<String> tableNameProvider;

    public BigQueryGenericRecordMapper(
        ValueProvider<String> datasetProvider,
        ValueProvider<String> tableNameProvider,
        ValueProvider<String> projectId) {
      super(projectId);

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
}
