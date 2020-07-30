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
import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * BigQueryMappers contains different versions of a generic BigQueryMapper class.
 *    The BigQueryMapper can be easily extended by overriding:
 *    - public TableId getTableId(InputT input)
 *    - public TableRow getTableRow(InputT input)
 *    - public OutputT getOutputObject(InputT input)
 *    - public Map<String, LegacySQLTypeName> getInputSchema(InputT input)
 *
 * <p>
 * BigQueryMapper Versions can be used via helper functions
 *  buildBigQueryTableMapper(String datasetProvider, String tableNameProvider)
 *    - This expects table name to be provided and handles schema changes for a TableRow object
 *
 *  buildBigQueryDynamicTableMapper
 *    - Expect to process a KV<TableId, TableRow> and the class will manage the schema
 *    - for each table supplied in the stream
 */
public class BigQueryMappers {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryMappers.class);
  private static String projectId;

  public BigQueryMappers(String projectId) {
    this.projectId = projectId;
  }

  /*** Section 1: Functions to build Mapper Class for each different required input ***/
  /* Build Static TableRow BigQuery Mapper */
  public static BigQueryMapper<TableRow, KV<TableId, TableRow>>
          buildBigQueryTableMapper(String datasetNameTemplate, String tableNameTemplate) {
    return new BigQueryTableMapper(datasetNameTemplate, tableNameTemplate, projectId);
  }

  /* Build Dynamic TableRow BigQuery Mapper */
  public static BigQueryMapper<KV<TableId, TableRow>, KV<TableId, TableRow>>
          buildBigQueryDynamicTableMapper() {
    return new BigQueryDynamicTableMapper(projectId);
  }

  /**
   * Section 2: Extended Mapper Classes implemented for different input types.
   */
  public static class BigQueryTableMapper
      extends BigQueryMapper<TableRow, KV<TableId, TableRow>> {

    private String datasetNameTemplate;
    private String tableNameTemplate;

    public BigQueryTableMapper(
        String datasetNameTemplate,
        String tableNameTemplate,
        String projectId) {
      super(projectId);

      this.datasetNameTemplate = datasetNameTemplate;
      this.tableNameTemplate = tableNameTemplate;
    }

    @Override
    public TableId getTableId(TableRow input) {
      String datasetName = BigQueryConverters.formatStringTemplate(this.datasetNameTemplate, input);
      String tableName = BigQueryConverters.formatStringTemplate(this.tableNameTemplate, input);
      return TableId.of(datasetName, tableName);
    }
    @Override
    public TableRow getTableRow(TableRow input) {
      return input;
    }
    @Override
    public KV<TableId, TableRow> getOutputObject(TableRow input) {
      TableId tableId = getTableId(input);
      TableRow tableRow = getTableRow(input);
      TableRow cleanedTableRow = getCleanedTableRow(tableId, tableRow);

      return KV.of(tableId, cleanedTableRow);
    }
  }

  /** Dynamic TableRow BigQuery Mapper.
   */
  public static class BigQueryDynamicTableMapper
      extends BigQueryMapper<KV<TableId, TableRow>, KV<TableId, TableRow>> {

    private BigQueryDynamicTableMapper(String projectId) {
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
      TableId tableId = getTableId(input);
      TableRow tableRow = getTableRow(input);
      TableRow cleanedTableRow = getCleanedTableRow(tableId, tableRow);

      return KV.of(tableId, cleanedTableRow);
    }
  }
}
