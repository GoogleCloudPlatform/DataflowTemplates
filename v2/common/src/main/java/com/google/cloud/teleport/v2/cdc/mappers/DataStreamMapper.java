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
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters;
import com.google.cloud.teleport.v2.utils.DataStreamClient;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DataStreamMapper extends the BigQueryMapper to support the DataStream API.
 */
public class DataStreamMapper extends BigQueryMapper<TableRow, KV<TableId, TableRow>> {

  private static final Logger LOG = LoggerFactory.getLogger(DataStreamMapper.class);
  private static final int MAX_RETRIES = 5;
  private String datasetNameTemplate;
  private String tableNameTemplate;
  private DataStreamClient datastream;

  public DataStreamMapper(GcpOptions options,
                          String projectId,
                          String datasetNameTemplate,
                          String tableNameTemplate) {
      super(projectId);

      this.datasetNameTemplate = datasetNameTemplate;
      this.tableNameTemplate = tableNameTemplate;

      try {
        this.datastream = new DataStreamClient(options.getGcpCredential());
      } catch (IOException e) {
        LOG.error("IOException Occurred: DataStreamClient failed initialization.", e);
        this.datastream = null;
      }
  }

  public DataStreamMapper withDataStreamRootUrl(String url) {
    if (this.datastream != null) {
      this.datastream.setRootUrl(url);
    }

    return this;
  }

  @Override
  public TableId getTableId(TableRow input) {
    String datasetName = BigQueryConverters.formatStringTemplate(datasetNameTemplate, input);
    String tableName =
        BigQueryConverters.formatStringTemplate(tableNameTemplate, input)
        .replaceAll("\\$", "_");

    return TableId.of(datasetName, tableName);
  }

  @Override
  public TableRow getTableRow(TableRow input) {
    return input;
  }

  /* Return a HashMap with the Column->Column Type Mapping required from the source 
      Implementing getSchema will allow the mapper class to support your desired format
  */
  @Override
  public Map<String, StandardSQLTypeName> getInputSchema(TableId tableId, TableRow row) {
    String streamName = (String) row.get("_metadata_stream");
    String schemaName = (String) row.get("_metadata_schema");
    String tableName = (String) row.get("_metadata_table");

    Map<String, StandardSQLTypeName> datastreamSchema =
      new HashMap<String, StandardSQLTypeName>();
    try {
      datastreamSchema = getObjectSchema(streamName, schemaName, tableName, MAX_RETRIES);
    } catch (IOException e) {
      LOG.error("IOException Occurred: Failed to Retrieve Schema: {} {}.{} : {}",
        streamName, schemaName, tableName, e.toString());
    }
    return datastreamSchema;
  }

  @Override
  public KV<TableId, TableRow> getOutputObject(TableRow input) {
    TableId tableId = getTableId(input);
    TableRow tableRow = getTableRow(input);
    TableRow cleanedTableRow = getCleanedTableRow(tableId, tableRow);

    return KV.of(tableId, cleanedTableRow);
  }

  private Map<String, StandardSQLTypeName> getObjectSchema(
      String streamName, String schemaName, String tableName, int retriesRemaining)
      throws IOException {
    try {
      return this.datastream.getObjectSchema(streamName, schemaName, tableName);
    } catch (IOException e) {
      if (retriesRemaining > 0) {
        int sleepSecs = (int) Math.pow(MAX_RETRIES - retriesRemaining + 1, 2) * 10;
        LOG.info(
            "IOException Occurred, will retry after {} seconds: Failed to Retrieve Schema: {} {}.{} : {}",
            sleepSecs, streamName, schemaName, tableName, e.toString());
        try {
          Thread.sleep(sleepSecs * 1000);
        } catch (InterruptedException i) {
          throw e;
        }
        return getObjectSchema(streamName, schemaName, tableName, retriesRemaining - 1);
      }
      throw e;
    }
  }
}
