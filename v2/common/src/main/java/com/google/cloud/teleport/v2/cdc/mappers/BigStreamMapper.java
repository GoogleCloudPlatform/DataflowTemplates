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
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters;
import com.google.cloud.teleport.v2.utils.DataStream;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BigStreamMapper extends the BigQueryMapper to support the BigStream API.
 */
public class BigStreamMapper extends BigQueryMapper<TableRow, KV<TableId, TableRow>> {

  private static final Logger LOG = LoggerFactory.getLogger(BigStreamMapper.class);

  private String datasetNameTemplate;
  private String tableNameTemplate;
  private DataStream datastream;

  public BigStreamMapper(String projectId,
                         String datasetNameTemplate,
                         String tableNameTemplate) {
      super(projectId);

      this.datasetNameTemplate = datasetNameTemplate;
      this.tableNameTemplate = tableNameTemplate;
  }

  public void reloadBigStream() {
    if (datastream == null) {
      this.datastream = new DataStream(this.getProjectId());
    }
  }

  @Override
  public TableId getTableId(TableRow input) {
    String datasetName = BigQueryConverters.formatStringTemplate(datasetNameTemplate, input);
    String tableName = BigQueryConverters.formatStringTemplate(tableNameTemplate, input);

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

    return KV.of(tableId, tableRow);
  }
  /* Return a HashMap with the Column->Column Type Mapping required from the source 
      Implementing getSchema will allow the mapper class to support your desired format
  */
  @Override
  public Map<String, LegacySQLTypeName> getInputSchema(TableRow input) {
    // Init or Reload BigStream Client when required
    reloadBigStream();

    String streamName = "";
    String objectName = "";
    Map<String, String> datastreamSchema = this.datastream.getObjectSchema(streamName, objectName);
    // TODO convert AVRO types to Legacy SQL Types
    return new HashMap<String, LegacySQLTypeName>();
  }
}
