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
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.v2.cdc.merge.MergeInfo;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters;
import com.google.cloud.teleport.v2.utils.CacheUtils.BigQueryTableCache;
import com.google.cloud.teleport.v2.utils.CacheUtils.DataStreamPkCache;
import com.google.cloud.teleport.v2.utils.DataStreamClient;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class {@link MergeInfoMapper}.
 */
public class MergeInfoMapper
    extends PTransform<PCollection<KV<TableId, TableRow>>, PCollection<MergeInfo>> {

  public static final String METADATA_TIMESTAMP = "_metadata_timestamp";
  public static final String METADATA_DELETED = "_metadata_deleted";
  public static final String METADATA_REPLICA_TABLE = "_metadata_table";

  private static final Logger LOG = LoggerFactory.getLogger(MergeInfoMapper.class);
  private DataStreamClient dataStreamClient;
  private String stagingDataset;
  private String stagingTable;
  private String replicaDataset;
  private String replicaTable;
  private BigQueryTableCache tableCache;
  private DataStreamPkCache pkCache;

  public MergeInfoMapper(
      GcpOptions options,
      String stagingDataset, String stagingTable,
      String replicaDataset, String replicaTable) {
    this.stagingDataset = stagingDataset;
    this.stagingTable = stagingTable;

    this.replicaDataset = replicaDataset;
    this.replicaTable = replicaTable;

    try {
        this.dataStreamClient = new DataStreamClient(options.getGcpCredential());
      } catch (IOException e) {
        LOG.error("IOException Occurred: DataStreamClient failed initialization.");
        this.dataStreamClient = null;
      }
  }

  public BigQueryTableCache getTableCache() {
    if (this.tableCache == null) {
      BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
      this.tableCache = new BigQueryTableCache(bigquery);
    }

    return this.tableCache;
  }

  public DataStreamPkCache getPkCache() {
    if (this.pkCache == null) {
      this.pkCache = new DataStreamPkCache(this.dataStreamClient);
    }

    return this.pkCache;
  }

  @Override
  public PCollection<MergeInfo> expand(PCollection<KV<TableId, TableRow>> input) {
    return input.apply(
        MapElements.into(TypeDescriptor.of(MergeInfo.class))
            .via(
                element -> {
                  TableId tableId = element.getKey();
                  TableRow row = element.getValue();
                  String streamName = (String) row.get("_metadata_stream");
                  String schemaName = (String) row.get("_metadata_schema");
                  String tableName = (String) row.get("_metadata_table");
                  List allPkFields = getPrimaryKeys(streamName, schemaName, tableName);

                  return MergeInfo.create(
                      METADATA_TIMESTAMP, // TODO should be list pulled from Datastream API
                      METADATA_DELETED,
                      String.format("%s.%s",
                          // Staging Table // TODO these should possibly be passed separately
                          BigQueryConverters
                              .formatStringTemplate(stagingDataset, row),
                          BigQueryConverters
                              .formatStringTemplate(stagingTable, row)),
                      String.format("%s.%s", // Replica Table
                          BigQueryConverters
                              .formatStringTemplate(replicaDataset, row),
                          BigQueryConverters
                              .formatStringTemplate(replicaTable, row)),
                      getMergeFields(tableId, row),
                      allPkFields);
                }));
  }

  public List getPrimaryKeys(String streamName, String schemaName, String tableName) {
    List<String> searchKey = ImmutableList.of(streamName, schemaName, tableName);
    List<String> primaryKeys = getPkCache().get(searchKey);
    try {
      primaryKeys = this.dataStreamClient.getPrimaryKeys(streamName, schemaName, tableName);
    } catch (IOException e) {
      LOG.error("IOException: DataStream Discovery on Primary Keys Failed.");
    }
    if (primaryKeys.size() == 0) {
      // TODO when DataStream releases new outputs, change logic here.
      // Possibly move upstream to DataStream Client
      primaryKeys.add("_metadata_row_id");
    }

    // TODO logic should be added to allow primary key append and override.
    return primaryKeys;
  }

  public List<String> getMergeFields(TableId tableId, TableRow row) {
    List<String> mergeFields = new ArrayList<String>();
    Table table = getTableCache().get(tableId);
    FieldList tableFields = table.getDefinition().getSchema().getFields();

    for (Field field : tableFields) {
      mergeFields.add(field.getName());
    }

    return mergeFields;
  }
}
