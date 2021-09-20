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
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.v2.cdc.merge.MergeInfo;
import com.google.cloud.teleport.v2.utils.BigQueryTableCache;
import com.google.cloud.teleport.v2.utils.DataStreamClient;
import com.google.cloud.teleport.v2.values.DatastreamRow;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class {@link MergeInfoMapper}. */
public class MergeInfoMapper
    extends PTransform<PCollection<KV<TableId, TableRow>>, PCollection<MergeInfo>> {

  public static final List<String> ORACLE_ORDER_BY_FIELDS =
      Arrays.asList("_metadata_timestamp", "_metadata_scn");
  public static final List<String> MYSQL_ORDER_BY_FIELDS =
      Arrays.asList("_metadata_timestamp", "_metadata_log_file", "_metadata_log_position");
  public static final String METADATA_DELETED = "_metadata_deleted";
  public static final String METADATA_REPLICA_TABLE = "_metadata_table";

  private static final Logger LOG = LoggerFactory.getLogger(MergeInfoMapper.class);
  private DataStreamClient dataStreamClient;
  private String projectId;
  private String stagingDataset;
  private String stagingTable;
  private String replicaDataset;
  private String replicaTable;
  private static BigQueryTableCache tableCache;

  private final Counter foregoneMerges = Metrics.counter(MergeInfoMapper.class, "mergesForegone");

  public MergeInfoMapper(
      DataStreamClient dataStreamClient,
      String projectId,
      String stagingDataset,
      String stagingTable,
      String replicaDataset,
      String replicaTable) {
    this.projectId = projectId;
    this.stagingDataset = stagingDataset;
    this.stagingTable = stagingTable;

    this.replicaDataset = replicaDataset;
    this.replicaTable = replicaTable;

    this.dataStreamClient = dataStreamClient;
  }

  public MergeInfoMapper withDataStreamRootUrl(String url) {
    if (this.dataStreamClient != null) {
      this.dataStreamClient.setRootUrl(url);
    }

    return this;
  }

  private synchronized void setUpTableCache() {
    if (tableCache == null) {
      BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(projectId).build().getService();
      tableCache = new BigQueryTableCache(bigquery);
    }
  }

  public BigQueryTableCache getTableCache() {
    if (this.tableCache == null) {
      setUpTableCache();
    }

    return this.tableCache;
  }

  @Override
  public PCollection<MergeInfo> expand(PCollection<KV<TableId, TableRow>> input) {
    return input.apply(
        FlatMapElements.into(TypeDescriptor.of(MergeInfo.class))
            .via(
                element -> {
                  try {
                    TableId tableId = element.getKey();
                    DatastreamRow row = DatastreamRow.of(element.getValue());

                    String streamName = row.getStreamName();
                    String schemaName = row.getSchemaName();
                    String tableName = row.getTableName();

                    List<String> mergeFields = getMergeFields(tableId);
                    List<String> allPkFields = row.getPrimaryKeys();
                    List<String> allSortFields = row.getSortFields();

                    if (allPkFields.size() == 0) {
                      LOG.warn(
                          "Unable to retrieve primary keys for table {}.{} in stream {}. "
                              + "Not performing merge-based consolidation.",
                          schemaName,
                          tableName,
                          streamName);
                      foregoneMerges.inc();
                      return Lists.newArrayList();
                    } else if (allSortFields.size() == 0) {
                      LOG.warn(
                          "Unable to retrieve sort keys for table {}.{} in stream {}. "
                              + "Not performing merge-based consolidation.",
                          schemaName,
                          tableName,
                          streamName);
                      foregoneMerges.inc();
                    }

                    MergeInfo mergeInfo =
                        MergeInfo.create(
                            allPkFields,
                            allSortFields,
                            METADATA_DELETED,
                            String.format(
                                    "%s.%s.%s", // Staging Table
                                    projectId,
                                    row.formatStringTemplate(stagingDataset),
                                    row.formatStringTemplate(stagingTable))
                                .replaceAll("\\$", "_"),
                            String.format(
                                    "%s.%s.%s", // Replica Table
                                    projectId,
                                    row.formatStringTemplate(replicaDataset),
                                    row.formatStringTemplate(replicaTable))
                                .replaceAll("\\$", "_"),
                            mergeFields);

                    return Lists.newArrayList(mergeInfo);
                  } catch (Exception e) {
                    LOG.error(
                        "Merge Info Failure, skipping merge for: {} -> {}",
                        element.getValue().toString(),
                        e.toString());
                    return Lists.newArrayList();
                  }
                }));
  }

  public List<String> getMergeFields(TableId tableId) {
    List<String> mergeFields = new ArrayList<String>();
    Table table = getTableCache().get(tableId);
    FieldList tableFields = table.getDefinition().getSchema().getFields();

    for (Field field : tableFields) {
      mergeFields.add(field.getName());
    }

    return mergeFields;
  }
}
