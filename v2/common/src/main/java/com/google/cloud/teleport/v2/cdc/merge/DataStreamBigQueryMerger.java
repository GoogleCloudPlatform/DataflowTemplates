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
package com.google.cloud.teleport.v2.cdc.merge;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.v2.cdc.mappers.MergeInfoMapper;
import com.google.cloud.teleport.v2.utils.DataStreamClient;
import java.io.IOException;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class BigQueryMerger. */
public class DataStreamBigQueryMerger
    extends PTransform<PCollection<KV<TableId, TableRow>>, PCollection<Void>> {

  private static final Logger LOG = LoggerFactory.getLogger(DataStreamBigQueryMerger.class);

  private DataStreamClient dataStreamClient;
  private String projectId;
  private String stagingDataset;
  private String stagingTable;
  private String replicaDataset;
  private String replicaTable;
  private Duration windowDuration;
  private BigQuery bigQueryClient;
  private MergeConfiguration mergeConfiguration;

  public DataStreamBigQueryMerger(
      GcpOptions options,
      String projectId,
      String stagingDataset,
      String stagingTable,
      String replicaDataset,
      String replicaTable,
      Duration windowDuration,
      BigQuery bigQueryClient,
      MergeConfiguration mergeConfiguration) {
    this.projectId = projectId == null ? options.getProject() : projectId;
    this.stagingDataset = stagingDataset;
    this.stagingTable = stagingTable;

    this.replicaDataset = replicaDataset;
    this.replicaTable = replicaTable;

    this.windowDuration = windowDuration;
    this.bigQueryClient = bigQueryClient;
    this.mergeConfiguration = mergeConfiguration;

    try {
      this.dataStreamClient = new DataStreamClient(options.getGcpCredential());
    } catch (IOException e) {
      LOG.error("IOException Occurred: DataStreamClient failed initialization.");
      this.dataStreamClient = null;
    }
  }

  public DataStreamBigQueryMerger withDataStreamRootUrl(String url) {
    if (this.dataStreamClient != null) {
      this.dataStreamClient.setRootUrl(url);
    }

    return this;
  }

  @Override
  public PCollection<Void> expand(PCollection<KV<TableId, TableRow>> input) {
    final MergeStatementBuilder mergeBuilder = new MergeStatementBuilder(mergeConfiguration);

    // Group each batch of rows into a single table object for merge
    PCollection<KV<TableId, TableRow>> groupedByTable =
        input
            .apply(
                MapElements.into(
                        TypeDescriptors.kvs(
                            TypeDescriptors.strings(),
                            TypeDescriptors.kvs(
                                TypeDescriptor.of(TableId.class),
                                TypeDescriptor.of(TableRow.class))))
                    .via(tableInfo -> KV.of(tableInfo.getKey().toString(), tableInfo)))
            .apply(
                new BigQueryMerger.TriggerPerKeyOnFixedIntervals<String, KV<TableId, TableRow>>(
                    windowDuration))
            .apply(Values.create());

    // Create MergeInfo objects using DataStream APIs
    PCollection<MergeInfo> mergeInfoRecords =
        groupedByTable.apply(
            "Create MergeInfo Objects",
            new MergeInfoMapper(
                this.dataStreamClient,
                this.projectId,
                this.stagingDataset,
                this.stagingTable,
                this.replicaDataset,
                this.replicaTable));

    // Excute Merge Statement
    return BigQueryMerger.expandExecuteMerge(mergeInfoRecords, mergeConfiguration, bigQueryClient);
  }
}
