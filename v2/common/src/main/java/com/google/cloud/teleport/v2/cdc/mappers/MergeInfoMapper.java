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
import com.google.cloud.teleport.v2.cdc.merge.MergeInfo;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters;
import com.google.common.collect.ImmutableList;
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

  private String stagingDataset;
  private String stagingTable;
  private String replicaDataset;
  private String replicaTable;

  public MergeInfoMapper(
      String stagingDataset, String stagingTable,
      String replicaDataset, String replicaTable) {
    this.stagingDataset = stagingDataset;
    this.stagingTable = stagingTable;

    this.replicaDataset = replicaDataset;
    this.replicaTable = replicaTable;
  }

  @Override
  public PCollection<MergeInfo> expand(PCollection<KV<TableId, TableRow>> input) {
    return input.apply(
        MapElements.into(TypeDescriptor.of(MergeInfo.class))
            .via(
                element -> {
                  return MergeInfo.create(
                      METADATA_TIMESTAMP, // TODO should be list pulled from Datastream API
                      METADATA_DELETED,
                      String.format("%s.%s",
                          // Staging Table // TODO these should possibly be passed separately
                          BigQueryConverters
                              .formatStringTemplate(stagingDataset, element.getValue()),
                          BigQueryConverters
                              .formatStringTemplate(stagingTable, element.getValue())),
                      String.format("%s.%s", // Replica Table
                          BigQueryConverters
                              .formatStringTemplate(replicaDataset, element.getValue()),
                          BigQueryConverters
                              .formatStringTemplate(replicaTable, element.getValue())),
                      ImmutableList.copyOf(element.getValue().keySet()),
                      ImmutableList.of("ID"));
                }));
  }
}
