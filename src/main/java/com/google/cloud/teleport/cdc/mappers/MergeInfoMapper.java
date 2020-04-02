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
package com.google.cloud.teleport.cdc.mappers;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.cdc.merge.MergeInfo;
import com.google.cloud.teleport.templates.common.BigQueryConverters;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.options.ValueProvider;
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

  private ValueProvider<String> stagingDataset;
  private ValueProvider<String> stagingTable;
  private ValueProvider<String> replicaDataset;
  private ValueProvider<String> replicaTable;

  public MergeInfoMapper(
      ValueProvider<String> stagingDataset, ValueProvider<String> stagingTable,
      ValueProvider<String> replicaDataset, ValueProvider<String> replicaTable) {
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
                              .formatStringTemplate(stagingDataset.get(), element.getValue()),
                          BigQueryConverters
                              .formatStringTemplate(stagingTable.get(), element.getValue())),
                      String.format("%s.%s", // Replica Table
                          BigQueryConverters
                              .formatStringTemplate(replicaDataset.get(), element.getValue()),
                          BigQueryConverters
                              .formatStringTemplate(replicaTable.get(), element.getValue())),
                      ImmutableList.copyOf(element.getValue().keySet()),
                      ImmutableList.of("ID"));
                }));
  }
}
