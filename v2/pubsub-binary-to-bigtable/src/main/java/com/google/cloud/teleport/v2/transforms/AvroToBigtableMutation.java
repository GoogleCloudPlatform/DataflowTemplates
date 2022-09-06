/*
 * Copyright (C) 2020 Google LLC
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
package com.google.cloud.teleport.v2.transforms;

import com.google.cloud.teleport.v2.avro.BigtableCell;
import com.google.cloud.teleport.v2.avro.BigtableRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This ParDo converts the payload from received {@link BigtableRow} Avro format into Bigtable
 * {@link Mutation}.
 */
public class AvroToBigtableMutation extends DoFn<BigtableRow, Mutation> {

  private static final Logger LOG = LoggerFactory.getLogger(AvroToBigtableMutation.class);

  public AvroToBigtableMutation() {}

  @ProcessElement
  public void processElement(@Element BigtableRow row, OutputReceiver<Mutation> out) {
    Mutation outputMutation = convert(row);
    if (outputMutation != null) {
      out.output(outputMutation);
    }
  }

  /**
   * Translates {@link BigtableRow} to {@link Mutation}s along with a row key. The mutations are
   * {@link BigtableCell}s that set the value for specified cells with family name, column qualifier
   * and timestamp.
   *
   * @param row {@link BigtableRow} Avro Payload.
   * @return Bigtable Hbase{@link Mutation} object.
   */
  public Mutation convert(final BigtableRow row) {
    try {
      byte[] rowKey = row.getRowKey().array();

      // Create a Mutation object
      Put put = new Put(rowKey);

      for (BigtableCell cell : row.getCells()) {
        // Populate Column in Bigtable row
        put.addColumn(
            cell.getColumnFamily().array(),
            cell.getColumnQualifier().array(),
            cell.getTimestamp(),
            cell.getValue().array());
      }
      return put;
    } catch (Exception e) {
      LOG.error("Failed to create mutation object from received avro payload", e);
    }
    return null;
  }
}
