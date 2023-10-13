/*
 * Copyright (C) 2022 Google LLC
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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Partition;
import com.google.cloud.teleport.v2.clients.DataplexClient;
import com.google.cloud.teleport.v2.clients.DataplexClientFactory;
import com.google.cloud.teleport.v2.values.DataplexPartitionMetadata;
import java.io.IOException;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Updates Dataplex Metadata for the files generated as a result of JDBC ingestion. */
public class DataplexJdbcIngestionUpdateMetadata
    extends PTransform<PCollection<DataplexPartitionMetadata>, PCollection<Void>> {

  private static final Logger LOG =
      LoggerFactory.getLogger(DataplexJdbcIngestionUpdateMetadata.class);

  private final DataplexClientFactory dataplexClientFactory;
  private final String outputEntityName;

  public DataplexJdbcIngestionUpdateMetadata(
      DataplexClientFactory dataplexClientFactory, String outputEntityName) {
    this.dataplexClientFactory = dataplexClientFactory;
    this.outputEntityName = outputEntityName;
  }

  @Override
  public PCollection<Void> expand(PCollection<DataplexPartitionMetadata> input) {
    return input.apply("UpdateDataplex", ParDo.of(new CallDataplexFn()));
  }

  private class CallDataplexFn extends DoFn<DataplexPartitionMetadata, Void> {
    private transient DataplexClient dataplex;

    @Setup
    public void setup() throws IOException {
      dataplex = dataplexClientFactory.createClient();
    }

    @ProcessElement
    public void processElement(@Element DataplexPartitionMetadata input) {
      // All entities should've been pre-created and updated with the latest schema,
      // only need to create or update partitions here.

      GoogleCloudDataplexV1Partition createdPartition;
      try {
        createdPartition =
            dataplex.createOrUpdatePartition(outputEntityName, input.toDataplexPartition());
        checkNotNull(createdPartition, "Got null in response to create partition.");
      } catch (Exception e) {
        throw new IllegalStateException(
            String.format(
                "Error creating partition for entity %s with values: %s.",
                outputEntityName, input.getValues()),
            e);
      }
      LOG.info("Created partition {} for entity {}.", createdPartition.getName(), outputEntityName);
    }
  }
}
