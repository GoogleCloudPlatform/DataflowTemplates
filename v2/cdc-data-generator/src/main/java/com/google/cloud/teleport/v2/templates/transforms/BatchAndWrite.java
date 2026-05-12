/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.templates.transforms;

import com.google.cloud.teleport.v2.templates.CdcDataGeneratorOptions.SinkType;
import com.google.cloud.teleport.v2.templates.dofn.BatchAndWriteFn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.GeneratedRecord;
import com.google.cloud.teleport.v2.templates.utils.FailureRecord;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * {@link PTransform} that takes a stream of partially-generated keyed rows, completes them,
 * recursively materialises child rows from the schema DAG, batches them by {@code (table, shard,
 * op)}, and writes to the sink configured by {@code sinkType}/{@code sinkOptionsPath}.
 *
 * <p>Output is a {@code PCollection<String>} of JSON-encoded {@link FailureRecord}s — one per row
 * that could not be generated or that the sink rejected. The output is empty when no failures
 * occur.
 */
public class BatchAndWrite
    extends PTransform<PCollection<KV<Integer, GeneratedRecord>>, PCollection<String>> {

  private final SinkType sinkType;
  private final String sinkOptionsPath;
  private final Integer batchSize;
  private final Integer jdbcPoolSize;
  private final Integer updateInterval;
  private final Integer deleteInterval;
  private final PCollectionView<DataGeneratorSchema> schemaView;

  /**
   * @param sinkType which sink writer the underlying {@link BatchAndWriteFn} should instantiate
   * @param sinkOptionsPath path/URI to the sink-specific configuration document
   * @param batchSize maximum rows buffered per {@code (table, shard, op)} before flush;
   * @param jdbcPoolSize connection pool size limit per MySQL shard node
   * @param updateInterval custom UPDATE interval in seconds for lifecycle events
   * @param deleteInterval custom trailing DELETE interval in seconds for lifecycle events
   * @param schemaView side input carrying the {@link DataGeneratorSchema}
   */
  public BatchAndWrite(
      SinkType sinkType,
      String sinkOptionsPath,
      Integer batchSize,
      Integer jdbcPoolSize,
      Integer updateInterval,
      Integer deleteInterval,
      PCollectionView<DataGeneratorSchema> schemaView) {
    this.sinkType = sinkType;
    this.sinkOptionsPath = sinkOptionsPath;
    this.batchSize = batchSize;
    this.jdbcPoolSize = jdbcPoolSize;
    this.updateInterval = updateInterval;
    this.deleteInterval = deleteInterval;
    this.schemaView = schemaView;
  }

  @Override
  public PCollection<String> expand(PCollection<KV<Integer, GeneratedRecord>> input) {
    return input
        .apply(
            "BatchAndWriteFn",
            ParDo.of(
                    new BatchAndWriteFn(
                        sinkType,
                        sinkOptionsPath,
                        batchSize,
                        jdbcPoolSize,
                        updateInterval,
                        deleteInterval,
                        schemaView))
                .withSideInputs(schemaView))
        .setCoder(StringUtf8Coder.of());
  }
}
