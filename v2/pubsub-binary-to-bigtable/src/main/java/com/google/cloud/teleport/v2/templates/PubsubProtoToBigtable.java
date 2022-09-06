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
package com.google.cloud.teleport.v2.templates;

import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import com.google.cloud.teleport.v2.options.BigtableCommonOptions.WriteOptions;
import com.google.cloud.teleport.v2.options.PubsubCommonOptions.ReadSubscriptionOptions;
import com.google.cloud.teleport.v2.options.PubsubCommonOptions.WriteTopicOptions;
import com.google.cloud.teleport.v2.proto.BigtableRow;
import com.google.cloud.teleport.v2.transforms.ProtoToBigtableMutation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;

/**
 * A Dataflow pipeline to stream <a
 * href="https://developers.google.com/protocol-buffers">Protobuf</a> records from Pub/Sub into a
 * Bigtable table.
 *
 * <p>If the pipeline fails to acknowledge the packet received, PubSubIO will forward this
 * unprocessed packet to a dead-letter topic.
 */
public final class PubsubProtoToBigtable {

  /**
   * Validates input flags and executes the Dataflow pipeline.
   *
   * @param args command line arguments to the pipeline
   */
  public static void main(String[] args) {
    PubsubProtoToBigtableOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(PubsubProtoToBigtableOptions.class);

    run(options);
  }

  /**
   * Provides custom {@link org.apache.beam.sdk.options.PipelineOptions} required to execute the
   * {@link PubsubProtoToBigtable} pipeline.
   */
  public interface PubsubProtoToBigtableOptions
      extends ReadSubscriptionOptions, WriteTopicOptions, WriteOptions {}

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options execution parameters to the pipeline
   * @return result of the pipeline execution as a {@link PipelineResult}
   */
  private static PipelineResult run(PubsubProtoToBigtableOptions options) {

    // Create the pipeline.
    Pipeline pipeline = Pipeline.create(options);

    // Create Bigtable configuration
    CloudBigtableTableConfiguration bigtableTableConfig =
        new CloudBigtableTableConfiguration.Builder()
            .withProjectId(options.getBigtableWriteProjectId())
            .withInstanceId(options.getBigtableWriteInstanceId())
            .withAppProfileId(options.getBigtableWriteAppProfile())
            .withTableId(options.getBigtableWriteTableId())
            .build();

    pipeline
        .apply(
            "Read Proto records from Pub/Sub Subscription",
            PubsubIO.readProtos(BigtableRow.class)
                .fromSubscription(options.getInputSubscription())
                .withDeadLetterTopic(options.getOutputTopic()))
        .apply("Transform to Bigtable Mutation", ParDo.of(new ProtoToBigtableMutation()))
        .apply("Write To Bigtable", CloudBigtableIO.writeToTable(bigtableTableConfig));

    // Execute the pipeline and return the result.
    return pipeline.run();
  }
}
