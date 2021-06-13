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

package com.google.cloud.teleport.templates;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;
import com.google.cloud.teleport.templates.common.KinesisConverters.KinesisStreamOptions;
import com.google.cloud.teleport.templates.common.KinesisConverters.KinesisPartitionKeyOptions;
import com.google.cloud.teleport.templates.common.KinesisConverters.KinesisAccessKeyOptions;
import com.google.cloud.teleport.templates.common.KinesisConverters.KinesisSecretKeyOptions;
import com.google.cloud.teleport.templates.common.KinesisConverters.KinesisRegionOptions;
import com.google.cloud.teleport.templates.common.PubsubConverters.PubsubReadSubscriptionOptions;
import com.google.auto.value.AutoValue;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.Arrays;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.io.kinesis.KinesisIO;
import com.amazonaws.regions.Regions;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An template that copies messages from one Pubsub subscription to another Pubsub topic. */
public class PubsubToKinesis {

  /**
   * Main entry point for executing the pipeline.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {

    // Parse the user options passed from the command-line
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    options.setStreaming(true);

    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  public static PipelineResult run(Options options) {
    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply(
            "Read PubSub Events",
            PubsubIO.readMessagesWithAttributes().fromSubscription(options.getInputSubscription()))
        .apply(
            "Prepare Kinesis input records",
            ParDo.of(new ConvertToBytes()))
        .apply(
            "Write Kinesis Events",
            KinesisIO.write()
            .withStreamName(options.getAwsKinesisStream().toString())
            .withPartitionKey(options.getAwsKinesisPartitionKey().toString())
            .withAWSClientsProvider(
              options.getAwsAccessKey().toString(),
              options.getAwsSecretKey().toString(),
              Regions.fromName(options.getAwsKinesisRegion().toString()
              )
            ));
    // Execute the pipeline and return the result.
    return pipeline.run();
  }

  /** Prepare Kinesis input records */
  private static class ConvertToBytes extends DoFn<PubsubMessage, byte[]> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(String.valueOf(c.element()).getBytes(StandardCharsets.UTF_8));
    }
  }

  /**
   * The {@link PubSubToSplunkOptions} class provides the custom options passed by the executor at
   * the command line.
   */
  public interface KinesisOptions extends KinesisStreamOptions, KinesisPartitionKeyOptions, KinesisAccessKeyOptions, KinesisSecretKeyOptions, KinesisRegionOptions {}
  public interface PubSubOptions extends PubsubReadSubscriptionOptions {}
  public interface Options extends StreamingOptions, KinesisOptions, PubSubOptions {}
}
