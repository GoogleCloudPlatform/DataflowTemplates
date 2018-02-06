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

import java.util.Objects;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An template that copies messages from one Pubsub subscription to another Pubsub topic. */
public class PubsubToPubsub {

  /**
   * Options supported by {@link PubsubToPubsub}.
   *
   * <p>Inherits standard configuration options.
   */
  public interface Options extends PipelineOptions, StreamingOptions {
    @Description(
        "The Cloud Pub/Sub subscription to consume from. "
            + "The name should be in the format of "
            + "projects/<project-id>/subscriptions/<subscription-name>.")
    @Validation.Required
    ValueProvider<String> getInputSubscription();

    void setInputSubscription(ValueProvider<String> inputSubscription);

    @Description(
        "The Cloud Pub/Sub topic to publish to. "
            + "The name should be in the format of "
            + "projects/<project-id>/topics/<topic-name>.")
    @Validation.Required
    ValueProvider<String> getOutputTopic();

    void setOutputTopic(ValueProvider<String> outputTopic);

    @Description(
        "Filter events based on an optional attribute key. "
            + "No filters are applied if a filterKey is not specified.")
    @Validation.Required
    ValueProvider<String> getFilterKey();

    void setFilterKey(ValueProvider<String> filterKey);

    @Description(
        "Filter attribute value to use in case a filterKey is provided. "
            + "A null filterValue is used by default.")
    @Validation.Required
    ValueProvider<String> getFilterValue();

    void setFilterValue(ValueProvider<String> filterValue);
  }

  /**
   * DoFn that will determine if events are to be filtered. If filtering is enabled, it will only
   * publish events that pass the filter else, it will publish all input events.
   */
  public static class ExtractAndFilterEventsFn extends DoFn<PubsubMessage, PubsubMessage> {

    private Boolean doFilter;
    private String inputFilterKey;
    private String inputFilterValue;
    private static final Logger LOG = LoggerFactory.getLogger(ExtractAndFilterEventsFn.class);

    @StartBundle
    public void startBundle(StartBundleContext context) {
      if (this.doFilter != null) {
        return; // Filter has been evaluated already
      }

      Options options = context.getPipelineOptions().as(Options.class);

      inputFilterKey = (options.getFilterKey() == null ? null : options.getFilterKey().get());
      inputFilterValue = (options.getFilterValue() == null ? null : options.getFilterValue().get());

      if (inputFilterKey == null) {
        this.doFilter = false;
      } else {
        this.doFilter = true;
        LOG.info(
            "Enabling event filter [key: " + inputFilterKey + "][value: " + inputFilterValue + "]");
      }
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      if (!this.doFilter) {
        // Filter is not enabled
        context.output(context.element());
      } else {
        PubsubMessage message = context.element();
        String extractedValue = message.getAttribute(this.inputFilterKey);
        if (Objects.equals(extractedValue, this.inputFilterValue)) {
          context.output(message);
        }
      }
    }
  }

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

    /**
     * Steps: 1) Read PubSubMessage with attributes from input PubSub subscription.
     *        2) Apply any filters if an attribute=value pair is provided.
     *        3) Write each PubSubMessage to output PubSub topic.
     */
    pipeline
        .apply(
            "Read PubSub Events",
            PubsubIO.readMessagesWithAttributes().fromSubscription(options.getInputSubscription()))
        .apply("Filter Events If Enabled", ParDo.of(new ExtractAndFilterEventsFn()))
        .apply("Write PubSub Events", PubsubIO.writeMessages().to(options.getOutputTopic()));

    // Execute the pipeline and return the result.
    return pipeline.run();
  }
}
