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
package com.google.cloud.teleport.templates;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.templates.PubsubToPubsub.Options;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A template that copies messages from one Pubsub subscription to another Pubsub topic. Allows
 * filtering specific messages.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v1/README_Cloud_PubSub_to_Cloud_PubSub.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Cloud_PubSub_to_Cloud_PubSub",
    category = TemplateCategory.STREAMING,
    displayName = "Pub/Sub to Pub/Sub",
    description =
        "Streaming pipeline. Reads from a Pub/Sub subscription and writes to a Pub/Sub topic. ",
    additionalHelp = "The template allows filtering specific messages.",
    optionsClass = Options.class,
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/pubsub-to-pubsub",
    contactInformation = "https://cloud.google.com/support")
public class PubsubToPubsub {

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

    /*
     Steps:

     <p>1) Read PubSubMessage with attributes from input PubSub subscription.

     <p>2) Apply any filters if an attribute=value pair is provided.

     <p>3) Write each PubSubMessage to output PubSub topic.
    */
    pipeline
        .apply(
            "Read PubSub Events",
            PubsubIO.readMessagesWithAttributes().fromSubscription(options.getInputSubscription()))
        .apply(
            "Filter Events If Enabled",
            ParDo.of(
                ExtractAndFilterEventsFn.newBuilder()
                    .withFilterKey(options.getFilterKey())
                    .withFilterValue(options.getFilterValue())
                    .build()))
        .apply("Write PubSub Events", PubsubIO.writeMessages().to(options.getOutputTopic()));

    // Execute the pipeline and return the result.
    return pipeline.run();
  }

  /**
   * Options supported by {@link PubsubToPubsub}.
   *
   * <p>Inherits standard configuration options.
   */
  public interface Options extends PipelineOptions, StreamingOptions {
    @TemplateParameter.PubsubSubscription(
        order = 1,
        description = "Pub/Sub input subscription",
        helpText =
            "Pub/Sub subscription to read the input from, in the format of 'projects/your-project-id/subscriptions/your-subscription-name'",
        example = "projects/your-project-id/subscriptions/your-subscription-name")
    @Validation.Required
    ValueProvider<String> getInputSubscription();

    void setInputSubscription(ValueProvider<String> inputSubscription);

    @TemplateParameter.PubsubTopic(
        order = 2,
        description = "Output Pub/Sub topic",
        helpText =
            "The name of the topic to which data should published, in the format of 'projects/your-project-id/topics/your-topic-name'",
        example = "projects/your-project-id/topics/your-topic-name")
    @Validation.Required
    ValueProvider<String> getOutputTopic();

    void setOutputTopic(ValueProvider<String> outputTopic);

    @TemplateParameter.Text(
        order = 3,
        optional = true,
        description = "Event filter key",
        helpText =
            "Attribute key by which events are filtered. No filters are applied if no key is specified.")
    ValueProvider<String> getFilterKey();

    void setFilterKey(ValueProvider<String> filterKey);

    @TemplateParameter.Text(
        order = 4,
        optional = true,
        description = "Event filter value",
        helpText =
            "Filter attribute value to use if an event filter key is provided. Accepts a valid "
                + "Java Regex string as an event filter value. In case a regex is provided, the complete "
                + "expression should match in order for the message to be filtered. Partial matches (e.g. "
                + "substring) will not be filtered. A null event filter value is used by default.")
    ValueProvider<String> getFilterValue();

    void setFilterValue(ValueProvider<String> filterValue);
  }

  /**
   * DoFn that will determine if events are to be filtered. If filtering is enabled, it will only
   * publish events that pass the filter else, it will publish all input events.
   */
  @AutoValue
  public abstract static class ExtractAndFilterEventsFn extends DoFn<PubsubMessage, PubsubMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(ExtractAndFilterEventsFn.class);

    // Counter tracking the number of incoming Pub/Sub messages.
    private static final Counter INPUT_COUNTER =
        Metrics.counter(ExtractAndFilterEventsFn.class, "inbound-messages");

    // Counter tracking the number of output Pub/Sub messages after the user provided filter
    // is applied.
    private static final Counter OUTPUT_COUNTER =
        Metrics.counter(ExtractAndFilterEventsFn.class, "filtered-outbound-messages");

    private Boolean doFilter;
    private String inputFilterKey;
    private Pattern inputFilterValueRegex;
    private Boolean isNullFilterValue;

    public static Builder newBuilder() {
      return new AutoValue_PubsubToPubsub_ExtractAndFilterEventsFn.Builder();
    }

    @Nullable
    abstract ValueProvider<String> filterKey();

    @Nullable
    abstract ValueProvider<String> filterValue();

    @Setup
    public void setup() {

      if (this.doFilter != null) {
        return; // Filter has been evaluated already
      }

      inputFilterKey = (filterKey() == null ? null : filterKey().get());

      if (inputFilterKey == null) {

        // Disable input message filtering.
        this.doFilter = false;

      } else {

        this.doFilter = true; // Enable filtering.

        String inputFilterValue = (filterValue() == null ? null : filterValue().get());

        if (inputFilterValue == null) {

          LOG.warn(
              "User provided a NULL for filterValue. Only messages with a value of NULL for the"
                  + " filterKey: {} will be filtered forward",
              inputFilterKey);

          // For backward compatibility, we are allowing filtering by null filterValue.
          this.isNullFilterValue = true;
          this.inputFilterValueRegex = null;
        } else {

          this.isNullFilterValue = false;
          try {
            inputFilterValueRegex = getFilterPattern(inputFilterValue);
          } catch (PatternSyntaxException e) {
            LOG.error("Invalid regex pattern for supplied filterValue: {}", inputFilterValue);
            throw new RuntimeException(e);
          }
        }

        LOG.info(
            "Enabling event filter [key: " + inputFilterKey + "][value: " + inputFilterValue + "]");
      }
    }

    @ProcessElement
    public void processElement(ProcessContext context) {

      INPUT_COUNTER.inc();
      if (!this.doFilter) {

        // Filter is not enabled
        writeOutput(context, context.element());
      } else {

        PubsubMessage message = context.element();
        String extractedValue = message.getAttribute(this.inputFilterKey);

        if (this.isNullFilterValue) {

          if (extractedValue == null) {
            // If we are filtering for null and the extracted value is null, we forward
            // the message.
            writeOutput(context, message);
          }

        } else {

          if (extractedValue != null
              && this.inputFilterValueRegex.matcher(extractedValue).matches()) {
            // If the extracted value is not null and it matches the filter,
            // we forward the message.
            writeOutput(context, message);
          }
        }
      }
    }

    /**
     * Write a {@link PubsubMessage} and increment the output counter.
     *
     * @param context {@link ProcessContext} to write {@link PubsubMessage} to.
     * @param message {@link PubsubMessage} output.
     */
    private void writeOutput(ProcessContext context, PubsubMessage message) {
      OUTPUT_COUNTER.inc();
      context.output(message);
    }

    /**
     * Return a {@link Pattern} based on a user provided regex string.
     *
     * @param regex Regex string to compile.
     * @return {@link Pattern}
     * @throws PatternSyntaxException If the string is an invalid regex.
     */
    private Pattern getFilterPattern(String regex) throws PatternSyntaxException {
      checkNotNull(regex, "Filter regex cannot be null.");
      return Pattern.compile(regex);
    }

    /** Builder class for {@link ExtractAndFilterEventsFn}. */
    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setFilterKey(ValueProvider<String> filterKey);

      abstract Builder setFilterValue(ValueProvider<String> filterValue);

      abstract ExtractAndFilterEventsFn build();

      /**
       * Method to set the filterKey used for filtering messages.
       *
       * @param filterKey Lookup key for the {@link PubsubMessage} attribute map.
       * @return {@link Builder}
       */
      public Builder withFilterKey(ValueProvider<String> filterKey) {
        checkArgument(filterKey != null, "withFilterKey(filterKey) called with null input.");
        return setFilterKey(filterKey);
      }

      /**
       * Method to set the filterValue used for filtering messages.
       *
       * @param filterValue Lookup value for the {@link PubsubMessage} attribute map.
       * @return {@link Builder}
       */
      public Builder withFilterValue(ValueProvider<String> filterValue) {
        checkArgument(filterValue != null, "withFilterValue(filterValue) called with null input.");
        return setFilterValue(filterValue);
      }
    }
  }
}
