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
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An template that copies messages from one Pubsub subscription to another Pubsub topic. */
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

    /**
     * Steps:
     * 1) Read PubSubMessage with attributes from input PubSub subscription.
     * 2) Apply any filters if an attribute=value pair is provided.
     * 3) Enrich attributes with provided key value pairs.
     * 4) Write each PubSubMessage to output PubSub topic.
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
        .apply(
            "Enrich Events with Attributes If Enabled",
            ParDo.of(
                EnrichEventsFn.newBuilder()
                    .withEnrichKey1(options.getEnrichKey1())
                    .withEnrichValue1(options.getEnrichValue1())
                    .withEnrichKey2(options.getEnrichKey2())
                    .withEnrichValue2(options.getEnrichValue2())
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
        "Filter attribute value to use in case a filterKey is provided. Accepts a valid Java regex"
            + " string as a filterValue. In case a regex is provided, the complete expression"
            + " should match in order for the message to be filtered. Partial matches (e.g."
            + " substring) will not be filtered. A null filterValue is used by default.")
    @Validation.Required
    ValueProvider<String> getFilterValue();

    void setFilterValue(ValueProvider<String> filterValue);

    @Description(
        "Enrich message attributes with this key")
    @Validation.Required
    ValueProvider<String> getEnrichKey1();

    void setEnrichKey1(ValueProvider<String> enrichKey1);

    @Description(
        "Enrich message attributes with this value mapped to EnrichKey1")
    @Validation.Required
    ValueProvider<String> getEnrichValue1();

    void setEnrichValue1(ValueProvider<String> enrichValue1);

    @Description(
        "Enrich message attributes with this key")
    @Validation.Required
    ValueProvider<String> getEnrichKey2();

    void setEnrichKey2(ValueProvider<String> enrichKey2);

    @Description(
        "Enrich message attributes with this value mapped to EnrichKey2")
    @Validation.Required
    ValueProvider<String> getEnrichValue2();

    void setEnrichValue2(ValueProvider<String> enrichValue2);
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

  /**
   * DoFn that will enrich events with provided attributes.
   */
  @AutoValue
  public abstract static class EnrichEventsFn extends DoFn<PubsubMessage, PubsubMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(EnrichEventsFn.class);

    private Boolean doEnrich1;
    private String inputEnrichKey1;
    private String inputEnrichValue1;
    private Boolean doEnrich2;
    private String inputEnrichKey2;
    private String inputEnrichValue2;

    public static Builder newBuilder() {
      return new AutoValue_PubsubToPubsub_EnrichEventsFn.Builder();
    }

    @Nullable
    abstract ValueProvider<String> enrichKey1();

    @Nullable
    abstract ValueProvider<String> enrichValue1();

    @Nullable
    abstract ValueProvider<String> enrichKey2();

    @Nullable
    abstract ValueProvider<String> enrichValue2();

    @Setup
    public void setup() {

      if (this.doEnrich1 != null && this.doEnrich2 != null) {
        return; // Enrich setup has been evaluated already
      }

      inputEnrichKey1 = (enrichKey1() == null ? null : enrichKey1().get());

      if (inputEnrichKey1 == null) {
        this.doEnrich1 = false;
      } else {
        this.doEnrich1 = true;
        inputEnrichValue1 = (enrichValue1() == null ? null : enrichValue1().get());

        if (inputEnrichValue1 == null) {
          LOG.warn("User provided a NULL for enrichValue1.", inputEnrichKey1);
        }
      }

      inputEnrichKey2 = (enrichKey2() == null ? null : enrichKey2().get());

      if (inputEnrichKey2 == null) {
        this.doEnrich2 = false;
      } else {
        this.doEnrich2 = true;
        inputEnrichValue2 = (enrichValue2() == null ? null : enrichValue2().get());

        if (inputEnrichValue2 == null) {
          LOG.warn("User provided a NULL for enrichValue2.", inputEnrichKey2);
        }
      }
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      PubsubMessage message = context.element();
      Map<String, String> attrs = message.getAttributeMap();
      HashMap<String, String> newAttrs = new HashMap<String, String>(attrs);
      if (this.doEnrich1) {
        newAttrs.put(this.inputEnrichKey1, this.inputEnrichValue1);
      }
      if (this.doEnrich2) {
        newAttrs.put(this.inputEnrichKey2, this.inputEnrichValue2);
      }
      PubsubMessage newMessage = new PubsubMessage(message.getPayload(), newAttrs);
      context.output(newMessage);
    }

    /** Builder class for {@link EnrichEventsFn}. */
    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setEnrichKey1(ValueProvider<String> enrichKey1);

      abstract Builder setEnrichValue1(ValueProvider<String> enrichValue1);

      abstract Builder setEnrichKey2(ValueProvider<String> enrichKey2);

      abstract Builder setEnrichValue2(ValueProvider<String> enrichValue2);

      abstract EnrichEventsFn build();

      /**
       * Method to set the enrichKey1
       *
       * @param enrichKey1 key for the {@link PubsubMessage} attribute map.
       * @return {@link Builder}
       */
      public Builder withEnrichKey1(ValueProvider<String> enrichKey1) {
        checkArgument(enrichKey1 != null, "withEnrichKey1(enrichKey1) called with null input.");
        return setEnrichKey1(enrichKey1);
      }

      /**
       * Method to set the enrichValue1
       *
       * @param enrichValue1 value for the {@link PubsubMessage} attribute map.
       * @return {@link Builder}
       */
      public Builder withEnrichValue1(ValueProvider<String> enrichValue1) {
        checkArgument(enrichValue1 != null, "withFilterValue(enrichValue1) called with null input.");
        return setEnrichValue1(enrichValue1);
      }

      /**
       * Method to set the enrichKey2
       *
       * @param enrichKey2 key for the {@link PubsubMessage} attribute map.
       * @return {@link Builder}
       */
      public Builder withEnrichKey2(ValueProvider<String> enrichKey2) {
        checkArgument(enrichKey2 != null, "withEnrichKey2(enrichKey2) called with null input.");
        return setEnrichKey2(enrichKey2);
      }

      /**
       * Method to set the enrichValue2
       *
       * @param enrichValue2 value for the {@link PubsubMessage} attribute map.
       * @return {@link Builder}
       */
      public Builder withEnrichValue2(ValueProvider<String> enrichValue2) {
        checkArgument(enrichValue2 != null, "withFilterValue(enrichValue2) called with null input.");
        return setEnrichValue2(enrichValue2);
      }
    }
  }
}
