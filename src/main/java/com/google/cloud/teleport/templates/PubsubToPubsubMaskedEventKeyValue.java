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

import com.google.auto.value.AutoValue;
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
import java.nio.charset.StandardCharsets;
import com.google.cloud.teleport.templates.common.PubsubConverters.PubsubReadSubscriptionOptions;
import com.google.cloud.teleport.templates.common.PubsubConverters.PubsubWriteOptions;
import com.google.cloud.teleport.templates.common.PubsubConverters.PubsubMaskedAttributeKeyOptions;
import com.google.cloud.teleport.templates.common.PubsubConverters.PubsubMaskedEventKeyValueOptions;
import org.json.JSONObject;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An template that copies messages from one Pubsub subscription to another Pubsub topic. */
public class PubsubToPubsubMaskedEventKeyValue {
  private static final Logger LOG = LoggerFactory.getLogger(PubsubToPubsubMaskedEventKeyValue.class);

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
            "Masked Event Key Value",
            ParDo.of(new MaskedEventKeyValue(options.getMaskedAttributeKey(), options.getMaskedEventKeyValue())))
        .apply("Write PubSub Events", PubsubIO.writeMessages().to(options.getPubsubWriteTopic()));
    return pipeline.run();
  }

  private static class MaskedEventKeyValue extends DoFn<PubsubMessage, PubsubMessage> {

    private final String masked_attribute_key;
    private final String masked_event_key_value;

    MaskedEventKeyValue(ValueProvider<String>  masked_attribute_key, ValueProvider<String>  masked_event_key_value) {
      this.masked_attribute_key = masked_attribute_key.get();
      this.masked_event_key_value = masked_event_key_value.get();
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      PubsubMessage message = context.element();
      if (message.getAttribute(masked_attribute_key)!= null) {
        String [] masked_target_columns =  message.getAttribute(masked_attribute_key).split(",");
        if(masked_target_columns.length != 0) {
          JSONObject jsonObject = new JSONObject(new String(message.getPayload(), StandardCharsets.UTF_8));
          for(String s : masked_target_columns) {
              jsonObject.put(s, masked_event_key_value);
          }
          byte[] masked_message_byte = jsonObject.toString().getBytes(StandardCharsets.UTF_8);
          Map<String, String> attribute = message.getAttributeMap();
          message = new PubsubMessage(masked_message_byte, attribute, null);
        }
      }
      context.output(message);
    }
  }
  public interface PubSubOptions extends PubsubReadSubscriptionOptions, PubsubWriteOptions, PubsubMaskedAttributeKeyOptions, PubsubMaskedEventKeyValueOptions {}
  public interface Options extends StreamingOptions, PubSubOptions {}
}
