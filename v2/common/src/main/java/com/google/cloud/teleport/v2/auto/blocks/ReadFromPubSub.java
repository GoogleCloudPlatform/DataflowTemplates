/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.auto.blocks;

import com.google.auto.service.AutoService;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.metadata.auto.Outputs;
import com.google.cloud.teleport.metadata.auto.TemplateTransform;
import com.google.cloud.teleport.v2.auto.blocks.ReadFromPubSub.ReadFromPubSubOptions;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.expansion.ExternalTransformRegistrar;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.NonNull;

import static com.google.cloud.teleport.v2.auto.blocks.StandardCoderConverters.pubSubMessageToRow;

@AutoService(ExternalTransformRegistrar.class)
public class ReadFromPubSub
    implements TemplateTransform<ReadFromPubSubOptions>, ExternalTransformRegistrar {

  private static final String URN = "blocks:external:org.apache.beam:read_from_pubsub:v1";

  private static class Builder
      implements ExternalTransformBuilder<
          Configuration, @NonNull PBegin, @NonNull PCollection<Row>> {
    @Override
    public @NonNull PTransform<@NonNull PBegin, @NonNull PCollection<Row>> buildExternal(
        Configuration config) {
      return new PTransform<>() {
        @Override
        public @NonNull PCollection<Row> expand(@NonNull PBegin input) {
          return pubSubMessageToRow(underlyingTransform(input, config));
        }
      };
    }
  }

  public Map<String, Class<? extends ExternalTransformBuilder<?, ?, ?>>> knownBuilders() {
    return ImmutableMap.of(URN, Builder.class);
  }

  // TODO(polber) - See if this Config class can be generated programmatically from TransformOptions
  // Interface
  public static class Configuration {
    String inputSubscription = "";

    public void setInputSubscription(String input) {
      this.inputSubscription = input;
    }

    public String getInputSubscription() {
      return this.inputSubscription;
    }

    public static Configuration fromOptions(ReadFromPubSubOptions options) {
      Configuration config = new Configuration();
      config.setInputSubscription(options.getInputSubscription());
      return config;
    }
  }

  public interface ReadFromPubSubOptions extends PipelineOptions {

    @TemplateParameter.PubsubSubscription(
        order = 1,
        description = "Pub/Sub input subscription",
        helpText =
            "Pub/Sub subscription to read the input from, in the format of 'projects/your-project-id/subscriptions/your-subscription-name'")
    String getInputSubscription();

    void setInputSubscription(String input);
  }

  @Outputs(PubsubMessage.class)
  public PCollection<PubsubMessage> read(Pipeline pipeline, ReadFromPubSubOptions options) {

    return underlyingTransform(pipeline.begin(), Configuration.fromOptions(options));
  }

  static PCollection<PubsubMessage> underlyingTransform(PBegin input, Configuration config) {
    return input.apply(
        "ReadPubSubSubscription",
        PubsubIO.readMessagesWithAttributesAndMessageId()
            .fromSubscription(config.getInputSubscription()));
  }

  @Override
  public Class<ReadFromPubSubOptions> getOptionsClass() {
    return ReadFromPubSubOptions.class;
  }
}
