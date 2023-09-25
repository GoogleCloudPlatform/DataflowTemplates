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
import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.metadata.auto.Outputs;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.NonNull;

@AutoService(SchemaTransformProvider.class)
public class ReadFromPubSub
    extends TemplateReadTransform<
        ReadFromPubSub.ReadFromPubSubOptions, ReadFromPubSub.ReadFromPubSubTransformConfiguration> {

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class ReadFromPubSubTransformConfiguration extends Configuration {

    @TemplateParameter.PubsubSubscription(
        order = 1,
        description = "Pub/Sub input subscription",
        helpText =
            "Pub/Sub subscription to read the input from, in the format of 'projects/your-project-id/subscriptions/your-subscription-name'")
    abstract String getInputSubscription();

    public void validate() {}

    public ReadFromPubSubTransformConfiguration.Builder builder() {
      return new AutoValue_ReadFromPubSub_ReadFromPubSubTransformConfiguration.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder extends Configuration.Builder<Builder> {
      public abstract ReadFromPubSubTransformConfiguration.Builder setInputSubscription(
          String subscription);

      public abstract ReadFromPubSubTransformConfiguration build();
    }

    public static ReadFromPubSubTransformConfiguration fromOptions(ReadFromPubSubOptions options) {
      return new AutoValue_ReadFromPubSub_ReadFromPubSubTransformConfiguration.Builder()
          .setInputSubscription(options.getInputSubscription())
          .build();
    }
  }

  @Override
  public @NonNull Class<ReadFromPubSubTransformConfiguration> configurationClass() {
    return ReadFromPubSubTransformConfiguration.class;
  }

  @Override
  public @NonNull String identifier() {
    return "blocks:external:org.apache.beam:read_from_pubsub:v1";
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

  @Outputs(
      value = Row.class,
      types = {RowTypes.PubSubMessageRow.class})
  public PCollectionRowTuple read(Pipeline pipeline, ReadFromPubSubOptions options) {

    return transform(pipeline.begin(), ReadFromPubSubTransformConfiguration.fromOptions(options));
  }

  @Outputs(
      value = Row.class,
      types = {RowTypes.PubSubMessageRow.class})
  public PCollectionRowTuple transform(PBegin input, ReadFromPubSubTransformConfiguration config) {
    return PCollectionRowTuple.of(
        BlockConstants.OUTPUT_TAG,
        input
            .apply(
                "ReadPubSubSubscription",
                PubsubIO.readMessagesWithAttributesAndMessageId()
                    .fromSubscription(config.getInputSubscription()))
            .apply(
                MapElements.into(TypeDescriptor.of(Row.class))
                    .via(RowTypes.PubSubMessageRow::PubSubMessageToRow))
            .setCoder(RowCoder.of(RowTypes.PubSubMessageRow.SCHEMA)));
  }

  @Override
  public Class<ReadFromPubSubOptions> getOptionsClass() {
    return ReadFromPubSubOptions.class;
  }
}
