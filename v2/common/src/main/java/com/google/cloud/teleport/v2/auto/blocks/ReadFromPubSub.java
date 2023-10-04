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
import com.google.cloud.teleport.v2.auto.schema.RowTypes;
import com.google.cloud.teleport.v2.auto.schema.TemplateOptionSchema;
import com.google.cloud.teleport.v2.auto.schema.TemplateReadTransform;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.NonNull;

@AutoService(SchemaTransformProvider.class)
public class ReadFromPubSub extends TemplateReadTransform<ReadFromPubSub.ReadFromPubSubOptions> {

  @DefaultSchema(TemplateOptionSchema.class)
  public interface ReadFromPubSubOptions extends TemplateBlockOptions {

    @TemplateParameter.PubsubSubscription(
        order = 1,
        description = "Pub/Sub input subscription",
        helpText =
            "Pub/Sub subscription to read the input from, in the format of 'projects/your-project-id/subscriptions/your-subscription-name'")
    String getInputSubscription();

    void setInputSubscription(String input);
  }

  @Override
  public @NonNull String identifier() {
    return "blocks:external:org.apache.beam:read_from_pubsub:v1";
  }

  @Outputs(RowTypes.PubSubMessageRow.class)
  public PCollectionRowTuple read(PBegin input, ReadFromPubSubOptions options) {
    return PCollectionRowTuple.of(
        BlockConstants.OUTPUT_TAG,
        input
            .apply(
                "ReadPubSubSubscription",
                PubsubIO.readMessagesWithAttributesAndMessageId()
                    .fromSubscription(options.getInputSubscription()))
            .apply(RowTypes.PubSubMessageRow.MapToRow.of()));
  }

  @Override
  public Class<ReadFromPubSubOptions> getOptionsClass() {
    return ReadFromPubSubOptions.class;
  }
}
