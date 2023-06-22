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

import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.metadata.auto.Outputs;
import com.google.cloud.teleport.metadata.auto.TemplateSource;
import com.google.cloud.teleport.v2.auto.blocks.ReadFromPubSub.ReadFromPubSubOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.PCollection;

public class ReadFromPubSub implements TemplateSource<PubsubMessage, ReadFromPubSubOptions> {

  public interface ReadFromPubSubOptions extends PipelineOptions {

    @TemplateParameter.PubsubSubscription(
        order = 1,
        description = "Pub/Sub input subscription",
        helpText =
            "Pub/Sub subscription to read the input from, in the format of 'projects/your-project-id/subscriptions/your-subscription-name'")
    String getInputSubscription();

    void setInputSubscription(String input);
  }

  @Override
  @Outputs(PubsubMessage.class)
  public PCollection<PubsubMessage> read(Pipeline pipeline, ReadFromPubSubOptions options) {

    return pipeline.apply(
        "ReadPubSubSubscription",
        PubsubIO.readMessagesWithAttributes().fromSubscription(options.getInputSubscription()));
  }

  @Override
  public Class<ReadFromPubSubOptions> getOptionsClass() {
    return ReadFromPubSubOptions.class;
  }
}
