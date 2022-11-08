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
package com.google.cloud.teleport.templates.common;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

/** Options for Teleport PubsubIO. */
public class PubsubConverters {

  /** Options for Pubsub reads. */
  public interface PubsubReadOptions extends PipelineOptions {
    @Description("Pubsub topic to read data from")
    ValueProvider<String> getPubsubReadTopic();

    void setPubsubReadTopic(ValueProvider<String> pubsubReadTopic);
  }

  /** Options for Pubsub writes. */
  public interface PubsubWriteOptions extends PipelineOptions {
    @Description("Pubsub topic to write data to")
    ValueProvider<String> getPubsubWriteTopic();

    void setPubsubWriteTopic(ValueProvider<String> pubsubWriteTopic);
  }

  /** Options for Pubsub reads from a subscription. */
  public interface PubsubReadSubscriptionOptions extends PipelineOptions {
    @Description(
        "The Cloud Pub/Sub subscription to consume from. "
            + "The name should be in the format of "
            + "projects/<project-id>/subscriptions/<subscription-name>.")
    ValueProvider<String> getInputSubscription();

    void setInputSubscription(ValueProvider<String> inputSubscription);
  }

  /** Options for using Pub/Sub as a deadletter sink. */
  public interface PubsubWriteDeadletterTopicOptions extends PipelineOptions {
    @Description(
        "The Cloud Pub/Sub topic to publish deadletter records to. "
            + "The name should be in the format of "
            + "projects/<project-id>/topics/<topic-name>.")
    @Validation.Required
    ValueProvider<String> getOutputDeadletterTopic();

    void setOutputDeadletterTopic(ValueProvider<String> deadletterTopic);
  }
}
