/*
 * Copyright (C) 2020 Google Inc.
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
package com.google.cloud.teleport.v2.options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;

/**
 * Common {@link PipelineOptions} for reading and writing data using {@link
 * org.apache.beam.sdk.io.gcp.pubsub.PubsubIO}.
 */
public final class PubsubCommonOptions {

  private PubsubCommonOptions() {}

  /**
   * Provides {@link PipelineOptions} to read records from a Pub/Sub subscription.
   */
  public interface ReadSubscriptionOptions extends PipelineOptions {

    @Description(
        "The Cloud Pub/Sub subscription to consume from. "
            + "The name should be in the format of "
            + "projects/<project-id>/subscriptions/<subscription-name>.")
    @Required
    String getInputSubscription();
    void setInputSubscription(String inputSubscription);
  }

  /**
   * Provides {@link PipelineOptions} to read records from a Pub/Sub topic.
   */
  public interface ReadTopicOptions extends PipelineOptions {

    @Description(
        "The Cloud Pub/Sub topic to consume from. "
            + "The name should be in the format of "
            + "projects/<project-id>/topics/<topic-name>.")
    @Required
    String getInputTopic();

    void setInputTopic(String outputTopic);
  }

  /**
   * Provides {@link PipelineOptions} to write records to a Pub/Sub topic.
   */
  public interface WriteTopicOptions extends PipelineOptions {

    @Description(
        "The Cloud Pub/Sub topic to write to. "
            + "The name should be in the format of "
            + "projects/<project-id>/topics/<topic-name>.")
    @Required
    String getOutputTopic();

    void setOutputTopic(String outputTopic);
  }
}
