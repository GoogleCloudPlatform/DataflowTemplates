/*
 * Copyright (C) 2020 Google LLC
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
package com.google.cloud.teleport.v2.options;

import com.google.cloud.teleport.metadata.TemplateParameter;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;

/**
 * Common {@link PipelineOptions} for reading and writing data using {@link
 * org.apache.beam.sdk.io.gcp.pubsub.PubsubIO}.
 */
public final class PubsubCommonOptions {

  private PubsubCommonOptions() {}

  /** Provides {@link PipelineOptions} to read records from a Pub/Sub subscription. */
  public interface ReadSubscriptionOptions extends PipelineOptions {

    @TemplateParameter.PubsubSubscription(
        order = 1,
        description = "Pub/Sub input subscription",
        helpText =
            "Pub/Sub subscription to read the input from, in the format of 'projects/your-project-id/subscriptions/your-subscription-name'",
        example = "projects/your-project-id/subscriptions/your-subscription-name")
    @Required
    String getInputSubscription();

    void setInputSubscription(String inputSubscription);
  }

  /** Provides {@link PipelineOptions} to read records from a Pub/Sub topic. */
  public interface ReadTopicOptions extends PipelineOptions {

    @TemplateParameter.PubsubTopic(
        order = 2,
        description = "Pub/Sub input topic",
        helpText =
            "Pub/Sub topic to read the input from, in the format of "
                + "'projects/your-project-id/topics/your-topic-name'",
        example = "projects/your-project-id/topics/your-topic-name")
    @Required
    String getInputTopic();

    void setInputTopic(String outputTopic);
  }

  /** Provides {@link PipelineOptions} to write records to a Pub/Sub topic. */
  public interface WriteTopicOptions extends PipelineOptions {

    @TemplateParameter.PubsubTopic(
        order = 3,
        description = "Output Pub/Sub topic",
        helpText =
            "The name of the topic to which data should published, in the format of 'projects/your-project-id/topics/your-topic-name'",
        example = "projects/your-project-id/topics/your-topic-name")
    @Required
    String getOutputTopic();

    void setOutputTopic(String outputTopic);
  }
}
