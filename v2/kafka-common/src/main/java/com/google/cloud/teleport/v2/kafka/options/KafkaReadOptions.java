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
package com.google.cloud.teleport.v2.kafka.options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/**
 * The {@link KafkaReadOptions} interface provides the custom execution options passed by the
 * executor at the command-line.
 */
public interface KafkaReadOptions extends PipelineOptions {
  @Description(
      "Comma Separated list of source Kafka Bootstrap Servers (e.g:"
          + " server1:[port],server2:[port]).")
  @Validation.Required
  String getReadBootstrapServers();

  void setReadBootstrapServers(String bootstrapServers);

  @Description(
      "Comma Separated list of Kafka topic(s) to read the input from (e.g: topic1,topic2).")
  @Validation.Required
  String getKafkaReadTopics();

  void setReadTopics(String inputTopics);
}
