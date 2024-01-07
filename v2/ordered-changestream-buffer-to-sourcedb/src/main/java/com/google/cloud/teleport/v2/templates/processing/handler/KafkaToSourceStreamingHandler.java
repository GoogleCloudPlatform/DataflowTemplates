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
package com.google.cloud.teleport.v2.templates.processing.handler;

import com.google.cloud.teleport.v2.templates.common.InputBufferReader;
import com.google.cloud.teleport.v2.templates.common.ProcessingContext;
import com.google.cloud.teleport.v2.templates.kafka.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handles reading ordered change stream records from Kafka and writing to source. */
public class KafkaToSourceStreamingHandler extends StreamingHandler {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaToSourceStreamingHandler.class);

  public KafkaToSourceStreamingHandler(ProcessingContext taskContext) {
    super(taskContext);
  }

  @Override
  public InputBufferReader getBufferReader() {

    Consumer kafkaConsumer =
        new Consumer(
            taskContext.getKafkaConnectionProfile().getDataTopic(),
            taskContext.getKafkaConnectionProfile().getPartitionId(),
            taskContext.getKafkaConnectionProfile().getBootstrapServer());

    return kafkaConsumer;
  }
}
