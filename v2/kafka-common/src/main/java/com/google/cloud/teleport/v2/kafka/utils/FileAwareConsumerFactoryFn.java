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
package com.google.cloud.teleport.v2.kafka.utils;

import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * A Kafka consumer factory implementation that pre-processes the consumer config and replaces all
 * references to GCS paths with local files and all references to Secret Manager secrets with their
 * actual values.
 */
public class FileAwareConsumerFactoryFn extends FileAwareFactoryFn<Consumer<byte[], byte[]>> {
  public FileAwareConsumerFactoryFn() {

    this(null);
  }

  /**
   * @param filePrefix if using more than 1 {@code FileAwareConsumerFactoryFn} in a pipeline, this
   *     file prefix has to be set to different values for each of them
   */
  public FileAwareConsumerFactoryFn(String filePrefix) {
    super("consumer", filePrefix);
  }

  @Override
  protected Consumer<byte[], byte[]> createObject(Map<String, Object> config) {
    return new KafkaConsumer<>(config);
  }
}
