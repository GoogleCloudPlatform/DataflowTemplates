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
package com.google.cloud.teleport.v2.templates.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

/** */
public class Consumer {

  private KafkaConsumer<String, String> consumer;

  private ConsumerRecords<String, String> recordsWm;

  private TopicPartition partition;

  public Consumer(String topic, int partitionId, String bootstrapServers) {

    final Properties props = new Properties();
    props.setProperty("bootstrap.servers", bootstrapServers);
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test123");
    props.setProperty(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.setProperty(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "1800000");
    props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

    consumer = new KafkaConsumer<>(props);
    partition = new TopicPartition(topic, partitionId);

    consumer.assign(Arrays.asList(partition));
  }

  public void commitOffsets() {

    consumer.commitSync();
  }

  public List<String> getRecords() {

    List<String> buffer = new ArrayList<>();

    if (partition != null) {
      consumer.resume(Arrays.asList(partition));
    }

    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

    if (records.count() == 0) {
      return buffer;
    }
    for (ConsumerRecord<String, String> record : records) {
      buffer.add(record.value());
    }
    consumer.pause(Arrays.asList(partition));
    return buffer;
  }
}
