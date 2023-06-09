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

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Producer {
  private static final Logger LOGGER = LoggerFactory.getLogger(Producer.class);

  private final KafkaProducer<String, String> producer;

  int partition;
  String topic;

  public Producer(String topic, int partitionId, String bootstrapServers) {

    this.topic = topic;
    Properties props = new Properties();
    props.put("bootstrap.servers", bootstrapServers);
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.setProperty(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    this.partition = partitionId;

    this.producer = new KafkaProducer<>(props);
  }

  public void publish(String rec, String key) {
    try {

      ProducerRecord<String, String> record = new ProducerRecord<>(topic, partition, key, rec);
      producer.send(record).get();
      producer.flush();

    } catch (ExecutionException e) {
      producer.close();
      LOGGER.info("Error during publishing to the error topic", e);
    } catch (InterruptedException e) {
    }
  }
}
