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
package com.google.cloud.teleport.v2.templates.sinks;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.cloud.teleport.v2.templates.common.InputFileReader;
import com.google.cloud.teleport.v2.templates.common.Shard;
import com.google.cloud.teleport.v2.templates.common.TrimmedDataChangeRecord;
import com.google.cloud.teleport.v2.templates.constants.Constants;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/** Class to store connection info and write methods for Kafka. */
public class KafkaSink implements DataSink, Serializable {
  private KafkaConnectionProfile kafkaConn;
  private Map<String, Integer> shardIdToPartitionId;

  private KafkaProducer<String, String> producer;

  public KafkaSink(String kafkaClusterFilePath, String getSourceShardsFilePath) {
    this.kafkaConn = InputFileReader.getKafkaConnectionProfile(kafkaClusterFilePath);
    this.shardIdToPartitionId = mapShardIdToPartitionId(getSourceShardsFilePath);
  }

  public static Map<String, Integer> mapShardIdToPartitionId(String sourceShardsFilePath) {
    List<Shard> shards = InputFileReader.getOrderedShardDetails(sourceShardsFilePath);
    Integer partitionId = 0;
    Map<String, Integer> result = new HashMap<String, Integer>();
    for (Shard shard : shards) {
      result.put(shard.getLogicalShardId(), partitionId);
      partitionId++;
    }
    return result;
  }

  public void createClient() {
    // TODO: Configure retry settings.
    Properties props = new Properties();
    props.put(Constants.BOOTSTRAP_SERVER_CONFIG, kafkaConn.getBootstrapServer());
    props.put(Constants.ACKS_CONFIG, "1");
    props.put(Constants.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(Constants.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    this.producer = new KafkaProducer<>(props);
  }

  public void write(String shardId, List<TrimmedDataChangeRecord> recordsToOutput)
      throws Exception {
    try {
      // TODO: Consider configuring callbacks for error handling.
      ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
      for (TrimmedDataChangeRecord rec : recordsToOutput) {
        ProducerRecord<String, String> record =
            new ProducerRecord<>(
                kafkaConn.getDataTopic(),
                shardIdToPartitionId.get(shardId),
                shardId,
                ow.writeValueAsString(rec));
        producer.send(record);
      }
      producer.flush();
    } catch (Exception e) {
      throw new Exception("error while writing to kafka: " + e);
    }
  }
}
