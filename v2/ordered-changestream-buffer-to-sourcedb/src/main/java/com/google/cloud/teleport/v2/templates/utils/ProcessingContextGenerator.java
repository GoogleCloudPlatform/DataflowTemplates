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
package com.google.cloud.teleport.v2.templates.utils;

import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SessionFileReader;
import com.google.cloud.teleport.v2.templates.common.ProcessingContext;
import com.google.cloud.teleport.v2.templates.common.Shard;
import com.google.cloud.teleport.v2.templates.kafka.KafkaConnectionProfile;
import com.google.cloud.teleport.v2.templates.pubsub.PubSubConsumerProfile;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class to create the processing context. */
public class ProcessingContextGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(ProcessingContextGenerator.class);

  public static Map<String, ProcessingContext> getProcessingContextForKafka(
      String sourceShardsFilePath,
      String sourceType,
      String kafkaClusterFilePath,
      String sessionFilePath,
      String sourceDbTimezoneOffset,
      Boolean enableSourceDbSsl,
      Boolean enableSourceDbSslValidation) {

    LOG.info(" In getProcessingContextForKafka");

    List<Shard> shards = InputFileReader.getOrderedShardDetails(sourceShardsFilePath, sourceType);
    KafkaConnectionProfile kafkaConnectionProfileBase =
        InputFileReader.getKafkaConnectionProfile(kafkaClusterFilePath);
    Schema schema = SessionFileReader.read(sessionFilePath);

    int partitionId = 0;
    Map<String, ProcessingContext> response = new HashMap<>();

    try {
      for (Shard shard : shards) {
        LOG.info(" The sorted shard is: {}", shard);
        KafkaConnectionProfile kafkaConnectionProfile =
            (KafkaConnectionProfile) kafkaConnectionProfileBase.clone();
        kafkaConnectionProfile.setPartitionId(partitionId);
        partitionId++;
        ProcessingContext taskContext =
            new ProcessingContext(
                kafkaConnectionProfile,
                shard,
                schema,
                sourceDbTimezoneOffset,
                sourceType,
                enableSourceDbSsl,
                enableSourceDbSslValidation);
        response.put(shard.getLogicalShardId(), taskContext);
      }
    } catch (CloneNotSupportedException e) {
      LOG.error("Failed to clone kafka cluster object.", e);
      throw new RuntimeException("Failed to clone kafka cluster object", e);
    }
    return response;
  }

  public static Map<String, ProcessingContext> getProcessingContextForPubSub(
      String sourceShardsFilePath,
      String sourceType,
      String projectId,
      String sessionFilePath,
      Integer pubsubMaxReadCount,
      String sourceDbTimezoneOffset,
      Boolean enableSourceDbSsl,
      Boolean enableSourceDbSslValidation) {

    LOG.info(" In getProcessingContextForPubSub");

    List<Shard> shards = InputFileReader.getOrderedShardDetails(sourceShardsFilePath, sourceType);
    Schema schema = SessionFileReader.read(sessionFilePath);
    PubSubConsumerProfile pubSubConsumerProfile =
        new PubSubConsumerProfile(projectId, pubsubMaxReadCount);
    Map<String, ProcessingContext> response = new HashMap<>();

    for (Shard shard : shards) {
      LOG.info(" The sorted shard is: {}", shard);
      ProcessingContext taskContext =
          new ProcessingContext(
              shard,
              schema,
              pubSubConsumerProfile,
              sourceDbTimezoneOffset,
              sourceType,
              enableSourceDbSsl,
              enableSourceDbSslValidation);
      response.put(shard.getLogicalShardId(), taskContext);
    }
    return response;
  }
}
