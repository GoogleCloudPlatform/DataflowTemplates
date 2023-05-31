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
package com.google.cloud.teleport.v2.templates.common;

import com.google.cloud.teleport.v2.templates.kafka.KafkaConnectionProfile;
import com.google.cloud.teleport.v2.templates.pubsub.PubSubConsumerProfile;
import com.google.cloud.teleport.v2.templates.schema.Schema;
import java.io.Serializable;

/**
 * Each worker task context shard detail, the source connection profile and depending on the buffer
 * read from, either the PubSub project id or the KafkaConnectionProfile.
 */
public class ProcessingContext implements Serializable {

  private KafkaConnectionProfile kafkaConnectionProfile;
  private PubSubConsumerProfile pubSubConsumerProfile;
  private Shard shard;
  private Schema schema;
  private String bufferType;
  private String sourceDbTimezoneOffset;

  public ProcessingContext(
      KafkaConnectionProfile kafkaConnectionProfile,
      Shard shard,
      Schema schema,
      String sourceDbTimezoneOffset) {
    this.shard = shard;
    this.kafkaConnectionProfile = kafkaConnectionProfile;
    this.schema = schema;
    this.bufferType = "kafka";
    this.pubSubConsumerProfile = null;
    this.sourceDbTimezoneOffset = sourceDbTimezoneOffset;
  }

  public ProcessingContext(
      Shard shard,
      Schema schema,
      PubSubConsumerProfile pubSubConsumerProfile,
      String sourceDbTimezoneOffset) {
    this.shard = shard;
    this.kafkaConnectionProfile = null;
    this.schema = schema;
    this.bufferType = "pubsub";
    this.pubSubConsumerProfile = pubSubConsumerProfile;
    this.sourceDbTimezoneOffset = sourceDbTimezoneOffset;
  }

  public KafkaConnectionProfile getKafkaConnectionProfile() {
    return kafkaConnectionProfile;
  }

  public Shard getShard() {
    return shard;
  }

  public Schema getSchema() {
    return schema;
  }

  public PubSubConsumerProfile getPubSubConsumerProfile() {
    return pubSubConsumerProfile;
  }

  public String getBufferType() {
    return bufferType;
  }

  public String getSourceDbTimezoneOffset() {
    return sourceDbTimezoneOffset;
  }

  @Override
  public String toString() {
    if ("Kafka".equals(bufferType)) {
      return "{ Shard details :"
          + shard.toString()
          + " kakfaClusterDetails: "
          + kafkaConnectionProfile.toString()
          + " schema: "
          + schema.toString()
          + "}";
    } else {
      return "{ Shard details :"
          + shard.toString()
          + " PubSubConsumerProfile: "
          + pubSubConsumerProfile
          + " schema: "
          + schema.toString()
          + "}";
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof ProcessingContext)) {
      return false;
    }
    final ProcessingContext other = (ProcessingContext) o;
    return this.getShard().equals(other.getShard());
  }
}
