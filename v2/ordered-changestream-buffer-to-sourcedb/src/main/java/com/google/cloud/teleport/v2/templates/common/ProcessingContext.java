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

import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.templates.kafka.KafkaConnectionProfile;
import com.google.cloud.teleport.v2.templates.pubsub.PubSubConsumerProfile;
import java.io.Serializable;
import java.util.Objects;

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
  private String sourceDbType;

  private Boolean enableSourceDbSsl;

  private Boolean enableSourceDbSslValidation;

  public ProcessingContext(
      KafkaConnectionProfile kafkaConnectionProfile,
      Shard shard,
      Schema schema,
      String sourceDbTimezoneOffset,
      String sourceDbType,
      Boolean enableSourceDbSsl,
      Boolean enableSourceDbSslValidation) {
    this.shard = shard;
    this.kafkaConnectionProfile = kafkaConnectionProfile;
    this.schema = schema;
    this.bufferType = "kafka";
    this.sourceDbType = sourceDbType;
    this.pubSubConsumerProfile = null;
    this.sourceDbTimezoneOffset = sourceDbTimezoneOffset;
    this.enableSourceDbSsl = enableSourceDbSsl;
    this.enableSourceDbSslValidation = enableSourceDbSslValidation;
  }

  public ProcessingContext(
      Shard shard,
      Schema schema,
      PubSubConsumerProfile pubSubConsumerProfile,
      String sourceDbTimezoneOffset,
      String sourceDbType,
      Boolean enableSourceDbSsl,
      Boolean enableSourceDbSslValidation) {
    this.shard = shard;
    this.kafkaConnectionProfile = null;
    this.schema = schema;
    this.bufferType = "pubsub";
    this.sourceDbType = sourceDbType;
    this.pubSubConsumerProfile = pubSubConsumerProfile;
    this.sourceDbTimezoneOffset = sourceDbTimezoneOffset;
    this.enableSourceDbSsl = enableSourceDbSsl;
    this.enableSourceDbSslValidation = enableSourceDbSslValidation;
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

  public String getSourceDbType() {
    return sourceDbType;
  }

  public Boolean getEnableSourceDbSsl() {
    return enableSourceDbSsl;
  }

  public Boolean getEnableSourceDbSslValidation() {
    return enableSourceDbSslValidation;
  }

  @Override
  public String toString() {
    if ("Kafka".equals(bufferType)) {
      return "{ Shard details :"
          + shard.toString()
          + " kafkaClusterDetails: "
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

  @Override
  public int hashCode() {
    return Objects.hash(getShard());
  }
}
