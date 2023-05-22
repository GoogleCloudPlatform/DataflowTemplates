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

import java.io.Serializable;

/**
 * Each woker task context has a the kafka Partition to read/write, the data and error topic, kafka
 * broker List the connection detail of the database to write records to.
 */
public class ProcessingContext implements Serializable {

  private KafkaConnectionProfile kafkaConnectionProfile;
  private Shard shard;
  private Schema schema;

  public ProcessingContext(
      KafkaConnectionProfile kafkaConnectionProfile, Shard shard, Schema schema) {
    this.shard = shard;
    this.kafkaConnectionProfile = kafkaConnectionProfile;
    this.schema = schema;
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

  @Override
  public String toString() {
    return "{ Shard details :"
        + shard.toString()
        + " kakfaClusterDetails: "
        + kafkaConnectionProfile.toString()
        + " schema: "
        + schema.toString()
        + "}";
  }
}
