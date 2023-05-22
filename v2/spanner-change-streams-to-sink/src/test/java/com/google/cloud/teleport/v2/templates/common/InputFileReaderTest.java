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

import static org.junit.Assert.assertEquals;

import com.google.cloud.teleport.v2.templates.sinks.KafkaConnectionProfile;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

/** Tests for InputFileReaderTest class. */
public class InputFileReaderTest {

  @Test
  public void getOrderedShardDetailsTest() throws Exception {
    List<Shard> shards =
        (new InputFileReader()).getOrderedShardDetails("src/test/resources/sharding-cfg.json");
    List<Shard> expectedShards =
        Arrays.asList(
            new Shard("shardA", "10.240.0.204", "3306", "test", "test", "test"),
            new Shard("shardB", "10.240.0.205", "3306", "test2", "test2", "test2"));

    assertEquals(shards, expectedShards);
  }

  @Test
  public void getKafkaConnectionProfileTest() throws Exception {
    KafkaConnectionProfile kafkaConn =
        (new InputFileReader()).getKafkaConnectionProfile("src/test/resources/kafka-cfg.json");
    KafkaConnectionProfile expectedKafkaConn =
        new KafkaConnectionProfile("10.240.0.204:9092", "dataTopic", "errorTopic");

    assertEquals(kafkaConn.getBootstrapServer(), expectedKafkaConn.getBootstrapServer());
    assertEquals(kafkaConn.getDataTopic(), expectedKafkaConn.getDataTopic());
    assertEquals(kafkaConn.getErrorTopic(), expectedKafkaConn.getErrorTopic());
  }
}
