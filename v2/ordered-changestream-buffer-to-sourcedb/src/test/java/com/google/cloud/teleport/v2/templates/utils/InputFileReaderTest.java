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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.v2.templates.common.Shard;
import com.google.cloud.teleport.v2.templates.kafka.KafkaConnectionProfile;
import com.google.cloud.teleport.v2.templates.schema.Schema;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class InputFileReaderTest {

  @Test
  public void shardFileReading() {
    List<Shard> shards =
        InputFileReader.getOrderedShardDetails("src/test/resources/shard.json", "mysql");
    List<Shard> expectedShards =
        Arrays.asList(
            new Shard("shardA", "hostShardA", "3306", "test", "test", "test"),
            new Shard("shardB", "hostShardB", "3306", "test", "test", "test"));

    assertEquals(shards, expectedShards);
  }

  @Test
  public void shardFileReadingSourceTypeException() {
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                InputFileReader.getOrderedShardDetails(
                    "src/test/resources/shard.json", "somejunk"));
    assertTrue(thrown.getMessage().contains("Supported values are : mysql"));
  }

  @Test
  public void shardFileReadingFileNotExists() {
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                InputFileReader.getOrderedShardDetails(
                    "src/test/resources/somemissingfile.json", "mysql"));
    assertTrue(thrown.getMessage().contains("Failed to read shard input file"));
  }

  @Test
  public void kafkaConnectionFileRead() {
    KafkaConnectionProfile kafkaConnectionProfile =
        InputFileReader.getKafkaConnectionProfile("src/test/resources/kafkaConnectionProfile.json");

    assertEquals(kafkaConnectionProfile.getBootstrapServer(), "thewall");
    assertEquals(kafkaConnectionProfile.getDataTopic(), "winterfell");
    assertEquals(kafkaConnectionProfile.getErrorTopic(), "redkeep");
  }

  @Test
  public void kafkaConnectionFileReadFileNotExist() {
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                InputFileReader.getKafkaConnectionProfile(
                    "src/test/resources/somemisingfile.json"));
    assertTrue(thrown.getMessage().contains("Failed to read kafka cluster input file"));
  }

  @Test
  public void schemaFileRead() {
    Schema schema = InputFileReader.getSchema("src/test/resources/allMatchSession.json");

    assertEquals(schema.getSpannerSchema().size(), 4);
    assertEquals(schema.getSourceSchema().size(), 4);
  }

  @Test
  public void schemaFileReadFileNotExist() {

    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () -> InputFileReader.getSchema("src/test/resources/somemisingfile.json"));
    assertTrue(thrown.getMessage().contains("Failed to read Schema file input file"));
  }
}
