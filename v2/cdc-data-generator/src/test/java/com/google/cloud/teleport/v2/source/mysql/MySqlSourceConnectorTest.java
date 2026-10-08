/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.source.mysql;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.templates.CdcDataGeneratorOptions.SinkType;
import com.google.cloud.teleport.v2.templates.model.SinkConfig;
import com.google.cloud.teleport.v2.templates.sink.DataWriter;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for {@link MySqlSourceConnector}. */
@RunWith(JUnit4.class)
public class MySqlSourceConnectorTest {

  @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

  private final MySqlSourceConnector connector = new MySqlSourceConnector();

  @Test
  public void testGetSourceTypeAndSinkType() {
    assertThat(connector.getSourceType()).isEqualTo("MYSQL");
    assertThat(connector.getSinkType()).isEqualTo(SinkType.MYSQL);
  }

  @Test
  public void testParseConfig_nullOrEmptyThrows() {
    assertThrows(IllegalArgumentException.class, () -> connector.parseConfig(null));
    assertThrows(IllegalArgumentException.class, () -> connector.parseConfig(""));
  }

  @Test
  public void testParseConfig_success() throws Exception {
    String mysqlJson =
        "[\n"
            + "  {\n"
            + "    \"logicalShardId\": \"shardA\",\n"
            + "    \"host\": \"localhost\",\n"
            + "    \"port\": \"3306\",\n"
            + "    \"user\": \"root\",\n"
            + "    \"password\": \"pass\",\n"
            + "    \"dbName\": \"dbA\"\n"
            + "  },\n"
            + "  {\n"
            + "    \"host\": \"localhost\",\n"
            + "    \"port\": \"3306\",\n"
            + "    \"user\": \"root\",\n"
            + "    \"password\": \"pass\",\n"
            + "    \"dbName\": \"dbB\"\n"
            + "  }\n"
            + "]";
    File file = tempFolder.newFile("mysql_shards.json");
    FileUtils.writeStringToFile(file, mysqlJson, StandardCharsets.UTF_8);

    SinkConfig config = connector.parseConfig(file.getAbsolutePath());
    assertThat(config).isInstanceOf(MySqlSinkConfig.class);
    MySqlSinkConfig mysqlConfig = (MySqlSinkConfig) config;
    assertThat(mysqlConfig.getShards()).hasSize(2);
    assertThat(mysqlConfig.getShards().get(0).getLogicalShardId()).isEqualTo("shard0");
    assertThat(mysqlConfig.getShards().get(1).getLogicalShardId()).isEqualTo("shardA");
  }

  @Test
  public void testParseConfig_emptyShardsThrows() throws IOException {
    File file = tempFolder.newFile("empty_shards.json");
    FileUtils.writeStringToFile(file, "[]", StandardCharsets.UTF_8);

    assertThrows(RuntimeException.class, () -> connector.parseConfig(file.getAbsolutePath()));
  }

  @Test
  public void testGetSchemaFetcherAndTypeMapper() {
    assertThat(connector.getSchemaFetcher()).isInstanceOf(MySqlSchemaFetcher.class);
    assertThat(connector.getTypeMapper()).isInstanceOf(MySqlTypeMapper.class);
  }

  @Test
  public void testCreateDataWriter() {
    Shard shard = new Shard("shard0", "host", "user", "pass", "port", "db", null, null, null);
    MySqlSinkConfig config = new MySqlSinkConfig(ImmutableList.of(shard));
    DataWriter writer = connector.createDataWriter(config);
    assertThat(writer).isInstanceOf(MySqlDataWriter.class);
  }

  @Test
  public void testCreateDataWriter_invalidConfigThrows() {
    assertThrows(
        IllegalArgumentException.class,
        () -> connector.createDataWriter(new SinkConfig() {}));
  }

  @Test
  public void testResolveKeyParallelism() {
    Shard shard1 = new Shard("shard1", "host", "user", "pass", "port", "db", null, null, null);
    Shard shard2 = new Shard("shard2", "host", "user", "pass", "port", "db", null, null, null);

    MySqlSinkConfig twoShards = new MySqlSinkConfig(ImmutableList.of(shard1, shard2));
    assertThat(connector.resolveKeyParallelism(twoShards)).isEqualTo(10000);

    MySqlSinkConfig nullShards = new MySqlSinkConfig(null);
    assertThat(connector.resolveKeyParallelism(nullShards)).isEqualTo(5000);

    MySqlSinkConfig emptyShards = new MySqlSinkConfig(Collections.emptyList());
    assertThat(connector.resolveKeyParallelism(emptyShards)).isEqualTo(5000);
  }

  @Test
  public void testResolveKeyParallelism_invalidConfigThrows() {
    assertThrows(
        IllegalArgumentException.class,
        () -> connector.resolveKeyParallelism(new SinkConfig() {}));
  }

  @Test
  public void testGetLogicalShardIds() {
    Shard shard1 = new Shard("shardA", "host", "user", "pass", "port", "db", null, null, null);
    Shard shard2 = new Shard("shardB", "host", "user", "pass", "port", "db", null, null, null);

    MySqlSinkConfig config = new MySqlSinkConfig(ImmutableList.of(shard1, shard2));
    List<String> shardIds = connector.getLogicalShardIds(config);
    assertThat(shardIds).containsExactly("shardA", "shardB").inOrder();

    assertThat(connector.getLogicalShardIds(new MySqlSinkConfig(null))).isEmpty();
    assertThat(connector.getLogicalShardIds(null)).isEmpty();
  }
}
