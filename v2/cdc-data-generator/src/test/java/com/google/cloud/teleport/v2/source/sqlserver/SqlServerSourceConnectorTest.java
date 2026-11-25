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
package com.google.cloud.teleport.v2.source.sqlserver;

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

/** Test class for {@link SqlServerSourceConnector}. */
@RunWith(JUnit4.class)
public class SqlServerSourceConnectorTest {

  @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

  private final SqlServerSourceConnector connector = new SqlServerSourceConnector();

  private static String shardJson(String logicalShardIdLine, String dbName) {
    return "  {\n"
        + logicalShardIdLine
        + "    \"host\": \"localhost\",\n"
        + "    \"port\": \"1433\",\n"
        + "    \"user\": \"sqlserver\",\n"
        + "    \"password\": \"pass\",\n"
        + "    \"dbName\": \""
        + dbName
        + "\"\n"
        + "  }";
  }

  @Test
  public void testGetSourceTypeAndSinkType() {
    assertThat(connector.getSourceType()).isEqualTo("SQLSERVER");
    assertThat(connector.getSinkType()).isEqualTo(SinkType.SQLSERVER);
  }

  @Test
  public void testParseConfig_nullOrEmptyThrows() {
    assertThrows(IllegalArgumentException.class, () -> connector.parseConfig(null));
    assertThrows(IllegalArgumentException.class, () -> connector.parseConfig(""));
  }

  @Test
  public void testParseConfig_success() throws Exception {
    String json =
        "[\n"
            + shardJson("    \"logicalShardId\": \"shardA\",\n", "db1")
            + ",\n"
            + shardJson("", "db2")
            + "\n]";
    File file = tempFolder.newFile("sqlserver_shards.json");
    FileUtils.writeStringToFile(file, json, StandardCharsets.UTF_8);

    SinkConfig config = connector.parseConfig(file.getAbsolutePath());
    assertThat(config).isInstanceOf(SqlServerSinkConfig.class);
    SqlServerSinkConfig sqlServerConfig = (SqlServerSinkConfig) config;
    assertThat(sqlServerConfig.getShards()).hasSize(2);
    assertThat(sqlServerConfig.getShards().get(0).getLogicalShardId()).isEqualTo("shard0");
    assertThat(sqlServerConfig.getShards().get(1).getLogicalShardId()).isEqualTo("shardA");
  }

  @Test
  public void testParseConfig_emptyShardsThrows() throws IOException {
    File file = tempFolder.newFile("empty_shards.json");
    FileUtils.writeStringToFile(file, "[]", StandardCharsets.UTF_8);

    assertThrows(RuntimeException.class, () -> connector.parseConfig(file.getAbsolutePath()));
  }

  @Test
  public void testParseConfig_avoidsDefaultShardIdCollision() throws Exception {
    String json =
        "[\n"
            + shardJson("    \"logicalShardId\": \"shard0\",\n", "db1")
            + ",\n"
            + shardJson("    \"logicalShardId\": \"\",\n", "db2")
            + "\n]";
    File file = tempFolder.newFile("sqlserver_shards_collision.json");
    FileUtils.writeStringToFile(file, json, StandardCharsets.UTF_8);

    SqlServerSinkConfig config =
        (SqlServerSinkConfig) connector.parseConfig(file.getAbsolutePath());
    assertThat(config.getShards()).hasSize(2);
    assertThat(config.getShards().get(0).getLogicalShardId()).isEqualTo("shard1");
    assertThat(config.getShards().get(1).getLogicalShardId()).isEqualTo("shard0");
  }

  @Test
  public void testParseConfig_duplicateExplicitShardIdsThrows() throws Exception {
    String json =
        "[\n"
            + shardJson("    \"logicalShardId\": \"shardA\",\n", "db1")
            + ",\n"
            + shardJson("    \"logicalShardId\": \"shardA\",\n", "db2")
            + "\n]";
    File file = tempFolder.newFile("sqlserver_shards_duplicate.json");
    FileUtils.writeStringToFile(file, json, StandardCharsets.UTF_8);

    assertThrows(
        IllegalArgumentException.class, () -> connector.parseConfig(file.getAbsolutePath()));
  }

  @Test
  public void testGetSchemaFetcherAndTypeMapper() {
    assertThat(connector.getSchemaFetcher()).isInstanceOf(SqlServerSchemaFetcher.class);
    assertThat(connector.getTypeMapper()).isInstanceOf(SqlServerTypeMapper.class);
  }

  @Test
  public void testCreateDataWriter() {
    Shard shard = new Shard("shard0", "host", "1433", "user", "pass", "db", null, null, null);
    DataWriter writer =
        connector.createDataWriter(new SqlServerSinkConfig(ImmutableList.of(shard)));
    assertThat(writer).isInstanceOf(SqlServerDataWriter.class);
  }

  @Test
  public void testCreateDataWriter_invalidConfigThrows() {
    assertThrows(
        IllegalArgumentException.class, () -> connector.createDataWriter(new SinkConfig() {}));
  }

  @Test
  public void testResolveKeyParallelism() {
    Shard shard1 = new Shard("shard1", "host", "1433", "user", "pass", "db", null, null, null);
    Shard shard2 = new Shard("shard2", "host", "1433", "user", "pass", "db", null, null, null);

    assertThat(
            connector.resolveKeyParallelism(
                new SqlServerSinkConfig(ImmutableList.of(shard1, shard2))))
        .isEqualTo(10000);
    assertThat(connector.resolveKeyParallelism(new SqlServerSinkConfig(null))).isEqualTo(5000);
    assertThat(connector.resolveKeyParallelism(new SqlServerSinkConfig(Collections.emptyList())))
        .isEqualTo(5000);
  }

  @Test
  public void testResolveKeyParallelism_invalidConfigThrows() {
    assertThrows(
        IllegalArgumentException.class, () -> connector.resolveKeyParallelism(new SinkConfig() {}));
  }

  @Test
  public void testGetLogicalShardIds() {
    Shard shard1 = new Shard("shardA", "host", "1433", "user", "pass", "db", null, null, null);
    Shard shard2 = new Shard("shardB", "host", "1433", "user", "pass", "db", null, null, null);

    List<String> shardIds =
        connector.getLogicalShardIds(new SqlServerSinkConfig(ImmutableList.of(shard1, shard2)));
    assertThat(shardIds).containsExactly("shardA", "shardB").inOrder();

    assertThat(connector.getLogicalShardIds(new SqlServerSinkConfig(null))).isEmpty();
    assertThat(connector.getLogicalShardIds(null)).isEmpty();
  }
}
