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
package com.google.cloud.teleport.v2.source.oracle;

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

/** Test class for {@link OracleSourceConnector}. */
@RunWith(JUnit4.class)
public class OracleSourceConnectorTest {

  @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

  private final OracleSourceConnector connector = new OracleSourceConnector();

  @Test
  public void testGetSourceTypeAndSinkType() {
    assertThat(connector.getSourceType()).isEqualTo("ORACLE");
    assertThat(connector.getSinkType()).isEqualTo(SinkType.ORACLE);
  }

  @Test
  public void testParseConfig_nullOrEmptyThrows() {
    assertThrows(IllegalArgumentException.class, () -> connector.parseConfig(null));
    assertThrows(IllegalArgumentException.class, () -> connector.parseConfig(""));
  }

  @Test
  public void testParseConfig_success() throws Exception {
    String oracleJson =
        "[\n"
            + "  {\n"
            + "    \"logicalShardId\": \"shardA\",\n"
            + "    \"host\": \"localhost\",\n"
            + "    \"port\": \"1521\",\n"
            + "    \"user\": \"c##datastream\",\n"
            + "    \"password\": \"datastream\",\n"
            + "    \"dbName\": \"XEPDB1\"\n"
            + "  },\n"
            + "  {\n"
            + "    \"host\": \"localhost\",\n"
            + "    \"port\": \"1521\",\n"
            + "    \"user\": \"c##datastream\",\n"
            + "    \"password\": \"datastream\",\n"
            + "    \"dbName\": \"XEPDB2\"\n"
            + "  }\n"
            + "]";
    File file = tempFolder.newFile("oracle_shards.json");
    FileUtils.writeStringToFile(file, oracleJson, StandardCharsets.UTF_8);

    SinkConfig config = connector.parseConfig(file.getAbsolutePath());
    assertThat(config).isInstanceOf(OracleSinkConfig.class);
    OracleSinkConfig oracleConfig = (OracleSinkConfig) config;
    assertThat(oracleConfig.getShards()).hasSize(2);
    assertThat(oracleConfig.getShards().get(0).getLogicalShardId()).isEqualTo("shard0");
    assertThat(oracleConfig.getShards().get(1).getLogicalShardId()).isEqualTo("shardA");
  }

  @Test
  public void testParseConfig_emptyShardsThrows() throws IOException {
    File file = tempFolder.newFile("empty_shards.json");
    FileUtils.writeStringToFile(file, "[]", StandardCharsets.UTF_8);

    assertThrows(RuntimeException.class, () -> connector.parseConfig(file.getAbsolutePath()));
  }

  @Test
  public void testGetSchemaFetcherAndTypeMapper() {
    assertThat(connector.getSchemaFetcher()).isInstanceOf(OracleSchemaFetcher.class);
    assertThat(connector.getTypeMapper()).isInstanceOf(OracleTypeMapper.class);
  }

  @Test
  public void testCreateDataWriter() {
    Shard shard = new Shard("shard0", "host", "1521", "user", "pass", "XEPDB1", null, null, null);
    OracleSinkConfig config = new OracleSinkConfig(ImmutableList.of(shard));
    DataWriter writer = connector.createDataWriter(config);
    assertThat(writer).isInstanceOf(OracleDataWriter.class);
  }

  @Test
  public void testCreateDataWriter_invalidConfigThrows() {
    assertThrows(
        IllegalArgumentException.class, () -> connector.createDataWriter(new SinkConfig() {}));
  }

  @Test
  public void testResolveKeyParallelism() {
    Shard shard1 = new Shard("shard1", "host", "1521", "user", "pass", "XEPDB1", null, null, null);
    Shard shard2 = new Shard("shard2", "host", "1521", "user", "pass", "XEPDB1", null, null, null);

    OracleSinkConfig twoShards = new OracleSinkConfig(ImmutableList.of(shard1, shard2));
    assertThat(connector.resolveKeyParallelism(twoShards)).isEqualTo(10000);

    OracleSinkConfig nullShards = new OracleSinkConfig(null);
    assertThat(connector.resolveKeyParallelism(nullShards)).isEqualTo(5000);

    OracleSinkConfig emptyShards = new OracleSinkConfig(Collections.emptyList());
    assertThat(connector.resolveKeyParallelism(emptyShards)).isEqualTo(5000);
  }

  @Test
  public void testResolveKeyParallelism_invalidConfigThrows() {
    assertThrows(
        IllegalArgumentException.class, () -> connector.resolveKeyParallelism(new SinkConfig() {}));
  }

  @Test
  public void testGetLogicalShardIds() {
    Shard shard1 = new Shard("shardA", "host", "1521", "user", "pass", "XEPDB1", null, null, null);
    Shard shard2 = new Shard("shardB", "host", "1521", "user", "pass", "XEPDB1", null, null, null);

    OracleSinkConfig config = new OracleSinkConfig(ImmutableList.of(shard1, shard2));
    List<String> shardIds = connector.getLogicalShardIds(config);
    assertThat(shardIds).containsExactly("shardA", "shardB").inOrder();

    assertThat(connector.getLogicalShardIds(new OracleSinkConfig(null))).isEmpty();
    assertThat(connector.getLogicalShardIds(null)).isEmpty();
  }
}
