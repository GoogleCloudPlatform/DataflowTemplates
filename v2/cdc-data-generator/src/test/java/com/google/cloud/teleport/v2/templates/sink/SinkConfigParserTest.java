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
package com.google.cloud.teleport.v2.templates.sink;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.v2.templates.CdcDataGeneratorOptions.SinkType;
import com.google.cloud.teleport.v2.templates.model.MySqlSinkConfig;
import com.google.cloud.teleport.v2.templates.model.SinkConfig;
import com.google.cloud.teleport.v2.templates.model.SpannerSinkConfig;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SinkConfigParserTest {

  @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

  @After
  public void tearDown() {
    // Reset the static dialect provider after each test to avoid leaking mock state
    SinkConfigParser.setDialectProvider(null);
  }

  @Test
  public void testParse_nullOptionsPath_throwsException() {
    assertThrows(
        IllegalArgumentException.class, () -> SinkConfigParser.parse(SinkType.SPANNER, null));
  }

  @Test
  public void testParse_emptyOptionsPath_throwsException() {
    assertThrows(
        IllegalArgumentException.class, () -> SinkConfigParser.parse(SinkType.SPANNER, ""));
  }

  @Test
  public void testParse_unsupportedSinkType_throwsException() throws IOException {
    File file = tempFolder.newFile("dummy.json");
    FileUtils.writeStringToFile(file, "{}", StandardCharsets.UTF_8);

    assertThrows(
        IllegalArgumentException.class, () -> SinkConfigParser.parse(null, file.getAbsolutePath()));
  }

  @Test
  public void testParse_spannerSink_success() throws Exception {
    String spannerJson =
        "{\n"
            + "  \"projectId\": \"sp-project\",\n"
            + "  \"instanceId\": \"sp-instance\",\n"
            + "  \"databaseId\": \"sp-database\"\n"
            + "}";
    File file = tempFolder.newFile("spanner_config.json");
    FileUtils.writeStringToFile(file, spannerJson, StandardCharsets.UTF_8);

    // Use a mock DialectProvider to bypass static SpannerAccessor calls
    SinkConfigParser.setDialectProvider((projectId, instanceId, databaseId) -> Dialect.POSTGRESQL);

    SinkConfig config = SinkConfigParser.parse(SinkType.SPANNER, file.getAbsolutePath());

    assertThat(config).isInstanceOf(SpannerSinkConfig.class);
    SpannerSinkConfig spannerConfig = (SpannerSinkConfig) config;
    assertThat(spannerConfig.getProjectId()).isEqualTo("sp-project");
    assertThat(spannerConfig.getInstanceId()).isEqualTo("sp-instance");
    assertThat(spannerConfig.getDatabaseId()).isEqualTo("sp-database");
    assertThat(spannerConfig.getDialect()).isEqualTo(Dialect.POSTGRESQL);
  }

  @Test
  public void testParse_mysqlSink_success() throws Exception {
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
            + "    \"logicalShardId\": \"shardB\",\n"
            + "    \"host\": \"localhost\",\n"
            + "    \"port\": \"3306\",\n"
            + "    \"user\": \"root\",\n"
            + "    \"password\": \"pass\",\n"
            + "    \"dbName\": \"dbB\"\n"
            + "  }\n"
            + "]";
    File file = tempFolder.newFile("mysql_shards.json");
    FileUtils.writeStringToFile(file, mysqlJson, StandardCharsets.UTF_8);

    SinkConfig config = SinkConfigParser.parse(SinkType.MYSQL, file.getAbsolutePath());

    assertThat(config).isInstanceOf(MySqlSinkConfig.class);
    MySqlSinkConfig mysqlConfig = (MySqlSinkConfig) config;
    assertThat(mysqlConfig.getShards()).hasSize(2);
    assertThat(mysqlConfig.getShards().get(0).getLogicalShardId()).isEqualTo("shardA");
    assertThat(mysqlConfig.getShards().get(1).getLogicalShardId()).isEqualTo("shardB");
  }

  @Test
  public void testParse_mysqlSink_emptyShards_throwsException() throws IOException {
    File file = tempFolder.newFile("empty_shards.json");
    FileUtils.writeStringToFile(file, "[]", StandardCharsets.UTF_8);

    assertThrows(
        RuntimeException.class,
        () -> SinkConfigParser.parse(SinkType.MYSQL, file.getAbsolutePath()));
  }
}
