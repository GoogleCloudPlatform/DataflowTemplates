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
package com.google.cloud.teleport.v2.spanner.migrations.source.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.utils.ISecretManagerAccessor;
import com.google.cloud.teleport.v2.spanner.migrations.utils.JarFileReader;
import com.typesafe.config.ConfigException;
import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.MockedStatic;

@RunWith(JUnit4.class)
public class SourceConfigParserTest {

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private ISecretManagerAccessor mockSecretManagerAccessor;
  private SourceConfigParser parser;

  @BeforeClass
  public static void setupFileSystem() {
    FileSystems.setDefaultPipelineOptions(TestPipeline.testingPipelineOptions());
  }

  @Before
  public void setUp() {
    mockSecretManagerAccessor = mock(ISecretManagerAccessor.class);
    parser = new SourceConfigParser(mockSecretManagerAccessor);
  }

  @Test
  public void testResolveShardSecret_Success() {
    Shard shard = new Shard();
    shard.setLogicalShardId("shard1");
    shard.setSecretManagerUri("projects/123/secrets/my-secret/versions/1");

    JdbcShardConfig mockConfig = mock(JdbcShardConfig.class);
    when(mockConfig.getShardConfigs()).thenReturn(Collections.singletonList(shard));

    when(mockSecretManagerAccessor.resolvePassword(
            "projects/123/secrets/my-secret/versions/1", "shard1", ""))
        .thenReturn("superSecretPassword");

    parser.resolveShardSecret(mockConfig, "dummy/path.json");

    assertEquals("superSecretPassword", shard.getPassword());
  }

  @Test
  public void testResolveShardSecret_MultipleShardsSuccess() {
    Shard shard1 = new Shard();
    shard1.setLogicalShardId("shard1");
    shard1.setSecretManagerUri("projects/123/secrets/my-secret/versions/1");

    Shard shard2 = new Shard();
    shard2.setLogicalShardId("shard2");
    shard2.setPassword("directPassword");

    JdbcShardConfig mockConfig = mock(JdbcShardConfig.class);
    when(mockConfig.getShardConfigs()).thenReturn(Arrays.asList(shard1, shard2));

    when(mockSecretManagerAccessor.resolvePassword(
            "projects/123/secrets/my-secret/versions/1", "shard1", ""))
        .thenReturn("superSecretPassword");

    when(mockSecretManagerAccessor.resolvePassword("", "shard2", "directPassword"))
        .thenReturn("directPassword");

    parser.resolveShardSecret(mockConfig, "dummy/path.json");

    assertEquals("superSecretPassword", shard1.getPassword());
    assertEquals("directPassword", shard2.getPassword());
  }

  @Test
  public void testResolveShardSecret_ThrowsExceptionOnEmptyPassword() {
    Shard shard = new Shard();
    shard.setLogicalShardId("shard2");

    JdbcShardConfig mockConfig = mock(JdbcShardConfig.class);
    when(mockConfig.getShardConfigs()).thenReturn(Collections.singletonList(shard));

    when(mockSecretManagerAccessor.resolvePassword(null, "shard2", null)).thenReturn("");

    RuntimeException exception =
        assertThrows(
            RuntimeException.class, () -> parser.resolveShardSecret(mockConfig, "dummy/path.json"));

    assertEquals(
        "Neither password nor secretManagerUri was found in the shard file dummy/path.json  for shard shard2",
        exception.getMessage());
  }

  @Test
  public void testResolveShardSecret_ThrowsExceptionOnNullPassword() {
    Shard shard = new Shard();
    shard.setLogicalShardId("shard3");

    JdbcShardConfig mockConfig = mock(JdbcShardConfig.class);
    when(mockConfig.getShardConfigs()).thenReturn(Collections.singletonList(shard));

    when(mockSecretManagerAccessor.resolvePassword(null, "shard3", null)).thenReturn(null);

    RuntimeException exception =
        assertThrows(
            RuntimeException.class, () -> parser.resolveShardSecret(mockConfig, "dummy/path.json"));

    assertEquals(
        "Neither password nor secretManagerUri was found in the shard file dummy/path.json  for shard shard3",
        exception.getMessage());
  }

  @Test
  public void testParseConfigToConfigMap_ValidJson() {
    String jsonContent = "{ \"host\": \"localhost\", \"port\": 3306 }";
    Map<String, Object> configMap = SourceConfigParser.parseConfigToConfigMap(jsonContent);

    assertEquals("localhost", configMap.get("host"));
    assertEquals(3306, configMap.get("port"));
  }

  @Test
  public void testParseConfigToConfigMap_ValidHocon() {
    String hoconContent = "host = localhost\nport = 3306\nnested { key = value }";
    Map<String, Object> configMap = SourceConfigParser.parseConfigToConfigMap(hoconContent);

    assertEquals("localhost", configMap.get("host"));
    assertEquals(3306, configMap.get("port"));

    @SuppressWarnings("unchecked")
    Map<String, Object> nested = (Map<String, Object>) configMap.get("nested");
    assertNotNull(nested);
    assertEquals("value", nested.get("key"));
  }

  @Test
  public void testParseConfigToConfigMap_HoconWithSubstitution() {
    String hoconContent = "defaultPort = 3306\nshard { port = ${defaultPort} }";
    Map<String, Object> configMap = SourceConfigParser.parseConfigToConfigMap(hoconContent);

    @SuppressWarnings("unchecked")
    Map<String, Object> shard = (Map<String, Object>) configMap.get("shard");
    assertEquals(3306, shard.get("port"));
  }

  @Test(expected = ConfigException.Parse.class)
  public void testParseConfigToConfigMap_InvalidContent() {
    String invalidContent = "{ invalid_json_missing_quotes: value ";
    SourceConfigParser.parseConfigToConfigMap(invalidContent);
  }

  @Test
  public void testParseConfiguration_Cassandra_Success() throws Exception {
    try (MockedStatic<JarFileReader> mockFileReader = mockStatic(JarFileReader.class)) {
      String testGcsPath = "gs://smt-test-bucket/cassandraConfig.conf";
      URL testUrl = com.google.common.io.Resources.getResource("test-cassandra-config.conf");
      mockFileReader
          .when(() -> JarFileReader.saveFilesLocally(testGcsPath))
          .thenReturn(new URL[] {testUrl});

      SourceConnectionConfig config = parser.parseConfiguration("cassandra", testGcsPath);
      assertNotNull(config);
      assertEquals(CassandraConnectionConfig.class, config.getClass());
    }
  }

  @Test
  public void testParseConfiguration_AstraDb_Success() throws Exception {
    File tempFile = tempFolder.newFile("astra-config.json");
    String astraJson =
        "{\n"
            + "  \"databaseId\": \"db-123\",\n"
            + "  \"astraToken\": \"token-xyz\",\n"
            + "  \"keySpace\": \"ks-abc\",\n"
            + "  \"astraDbRegion\": \"us-east1\"\n"
            + "}";
    Files.writeString(tempFile.toPath(), astraJson);

    SourceConnectionConfig config =
        parser.parseConfiguration("astra_db", tempFile.getAbsolutePath());
    assertNotNull(config);
    assertEquals(AstraConnectionConfig.class, config.getClass());
    AstraConnectionConfig astraConfig = (AstraConnectionConfig) config;
    assertEquals("db-123", astraConfig.getDatabaseId());
    assertEquals("token-xyz", astraConfig.getAstraToken());
    assertEquals("ks-abc", astraConfig.getKeySpace());
    assertEquals("us-east1", astraConfig.getAstraDbRegion());
  }

  @Test
  public void testParseConfiguration_Jdbc_Success() throws Exception {
    File tempFile = tempFolder.newFile("jdbc-config.json");
    String jdbcJson =
        "{\n"
            + "  \"shardConfigs\": [\n"
            + "    {\n"
            + "      \"logicalShardId\": \"shard1\",\n"
            + "      \"host\": \"10.0.0.1\",\n"
            + "      \"port\": \"3306\",\n"
            + "      \"user\": \"mysql-user\",\n"
            + "      \"password\": \"superSecretPassword\",\n"
            + "      \"dbName\": \"mydb\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"logicalShardId\": \"shard2\",\n"
            + "      \"host\": \"10.0.0.2\",\n"
            + "      \"port\": \"3306\",\n"
            + "      \"user\": \"mysql-user\",\n"
            + "      \"secretManagerUri\": \"projects/123/secrets/my-secret/versions/1\",\n"
            + "      \"dbName\": \"mydb\"\n"
            + "    }\n"
            + "  ]\n"
            + "}";
    Files.writeString(tempFile.toPath(), jdbcJson);

    when(mockSecretManagerAccessor.resolvePassword("", "shard1", "superSecretPassword"))
        .thenReturn("superSecretPassword");
    when(mockSecretManagerAccessor.resolvePassword(
            "projects/123/secrets/my-secret/versions/1", "shard2", ""))
        .thenReturn("resolvedPasswordFromSecretManager");

    // Test for MYSQL
    SourceConnectionConfig mysqlConfig =
        parser.parseConfiguration("mysql", tempFile.getAbsolutePath());
    assertNotNull(mysqlConfig);
    assertEquals(JdbcShardConfig.class, mysqlConfig.getClass());
    JdbcShardConfig mysqlJdbcConfig = (JdbcShardConfig) mysqlConfig;
    assertEquals(2, mysqlJdbcConfig.getShardConfigs().size());
    assertEquals("superSecretPassword", mysqlJdbcConfig.getShardConfigs().get(0).getPassword());
    assertEquals(
        "resolvedPasswordFromSecretManager",
        mysqlJdbcConfig.getShardConfigs().get(1).getPassword());

    // Test for PG
    SourceConnectionConfig pgConfig =
        parser.parseConfiguration("postgresql", tempFile.getAbsolutePath());
    assertNotNull(pgConfig);
    assertEquals(JdbcShardConfig.class, pgConfig.getClass());
    JdbcShardConfig pgJdbcConfig = (JdbcShardConfig) pgConfig;
    assertEquals(2, pgJdbcConfig.getShardConfigs().size());
    assertEquals("superSecretPassword", pgJdbcConfig.getShardConfigs().get(0).getPassword());
    assertEquals(
        "resolvedPasswordFromSecretManager", pgJdbcConfig.getShardConfigs().get(1).getPassword());
  }

  @Test
  public void testParseConfiguration_Jdbc_ShardConfigsOrdered() throws Exception {
    File tempFile = tempFolder.newFile("jdbc-ordered-config.json");
    String jdbcJson =
        "{\n"
            + "  \"shardConfigs\": [\n"
            + "    {\n"
            + "      \"logicalShardId\": \"shard3\",\n"
            + "      \"host\": \"10.0.0.3\",\n"
            + "      \"port\": \"3306\",\n"
            + "      \"user\": \"mysql-user\",\n"
            + "      \"password\": \"pass3\",\n"
            + "      \"dbName\": \"mydb\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"logicalShardId\": \"shard1\",\n"
            + "      \"host\": \"10.0.0.1\",\n"
            + "      \"port\": \"3306\",\n"
            + "      \"user\": \"mysql-user\",\n"
            + "      \"password\": \"pass1\",\n"
            + "      \"dbName\": \"mydb\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"logicalShardId\": \"shard2\",\n"
            + "      \"host\": \"10.0.0.2\",\n"
            + "      \"port\": \"3306\",\n"
            + "      \"user\": \"mysql-user\",\n"
            + "      \"password\": \"pass2\",\n"
            + "      \"dbName\": \"mydb\"\n"
            + "    }\n"
            + "  ]\n"
            + "}";
    Files.writeString(tempFile.toPath(), jdbcJson);

    when(mockSecretManagerAccessor.resolvePassword("", "shard1", "pass1")).thenReturn("pass1");
    when(mockSecretManagerAccessor.resolvePassword("", "shard2", "pass2")).thenReturn("pass2");
    when(mockSecretManagerAccessor.resolvePassword("", "shard3", "pass3")).thenReturn("pass3");

    SourceConnectionConfig config = parser.parseConfiguration("mysql", tempFile.getAbsolutePath());
    assertNotNull(config);
    JdbcShardConfig jdbcConfig = (JdbcShardConfig) config;
    assertEquals(3, jdbcConfig.getShardConfigs().size());
    assertEquals("shard1", jdbcConfig.getShardConfigs().get(0).getLogicalShardId());
    assertEquals("shard2", jdbcConfig.getShardConfigs().get(1).getLogicalShardId());
    assertEquals("shard3", jdbcConfig.getShardConfigs().get(2).getLogicalShardId());
  }

  @Test
  public void testParseConfiguration_MissingAndNullFieldsSetToEmptyString() throws Exception {
    File tempFile = tempFolder.newFile("jdbc-empty-fields-config.json");
    String jdbcJson =
        "{\n"
            + "  \"shardConfigs\": [\n"
            + "    {\n"
            + "      \"logicalShardId\": \"shard1\",\n"
            + "      \"host\": null,\n"
            + "      \"port\": \"3306\",\n"
            + "      \"user\": \"mysql-user\",\n"
            + "      \"password\": \"superSecretPassword\",\n"
            + "      \"dbName\": \"mydb\"\n"
            + "    }\n"
            + "  ]\n"
            + "}";
    Files.writeString(tempFile.toPath(), jdbcJson);

    when(mockSecretManagerAccessor.resolvePassword("", "shard1", "superSecretPassword"))
        .thenReturn("superSecretPassword");

    SourceConnectionConfig config = parser.parseConfiguration("mysql", tempFile.getAbsolutePath());
    assertNotNull(config);
    JdbcShardConfig jdbcConfig = (JdbcShardConfig) config;
    Shard shard = jdbcConfig.getShardConfigs().get(0);

    assertEquals("", shard.getHost());
    assertEquals("", shard.getNamespace());
    assertEquals("", shard.getSecretManagerUri());
    assertEquals("", shard.getConnectionProperties());
  }

  @Test
  public void testParseConfiguration_AstraDb_MissingAndNullFieldsSetToEmptyString()
      throws Exception {
    File tempFile = tempFolder.newFile("astra-empty-fields-config.json");
    String astraJson = "{\n" + "  \"databaseId\": \"db-123\",\n" + "  \"astraToken\": null\n" + "}";
    Files.writeString(tempFile.toPath(), astraJson);

    SourceConnectionConfig config =
        parser.parseConfiguration("astra_db", tempFile.getAbsolutePath());
    assertNotNull(config);
    AstraConnectionConfig astraConfig = (AstraConnectionConfig) config;
    assertEquals("db-123", astraConfig.getDatabaseId());
    assertEquals("", astraConfig.getAstraToken());
    assertEquals("", astraConfig.getKeySpace());
    assertEquals("", astraConfig.getAstraDbRegion());
  }

  @Test
  public void testParseConfiguration_UnsupportedSourceType() {
    Exception exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> parser.parseConfiguration("unsupported_db", "dummy/path.json"));
    assertEquals("Unsupported source type: unsupported_db", exception.getMessage());
  }
}
