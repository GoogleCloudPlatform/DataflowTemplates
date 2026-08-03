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
package com.google.cloud.teleport.v2.templates.source.postgres;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.spanner.migrations.connection.ConnectionHelperRequest;
import com.google.cloud.teleport.v2.spanner.migrations.connection.IConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.IDao;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.JdbcDao;
import com.google.cloud.teleport.v2.templates.dbutils.dml.IDMLGenerator;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PostgreSQLSpToSrcSourceConnectorTest {

  @org.junit.Rule
  public org.junit.rules.TemporaryFolder tempFolder = new org.junit.rules.TemporaryFolder();

  @Mock private IConnectionHelper mockConnectionHelper;
  @Mock private Shard mockShard;

  private PostgreSQLSpToSrcSourceConnector connector;

  @Before
  public void setUp() {
    connector = new PostgreSQLSpToSrcSourceConnector(mockConnectionHelper);
  }

  @Test
  public void testGetDmlGenerator() {
    IDMLGenerator dmlGenerator = connector.getDmlGenerator();
    assertNotNull(dmlGenerator);
    assertTrue(dmlGenerator instanceof PostgreSQLDMLGenerator);
  }

  @Test
  public void testGetConnectionHelper() {
    assertEquals(mockConnectionHelper, connector.getConnectionHelper());
  }

  @Test
  public void testGetConnectionUrl() {
    when(mockShard.getHost()).thenReturn("localhost");
    when(mockShard.getPort()).thenReturn("5432");
    when(mockShard.getDbName()).thenReturn("mydb");

    String url = connector.getConnectionUrl(mockShard);
    assertEquals("jdbc:postgresql://localhost:5432/mydb", url);
  }

  @Test
  public void testGetDao() {
    when(mockShard.getHost()).thenReturn("localhost");
    when(mockShard.getPort()).thenReturn("5432");
    when(mockShard.getDbName()).thenReturn("mydb");
    when(mockShard.getUserName()).thenReturn("user");

    IDao dao = connector.getDao(mockShard);
    assertNotNull(dao);
    assertTrue(dao instanceof JdbcDao);
  }

  @Test
  public void testInitConnectionHelper() {
    List<Shard> shards = Collections.singletonList(mockShard);
    int maxConnections = 10;

    when(mockConnectionHelper.isConnectionPoolInitialized()).thenReturn(false);

    connector.initConnectionHelper(shards, maxConnections);

    ArgumentCaptor<ConnectionHelperRequest> requestCaptor =
        ArgumentCaptor.forClass(ConnectionHelperRequest.class);
    verify(mockConnectionHelper).init(requestCaptor.capture());

    ConnectionHelperRequest request = requestCaptor.getValue();
    assertEquals(shards, request.getShards());
    assertEquals(maxConnections, request.getMaxConnections());
    assertEquals("org.postgresql.Driver", request.getDriver());
    assertEquals(null, request.getConnectionInitQuery());
    assertEquals("jdbc:postgresql://", request.getJdbcUrlPrefix());
  }

  @Test
  public void testInitConnectionHelper_alreadyInitialized() {
    List<Shard> shards = Collections.singletonList(mockShard);
    int maxConnections = 10;

    when(mockConnectionHelper.isConnectionPoolInitialized()).thenReturn(true);

    connector.initConnectionHelper(shards, maxConnections);

    verify(mockConnectionHelper, never()).init(any());
  }

  @Test
  public void testClassifyException() {
    Throwable syntaxEx = new java.sql.SQLSyntaxErrorException("syntax error");
    org.junit.Assert.assertNull(connector.classifyException(syntaxEx));

    Throwable dataEx = new java.sql.SQLDataException("data error");
    org.junit.Assert.assertNull(connector.classifyException(dataEx));

    Throwable connEx = new java.sql.SQLNonTransientConnectionException("conn error");
    org.junit.Assert.assertNull(connector.classifyException(connEx));

    Throwable genericEx = new RuntimeException("generic error");
    org.junit.Assert.assertNull(connector.classifyException(genericEx));
  }

  @Test
  public void testGetInformationSchema() throws Exception {
    java.sql.Connection mockConnection = mock(java.sql.Connection.class);
    when(mockShard.getDbName()).thenReturn("mydb");
    when(mockShard.getNamespace()).thenReturn("public");
    PostgreSQLSpToSrcSourceConnector spyConnector = spy(connector);
    doReturn(mockConnection).when(spyConnector).createConnection(mockShard);

    com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema dummySchema =
        com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema.builder(
                com.google.cloud.teleport.v2.spanner.sourceddl.SourceDatabaseType.POSTGRESQL)
            .databaseName("mydb")
            .tables(com.google.common.collect.ImmutableMap.of())
            .build();

    try (org.mockito.MockedConstruction<
            com.google.cloud.teleport.v2.spanner.sourceddl.PostgreSQLInformationSchemaScanner>
        mocked =
            org.mockito.Mockito.mockConstruction(
                com.google.cloud.teleport.v2.spanner.sourceddl.PostgreSQLInformationSchemaScanner
                    .class,
                (mock, context) -> {
                  when(mock.scan()).thenReturn(dummySchema);
                })) {

      com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema result =
          spyConnector.getInformationSchema(List.of(mockShard));
      assertEquals(dummySchema, result);
    }
  }

  @Test
  public void testParseShardList_validJsonWrapped() throws Exception {
    java.io.File tempFile = tempFolder.newFile("jdbc-config-wrapped.json");
    String wrappedJson =
        "{\n"
            + "  \"shardConfigs\": [\n"
            + "    {\n"
            + "      \"logicalShardId\": \"shard1\",\n"
            + "      \"host\": \"localhost\",\n"
            + "      \"port\": \"5432\",\n"
            + "      \"user\": \"test-user\",\n"
            + "      \"password\": \"secret-pass\",\n"
            + "      \"dbName\": \"testdb\",\n"
            + "      \"namespace\": \"public\"\n"
            + "    }\n"
            + "  ]\n"
            + "}";
    java.nio.file.Files.writeString(tempFile.toPath(), wrappedJson);

    List<Shard> shards = connector.parseShardConfig(tempFile.getAbsolutePath());
    assertNotNull(shards);
    assertEquals(1, shards.size());
    Shard shard = shards.get(0);
    assertEquals("shard1", shard.getLogicalShardId());
    assertEquals("localhost", shard.getHost());
    assertEquals("5432", shard.getPort());
    assertEquals("test-user", shard.getUserName());
    assertEquals("secret-pass", shard.getPassword());
    assertEquals("testdb", shard.getDbName());
    assertEquals("public", shard.getNamespace());
  }

  @Test
  public void testSupportsSharding() {
    assertTrue(connector.supportsSharding());
  }

  @Test
  public void testShouldUpdateReadValuesToSpannerRecord() {
    assertTrue(connector.shouldUpdateReadValuesToSpannerRecord());
  }
}
