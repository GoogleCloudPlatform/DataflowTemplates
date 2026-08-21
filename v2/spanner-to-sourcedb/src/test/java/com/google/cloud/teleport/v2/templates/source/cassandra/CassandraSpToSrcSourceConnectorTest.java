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
package com.google.cloud.teleport.v2.templates.source.cassandra;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
import com.google.cloud.teleport.v2.spanner.migrations.shard.CassandraShard;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.IDao;
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
public class CassandraSpToSrcSourceConnectorTest {

  @Mock private IConnectionHelper mockConnectionHelper;
  @Mock private CassandraShard mockCassandraShard;

  private CassandraSpToSrcSourceConnector connector;

  @Before
  public void setUp() {
    connector = new CassandraSpToSrcSourceConnector(mockConnectionHelper);
  }

  @Test
  public void testGetDmlGenerator() {
    IDMLGenerator dmlGenerator = connector.getDmlGenerator();
    assertNotNull(dmlGenerator);
    assertTrue(dmlGenerator instanceof CassandraDMLGenerator);
  }

  @Test
  public void testGetConnectionHelper() {
    assertEquals(mockConnectionHelper, connector.getConnectionHelper());
  }

  @Test
  public void testGetConnectionUrl() {
    when(mockCassandraShard.getHost()).thenReturn("localhost");
    when(mockCassandraShard.getPort()).thenReturn("9042");
    when(mockCassandraShard.getUserName()).thenReturn("cassandra");
    when(mockCassandraShard.getKeySpaceName()).thenReturn("mykeyspace");

    String url = connector.getConnectionUrl(mockCassandraShard);
    assertEquals("localhost:9042/cassandra/mykeyspace", url);
  }

  @Test
  public void testGetDao() {
    when(mockCassandraShard.getHost()).thenReturn("localhost");
    when(mockCassandraShard.getPort()).thenReturn("9042");
    when(mockCassandraShard.getUserName()).thenReturn("cassandra");
    when(mockCassandraShard.getKeySpaceName()).thenReturn("mykeyspace");

    IDao dao = connector.getDao(mockCassandraShard);
    assertNotNull(dao);
    assertTrue(dao instanceof CassandraDao);
  }

  @Test
  public void testInitConnectionHelper() {
    List<Shard> shards = Collections.singletonList(mockCassandraShard);
    int maxConnections = 10;

    when(mockConnectionHelper.isConnectionPoolInitialized()).thenReturn(false);

    connector.initConnectionHelper(shards, maxConnections);

    ArgumentCaptor<ConnectionHelperRequest> requestCaptor =
        ArgumentCaptor.forClass(ConnectionHelperRequest.class);
    verify(mockConnectionHelper).init(requestCaptor.capture());

    ConnectionHelperRequest request = requestCaptor.getValue();
    assertEquals(shards, request.getShards());
    assertEquals(maxConnections, request.getMaxConnections());
    assertEquals("com.datastax.oss.driver.api.core.CqlSession", request.getDriver());
    assertEquals(null, request.getConnectionInitQuery());
    assertEquals(null, request.getJdbcUrlPrefix());
  }

  @Test
  public void testInitConnectionHelper_alreadyInitialized() {
    List<Shard> shards = Collections.singletonList(mockCassandraShard);
    int maxConnections = 10;

    when(mockConnectionHelper.isConnectionPoolInitialized()).thenReturn(true);

    connector.initConnectionHelper(shards, maxConnections);

    verify(mockConnectionHelper, never()).init(any());
  }

  @Test
  public void testClassifyException_Permanent() {
    com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException mockException =
        org.mockito.Mockito.mock(
            com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException.class);
    assertEquals(
        com.google.cloud.teleport.v2.templates.constants.Constants.PERMANENT_ERROR_TAG,
        connector.classifyException(mockException));
  }

  @Test
  public void testClassifyException_Fallback() {
    Throwable genericEx = new RuntimeException("generic error");
    org.junit.Assert.assertNull(connector.classifyException(genericEx));
  }

  @Test
  public void testGetInformationSchema() throws Exception {
    com.datastax.oss.driver.api.core.CqlSession mockSession =
        mock(com.datastax.oss.driver.api.core.CqlSession.class);
    when(mockCassandraShard.getKeySpaceName()).thenReturn("keyspace");

    CassandraSpToSrcSourceConnector spyConnector = spy(connector);
    doReturn(mockSession).when(spyConnector).createCqlSession(any());

    com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema dummySchema =
        com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema.builder(
                com.google.cloud.teleport.v2.spanner.sourceddl.SourceDatabaseType.CASSANDRA)
            .databaseName("keyspace")
            .tables(com.google.common.collect.ImmutableMap.of())
            .build();

    try (org.mockito.MockedConstruction<
            com.google.cloud.teleport.v2.spanner.sourceddl.CassandraInformationSchemaScanner>
        mocked =
            org.mockito.Mockito.mockConstruction(
                com.google.cloud.teleport.v2.spanner.sourceddl.CassandraInformationSchemaScanner
                    .class,
                (mock, context) -> {
                  when(mock.scan()).thenReturn(dummySchema);
                })) {

      com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema result =
          spyConnector.getInformationSchema(List.of(mockCassandraShard));
      assertEquals(dummySchema, result);
    }
  }

  @Test
  public void testParseShardList_validConf() throws Exception {
    try (org.mockito.MockedStatic<
            com.google.cloud.teleport.v2.spanner.migrations.utils.JarFileReader>
        mockFileReader =
            org.mockito.Mockito.mockStatic(
                com.google.cloud.teleport.v2.spanner.migrations.utils.JarFileReader.class)) {
      String testGcsPath = "gs://smt-test-bucket/cassandraConfig.conf";
      java.net.URL testUrl =
          com.google.common.io.Resources.getResource("test-cassandra-config.conf");
      mockFileReader
          .when(
              () ->
                  com.google.cloud.teleport.v2.spanner.migrations.utils.JarFileReader
                      .saveFilesLocally(testGcsPath))
          .thenReturn(new java.net.URL[] {testUrl});

      List<Shard> shards = connector.parseShardConfig(testGcsPath);
      assertNotNull(shards);
      assertEquals(1, shards.size());
      Shard shard = shards.get(0);
      assertEquals("127.0.0.1", shard.getHost());
      assertEquals("9042", shard.getPort());

      CassandraShard cassandraShard = (CassandraShard) shard;
      assertEquals("cassandra", cassandraShard.getUsername());
      assertEquals("my_keyspace", cassandraShard.getKeySpaceName());
      assertEquals(
          "cassandra",
          cassandraShard
              .getOptionsMap()
              .get(
                  com.datastax.oss.driver.api.core.config.TypedDriverOption
                      .AUTH_PROVIDER_PASSWORD));
    }
  }

  @Test
  public void testSupportsSharding() {
    assertFalse(connector.supportsSharding());
  }

  @Test
  public void testShouldUpdateReadValuesToSpannerRecord() {
    assertFalse(connector.shouldUpdateReadValuesToSpannerRecord());
  }

  @Test
  public void testValidate_Success() throws Exception {
    connector.validate(List.of(mockCassandraShard), null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidate_MultipleShards_Failure() throws Exception {
    connector.validate(List.of(mockCassandraShard, mockCassandraShard), null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidate_InvalidShardType_Failure() throws Exception {
    connector.validate(List.of(mock(Shard.class)), null);
  }
}
