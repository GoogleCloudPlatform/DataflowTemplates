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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.google.cloud.teleport.v2.spanner.migrations.shard.CassandraShard;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.utils.CassandraDriverConfigLoader;
import com.google.cloud.teleport.v2.spanner.sourceddl.CassandraInformationSchemaScanner;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceDatabaseType;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.templates.dbutils.connection.CassandraConnectionHelper;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.CassandraDao;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** Unit tests for {@link CassandraSourceConnector}. */
public class CassandraSourceConnectorTest {

  @Test
  public void testValidateNotReadOnly_NoOp() throws Exception {
    CassandraSourceConnector source = new CassandraSourceConnector();
    source.validateNotReadOnly(List.of(new Shard()));
  }

  @Test
  public void testGetSourceSchema_Success() throws Exception {
    CassandraShard mockShard = mock(CassandraShard.class);
    when(mockShard.getKeySpaceName()).thenReturn("keyspace");

    SourceSchema dummySchema =
        SourceSchema.builder(SourceDatabaseType.CASSANDRA)
            .databaseName("db")
            .tables(ImmutableMap.of())
            .build();

    try (MockedStatic<CqlSession> mockedCqlSession = Mockito.mockStatic(CqlSession.class);
        MockedStatic<CassandraDriverConfigLoader> mockedConfigLoader =
            Mockito.mockStatic(CassandraDriverConfigLoader.class);
        MockedConstruction<CassandraInformationSchemaScanner> mockScannerConstruction =
            Mockito.mockConstruction(
                CassandraInformationSchemaScanner.class,
                (mockScanner, context) -> {
                  when(mockScanner.scan()).thenReturn(dummySchema);
                })) {

      CqlSessionBuilder mockBuilder = mock(CqlSessionBuilder.class);
      mockedCqlSession.when(CqlSession::builder).thenReturn(mockBuilder);
      when(mockBuilder.withConfigLoader(any())).thenReturn(mockBuilder);
      when(mockBuilder.build()).thenReturn(mock(CqlSession.class));

      mockedConfigLoader
          .when(() -> CassandraDriverConfigLoader.fromOptionsMap(any()))
          .thenReturn(mock(DriverConfigLoader.class));

      CassandraSourceConnector source = new CassandraSourceConnector();
      SourceSchema result = source.getSourceSchema(mockShard);
      Assert.assertSame(dummySchema, result);
    }
  }

  @Test
  public void testGetDmlGenerator() {
    CassandraSourceConnector connector = new CassandraSourceConnector();
    Assert.assertTrue(connector.getDmlGenerator() instanceof CassandraDMLGenerator);
  }

  @Test
  public void testGetConnectionHelper() {
    CassandraSourceConnector connector = new CassandraSourceConnector();
    Assert.assertTrue(connector.getConnectionHelper() instanceof CassandraConnectionHelper);
  }

  @Test
  public void testIsShardingSupported() {
    CassandraSourceConnector connector = new CassandraSourceConnector();
    Assert.assertFalse(connector.isShardingSupported());
  }

  @Test
  public void testGetConnectionUrl() {
    CassandraSourceConnector connector = new CassandraSourceConnector();
    CassandraShard shard = mock(CassandraShard.class);
    when(shard.getHost()).thenReturn("localhost");
    when(shard.getPort()).thenReturn("9042");
    when(shard.getUserName()).thenReturn("cassandra");
    when(shard.getKeySpaceName()).thenReturn("my_keyspace");
    Assert.assertEquals("localhost:9042/cassandra/my_keyspace", connector.getConnectionUrl(shard));
  }

  @Test
  public void testGetDao() {
    CassandraSourceConnector connector = new CassandraSourceConnector();
    CassandraShard shard = mock(CassandraShard.class);
    when(shard.getLogicalShardId()).thenReturn("shard1");
    when(shard.getHost()).thenReturn("localhost");
    when(shard.getPort()).thenReturn("9042");
    when(shard.getUserName()).thenReturn("cassandra");
    when(shard.getKeySpaceName()).thenReturn("my_keyspace");
    Assert.assertTrue(connector.getDao(shard) instanceof CassandraDao);
  }

  @Test
  public void testInitConnectionHelper() throws Exception {
    CassandraShard shard = mock(CassandraShard.class);
    when(shard.getLogicalShardId()).thenReturn("shard1");
    when(shard.getHost()).thenReturn("localhost");
    when(shard.getPort()).thenReturn("9042");
    when(shard.getUserName()).thenReturn("cassandra");
    when(shard.getKeySpaceName()).thenReturn("my_keyspace");

    try (MockedStatic<CqlSession> mockedCqlSession = Mockito.mockStatic(CqlSession.class);
         MockedStatic<CassandraDriverConfigLoader> mockedConfigLoader = Mockito.mockStatic(CassandraDriverConfigLoader.class)) {

      CqlSessionBuilder mockBuilder = mock(CqlSessionBuilder.class);
      mockedCqlSession.when(CqlSession::builder).thenReturn(mockBuilder);
      when(mockBuilder.withConfigLoader(any())).thenReturn(mockBuilder);
      when(mockBuilder.build()).thenReturn(mock(CqlSession.class));

      mockedConfigLoader
          .when(() -> CassandraDriverConfigLoader.fromOptionsMap(any()))
          .thenReturn(mock(DriverConfigLoader.class));

      CassandraSourceConnector connector = new CassandraSourceConnector();
      connector.initConnectionHelper(List.of(shard), 10);
      Assert.assertTrue(connector.getConnectionHelper().isConnectionPoolInitialized());
    }
  }
}
