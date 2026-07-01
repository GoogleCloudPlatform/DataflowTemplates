/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.templates.dbutils.processor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;

import com.google.cloud.teleport.v2.spanner.migrations.connection.JdbcConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.shard.CassandraShard;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.templates.constants.Constants;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.JdbcDao;
import com.google.cloud.teleport.v2.templates.exceptions.UnsupportedSourceException;
import com.google.cloud.teleport.v2.templates.source.cassandra.CassandraConnectionHelper;
import com.google.cloud.teleport.v2.templates.source.cassandra.CassandraDMLGenerator;
import com.google.cloud.teleport.v2.templates.source.cassandra.CassandraDao;
import com.google.cloud.teleport.v2.templates.source.cassandra.CassandraSpToSrcSourceConnector;
import com.google.cloud.teleport.v2.templates.source.mysql.MySQLDMLGenerator;
import com.google.cloud.teleport.v2.templates.source.mysql.MySQLSpToSrcSourceConnector;
import com.google.cloud.teleport.v2.templates.source.postgres.PostgreSQLSpToSrcSourceConnector;
import com.google.cloud.teleport.v2.templates.source.spanner.SpannerSpToSrcSourceConnector;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

@RunWith(JUnit4.class)
public class SourceProcessorFactoryTest {

  private static Map<String, ISpToSrcSourceConnector> originalSourceMap;

  @BeforeClass
  public static void setUpBeforeClass() {
    originalSourceMap = SourceProcessorFactory.getSourceMap();
  }

  @After
  public void tearDown() {
    SourceProcessorFactory.setSourceMap(originalSourceMap);
  }

  @Test
  public void testCreateSourceProcessor_validSource() throws Exception {
    List<Shard> shards =
        Arrays.asList(
            new Shard(
                "shard1",
                "localhost",
                "3306",
                "myuser",
                "mypassword",
                "mydatabase",
                "mynamespace",
                "projects/myproject/secrets/mysecret/versions/latest",
                ""));
    int maxConnections = 10;

    ISpToSrcSourceConnector mockSource = Mockito.mock(ISpToSrcSourceConnector.class);
    JdbcConnectionHelper mockConnectionHelper = Mockito.mock(JdbcConnectionHelper.class);
    doNothing().when(mockConnectionHelper).init(any());

    Mockito.when(mockSource.getDmlGenerator()).thenReturn(new MySQLDMLGenerator());
    Mockito.when(mockSource.getConnectionHelper()).thenReturn(mockConnectionHelper);
    Mockito.when(mockSource.getDao(any())).thenReturn(Mockito.mock(JdbcDao.class));

    SourceProcessorFactory.registerSource(Constants.SOURCE_MYSQL, mockSource);

    SourceProcessor processor =
        SourceProcessorFactory.createSourceProcessor(
            Constants.SOURCE_MYSQL, shards, maxConnections);

    Assert.assertNotNull(processor);
    Assert.assertTrue(processor.getDmlGenerator() instanceof MySQLDMLGenerator);
    Assert.assertEquals(1, processor.getSourceDaoMap().size());
    Assert.assertTrue(processor.getSourceDaoMap().get("shard1") instanceof JdbcDao);
  }

  @Test(expected = UnsupportedSourceException.class)
  public void testCreateSourceProcessor_invalidSource() throws Exception {
    List<Shard> shards =
        Arrays.asList(
            new Shard(
                "shard1",
                "localhost",
                "3306",
                "myuser",
                "mypassword",
                "mydatabase",
                "mynamespace",
                "projects/myproject/secrets/mysecret/versions/latest",
                ""));
    int maxConnections = 10;

    SourceProcessorFactory.createSourceProcessor("invalid_source", shards, maxConnections);
  }

  @Test
  public void testCreateSourceProcessor_cassandra_validSource() throws Exception {
    CassandraShard mockCassandraShard = Mockito.mock(CassandraShard.class);
    Mockito.when(mockCassandraShard.getContactPoints()).thenReturn(List.of("localhost:9042"));
    Mockito.when(mockCassandraShard.getKeySpaceName()).thenReturn("mydatabase");
    Mockito.when(mockCassandraShard.getLogicalShardId()).thenReturn("shard1");

    List<Shard> shards = List.of(mockCassandraShard);
    int maxConnections = 10;

    ISpToSrcSourceConnector mockSource = Mockito.mock(ISpToSrcSourceConnector.class);
    CassandraConnectionHelper mockConnectionHelper = Mockito.mock(CassandraConnectionHelper.class);
    doNothing().when(mockConnectionHelper).init(any());

    Mockito.when(mockSource.getDmlGenerator()).thenReturn(new CassandraDMLGenerator());
    Mockito.when(mockSource.getConnectionHelper()).thenReturn(mockConnectionHelper);
    Mockito.when(mockSource.getDao(any())).thenReturn(Mockito.mock(CassandraDao.class));

    SourceProcessorFactory.registerSource(Constants.SOURCE_CASSANDRA, mockSource);

    SourceProcessor processor =
        SourceProcessorFactory.createSourceProcessor(
            Constants.SOURCE_CASSANDRA, shards, maxConnections);

    Assert.assertNotNull(processor);
    Assert.assertTrue(processor.getDmlGenerator() instanceof CassandraDMLGenerator);
    Assert.assertEquals(1, processor.getSourceDaoMap().size());
    Assert.assertTrue(processor.getSourceDaoMap().get("shard1") instanceof CassandraDao);
  }

  @Test
  public void testGetSource_Success() throws Exception {
    ISpToSrcSourceConnector mysqlSource = SourceProcessorFactory.getSource("mysql");
    Assert.assertTrue(mysqlSource instanceof MySQLSpToSrcSourceConnector);

    ISpToSrcSourceConnector postgresSource = SourceProcessorFactory.getSource("postgresql");
    Assert.assertTrue(postgresSource instanceof PostgreSQLSpToSrcSourceConnector);

    ISpToSrcSourceConnector cassandraSource = SourceProcessorFactory.getSource("cassandra");
    Assert.assertTrue(cassandraSource instanceof CassandraSpToSrcSourceConnector);

    ISpToSrcSourceConnector spannerSource = SourceProcessorFactory.getSource("spanner");
    Assert.assertTrue(spannerSource instanceof SpannerSpToSrcSourceConnector);
  }

  @Test(expected = UnsupportedSourceException.class)
  public void testGetSource_UnsupportedSourceException() throws Exception {
    SourceProcessorFactory.getSource("unsupported_db");
  }
}
