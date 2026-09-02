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
package com.google.cloud.teleport.v2.templates.source.spanner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.teleport.v2.spanner.migrations.connection.ConnectionHelperRequest;
import com.google.cloud.teleport.v2.spanner.migrations.connection.IConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.shard.SpannerShard;
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
public class SpannerSpToSrcSourceConnectorTest {

  @Mock private IConnectionHelper<DatabaseClient> mockConnectionHelper;
  @Mock private SpannerShard mockSpannerShard;
  @Mock private Shard mockGenericShard;

  private SpannerSpToSrcSourceConnector connector;

  @Before
  public void setUp() {
    connector = new SpannerSpToSrcSourceConnector(mockConnectionHelper);
  }

  @Test
  public void testGetDmlGenerator() {
    IDMLGenerator dmlGenerator = connector.getDmlGenerator();
    assertNotNull(dmlGenerator);
    assertTrue(dmlGenerator instanceof SpannerDMLGenerator);
  }

  @Test
  public void testGetConnectionHelper() {
    assertEquals(mockConnectionHelper, connector.getConnectionHelper());
  }

  @Test
  public void testGetDao() {
    when(mockSpannerShard.getProjectId()).thenReturn("my-project");
    when(mockSpannerShard.getInstanceId()).thenReturn("my-instance");
    when(mockSpannerShard.getDatabaseId()).thenReturn("my-database");

    com.google.cloud.teleport.v2.spanner.ddl.Ddl ddl =
        com.google.cloud.teleport.v2.spanner.ddl.Ddl.builder().build();
    connector.setTargetDdl(ddl);

    assertEquals(ddl, connector.getTargetDdl());

    IDao dao = connector.getDao(mockSpannerShard);
    assertNotNull(dao);
    assertTrue(dao instanceof SpannerTargetDao);
  }

  @Test
  public void testInitConnectionHelper() {
    List<Shard> shards = Collections.singletonList(mockSpannerShard);
    int maxConnections = 10;

    when(mockConnectionHelper.isConnectionPoolInitialized()).thenReturn(false);

    connector.initConnectionHelper(shards, maxConnections);

    ArgumentCaptor<ConnectionHelperRequest> requestCaptor =
        ArgumentCaptor.forClass(ConnectionHelperRequest.class);
    verify(mockConnectionHelper).init(requestCaptor.capture());

    ConnectionHelperRequest request = requestCaptor.getValue();
    assertEquals(shards, request.getShards());
    assertEquals(maxConnections, request.getMaxConnections());
    assertEquals(null, request.getDriver());
    assertEquals(null, request.getConnectionInitQuery());
    assertEquals(null, request.getJdbcUrlPrefix());
  }

  @Test
  public void testInitConnectionHelper_alreadyInitialized() {
    List<Shard> shards = Collections.singletonList(mockSpannerShard);
    int maxConnections = 10;

    when(mockConnectionHelper.isConnectionPoolInitialized()).thenReturn(true);

    connector.initConnectionHelper(shards, maxConnections);

    verify(mockConnectionHelper, never()).init(any());
  }

  @Test
  public void testValidate_Success() throws Exception {
    connector.validate(List.of(mockSpannerShard), null);
  }

  @Test
  public void testSupportsSharding() {
    assertFalse(connector.supportsSharding());
  }

  @Test
  public void testShouldUpdateReadValuesToSpannerRecord() {
    assertTrue(connector.shouldUpdateReadValuesToSpannerRecord());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidate_MultipleShards_Failure() throws Exception {
    connector.validate(List.of(mockSpannerShard, mockSpannerShard), null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidate_InvalidShardType_Failure() throws Exception {
    connector.validate(List.of(mockGenericShard), null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidate_EmptyShards_Failure() throws Exception {
    connector.validate(List.of(), null);
  }

  @Test
  public void testDefaultConstructor() {
    SpannerSpToSrcSourceConnector defaultConnector = new SpannerSpToSrcSourceConnector();
    assertNotNull(defaultConnector.getConnectionHelper());
    assertNotNull(defaultConnector.getDmlGenerator());
  }

  @Test
  public void testGetConnectionUrl_Success() {
    when(mockSpannerShard.getProjectId()).thenReturn("p");
    when(mockSpannerShard.getInstanceId()).thenReturn("i");
    when(mockSpannerShard.getDatabaseId()).thenReturn("d");

    assertEquals("p/i/d", connector.getConnectionUrl(mockSpannerShard));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetConnectionUrl_InvalidShardTypeThrows() {
    connector.getConnectionUrl(mockGenericShard);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetConnectionUrl_NullShardThrows() {
    connector.getConnectionUrl(null);
  }

  @Test
  public void testClassifyExceptionReturnsNull() {
    org.junit.Assert.assertNull(connector.classifyException(new RuntimeException("test")));
  }
}
