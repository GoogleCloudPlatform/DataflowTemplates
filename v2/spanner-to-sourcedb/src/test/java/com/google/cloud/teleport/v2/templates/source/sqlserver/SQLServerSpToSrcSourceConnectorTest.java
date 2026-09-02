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
package com.google.cloud.teleport.v2.templates.source.sqlserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.spanner.migrations.connection.ConnectionHelperRequest;
import com.google.cloud.teleport.v2.spanner.migrations.connection.IConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.IDao;
import com.google.cloud.teleport.v2.templates.dbutils.dml.IDMLGenerator;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SQLServerSpToSrcSourceConnectorTest {

  @Mock private IConnectionHelper mockConnectionHelper;
  @Mock private Shard mockShard;

  private SQLServerSpToSrcSourceConnector connector;

  @Before
  public void setUp() {
    connector = new SQLServerSpToSrcSourceConnector(mockConnectionHelper);
  }

  @Test
  public void testGetDmlGenerator() {
    IDMLGenerator dmlGenerator = connector.getDmlGenerator();
    assertNotNull(dmlGenerator);
    assertTrue(dmlGenerator instanceof SQLServerDMLGenerator);
  }

  @Test
  public void testGetConnectionHelper() {
    assertEquals(mockConnectionHelper, connector.getConnectionHelper());
  }

  @Test
  public void testGetConnectionUrl() {
    when(mockShard.getHost()).thenReturn("localhost");
    when(mockShard.getPort()).thenReturn("1433");
    when(mockShard.getDbName()).thenReturn("testdb");

    String url = connector.getConnectionUrl(mockShard);
    assertEquals(
        "jdbc:sqlserver://localhost:1433;databaseName=testdb;trustServerCertificate=true;encrypt=false",
        url);
  }

  @Test
  public void testGetDao() {
    when(mockShard.getHost()).thenReturn("localhost");
    when(mockShard.getPort()).thenReturn("1433");
    when(mockShard.getDbName()).thenReturn("testdb");
    when(mockShard.getUserName()).thenReturn("user");

    IDao dao = connector.getDao(mockShard);
    assertNotNull(dao);
  }

  @Test
  public void testInitConnectionHelper() {
    when(mockConnectionHelper.isConnectionPoolInitialized()).thenReturn(false);
    doNothing().when(mockConnectionHelper).init(any(ConnectionHelperRequest.class));

    connector.initConnectionHelper(List.of(mockShard), 10);

    verify(mockConnectionHelper).init(any(ConnectionHelperRequest.class));
  }
}
