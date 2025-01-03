/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.templates.utils;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.google.cloud.teleport.v2.spanner.migrations.shard.CassandraShard;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class CassandraSourceSchemaReaderTest {

  private CassandraSourceSchemaReader schemaReader;
  private CqlSession mockSession;
  private CassandraShard mockCassandraShard;
  private PreparedStatement mockPreparedStatement;
  private BoundStatement mockBoundStatement;
  private ResultSet mockResultSet;
  private CqlSessionBuilder mockSessionBuilder;
  private DriverConfigLoader mockConfigLoader;
  private OptionsMap optionsMap;

  @Before
  public void setUp() {
    mockSession = mock(CqlSession.class);
    mockCassandraShard = mock(CassandraShard.class);
    mockPreparedStatement = mock(PreparedStatement.class);
    mockBoundStatement = mock(BoundStatement.class);
    mockResultSet = mock(ResultSet.class);
    mockSessionBuilder = mock(CqlSessionBuilder.class);
    mockConfigLoader = mock(DriverConfigLoader.class);
    optionsMap = mock(OptionsMap.class);
  }

  @Test
  public void testGetInformationSchemaAsResultSet_Success() throws Exception {
    String keyspace = "test_keyspace";
    String query =
        "SELECT table_name, column_name, type, kind FROM system_schema.columns WHERE keyspace_name = ?";
    when(mockCassandraShard.getKeySpaceName()).thenReturn(keyspace);
    when(mockCassandraShard.getOptionsMap()).thenReturn(optionsMap);
    when(mockSession.prepare(query)).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.bind(keyspace)).thenReturn(mockBoundStatement);
    when(mockSession.execute(mockBoundStatement)).thenReturn(mockResultSet);

    try (MockedStatic<CqlSession> mockedSession = Mockito.mockStatic(CqlSession.class)) {
      mockedSession.when(CqlSession::builder).thenReturn(mockSessionBuilder);
      when(mockSessionBuilder.withConfigLoader(any(DriverConfigLoader.class)))
          .thenReturn(mockSessionBuilder);
      when(mockSessionBuilder.build()).thenReturn(mockSession);

      ResultSet resultSet =
          CassandraSourceSchemaReader.getInformationSchemaAsResultSet(mockCassandraShard);

      assertNotNull("Result set should not be null", resultSet);
      verify(mockPreparedStatement, times(1)).bind(keyspace);
      verify(mockSession, times(1)).execute(mockBoundStatement);
    }
  }
}
