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
package com.google.cloud.teleport.v2.templates.dbutils.dao;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.spanner.migrations.connection.JdbcConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ConnectionException;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.JdbcDao;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorResponse;
import com.google.cloud.teleport.v2.templates.models.PreparedStatementValue;
import com.zaxxer.hikari.HikariDataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public final class JdbcDaoTest {
  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock private HikariDataSource mockHikariDataSource;
  @Mock private Connection mockConnection;
  @Mock private Statement mockStatement;
  @Mock private PreparedStatement mockPreparedStatement;

  @Before
  public void doBeforeEachTest() throws java.sql.SQLException {
    when(mockHikariDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockStatement.executeUpdate(any())).thenReturn(1);
    doNothing().when(mockStatement).close();
    doNothing().when(mockConnection).close();
  }

  @Test(expected = ConnectionException.class)
  public void testNullConnection() throws Exception {
    JdbcDao sqlDao = new JdbcDao("url", "user", new JdbcConnectionHelper());
    sqlDao.write(new DMLGeneratorResponse("sql"), null);
  }

  @Test
  public void testSuccess() throws Exception {
    Map<String, HikariDataSource> connectionPoolMap = new HashMap<>();
    connectionPoolMap.put("url/user", mockHikariDataSource);
    JdbcConnectionHelper jdbcConnectionHelper = new JdbcConnectionHelper();
    jdbcConnectionHelper.setConnectionPoolMap(connectionPoolMap);
    JdbcDao sqlDao = new JdbcDao("url", "user", jdbcConnectionHelper);
    sqlDao.write(new DMLGeneratorResponse("sql"), null);
    verify(mockStatement).executeUpdate(eq("sql"));
  }

  @Test
  public void testPreparedStatementWithTypedParameters() throws Exception {
    Map<String, HikariDataSource> connectionPoolMap = new HashMap<>();
    connectionPoolMap.put("url/user", mockHikariDataSource);
    JdbcConnectionHelper jdbcConnectionHelper = new JdbcConnectionHelper();
    jdbcConnectionHelper.setConnectionPoolMap(connectionPoolMap);
    when(mockConnection.prepareStatement(any())).thenReturn(mockPreparedStatement);
    byte[] bytes = new byte[] {1, 2, 3};
    DMLGeneratorResponse response = new DMLGeneratorResponse("sql ? ? ? ? ? ?");
    response.setPreparedStatementParameters(
        Arrays.asList(
            PreparedStatementValue.ofString("a'b"),
            PreparedStatementValue.ofBytes(bytes),
            PreparedStatementValue.ofBoolean(true),
            PreparedStatementValue.ofBigDecimal(new BigDecimal("4.2")),
            PreparedStatementValue.ofLong(7L),
            PreparedStatementValue.ofString(null)));

    new JdbcDao("url", "user", jdbcConnectionHelper).write(response, null);

    verify(mockConnection).prepareStatement(eq("sql ? ? ? ? ? ?"));
    verify(mockPreparedStatement).setString(1, "a'b");
    verify(mockPreparedStatement).setBytes(2, bytes);
    verify(mockPreparedStatement).setBoolean(3, true);
    verify(mockPreparedStatement).setBigDecimal(4, new BigDecimal("4.2"));
    verify(mockPreparedStatement).setLong(5, 7L);
    verify(mockPreparedStatement).setNull(6, Types.VARCHAR);
    verify(mockPreparedStatement).executeUpdate();
    verify(mockConnection).commit();
    verify(mockConnection, never()).createStatement();
  }
}
