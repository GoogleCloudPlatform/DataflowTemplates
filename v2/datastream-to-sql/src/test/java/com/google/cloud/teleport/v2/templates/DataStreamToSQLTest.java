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
package com.google.cloud.teleport.v2.templates;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.datastream.io.CdcJdbcIO;
import com.google.cloud.teleport.v2.datastream.values.DmlInfo;
import com.google.cloud.teleport.v2.templates.DataStreamToSQL.ExecuteDmlFn;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.values.KV;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test cases for {@link ExecuteDmlFn} in {@link DataStreamToSQL}. */
@RunWith(JUnit4.class)
public class DataStreamToSQLTest {

  private DataSource mockDataSource;
  private Connection mockConnection;
  private Statement mockStatement;
  private ProcessContext mockContext;
  private DmlInfo mockDmlInfo;
  private KV<String, DmlInfo> element;
  private ExecuteDmlFn executeDmlFn;

  @Before
  public void setUp() throws SQLException {
    mockDataSource = mock(DataSource.class);
    mockConnection = mock(Connection.class);
    mockStatement = mock(Statement.class);
    mockContext = mock(ProcessContext.class);
    mockDmlInfo = mock(DmlInfo.class);

    when(mockDmlInfo.getDmlSql()).thenReturn("INSERT INTO test_table VALUES (1);");
    element = KV.of("test_table", mockDmlInfo);
    when(mockContext.element()).thenReturn(element);

    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockConnection.isClosed()).thenReturn(false);

    executeDmlFn = new ExecuteDmlFn(null, new CdcJdbcIO.DefaultRetryStrategy());
    executeDmlFn.setDataSource(mockDataSource);
    executeDmlFn.setConnection(mockConnection);
    executeDmlFn.setSleeper(millis -> {});
  }

  @Test
  public void testExecuteDmlFn_successfulExecution() throws SQLException {
    when(mockStatement.execute(mockDmlInfo.getDmlSql())).thenReturn(true);

    executeDmlFn.processElement(mockContext);

    verify(mockStatement, times(1)).execute(mockDmlInfo.getDmlSql());
    verify(mockContext, times(1)).output(eq(ExecuteDmlFn.SUCCESS_TAG), eq(element));
    verify(mockContext, never()).output(eq(ExecuteDmlFn.FAILURE_TAG), any(FailsafeElement.class));
  }

  @Test
  public void testExecuteDmlFn_nonRetryableError_routesImmediatelyToFailureTag()
      throws SQLException {
    SQLException syntaxError = new SQLException("Syntax error", "42601");
    when(mockStatement.execute(mockDmlInfo.getDmlSql())).thenThrow(syntaxError);

    executeDmlFn.processElement(mockContext);

    verify(mockStatement, times(1)).execute(mockDmlInfo.getDmlSql());
    verify(mockContext, never()).output(eq(ExecuteDmlFn.SUCCESS_TAG), any());
    verify(mockContext, times(1)).output(eq(ExecuteDmlFn.FAILURE_TAG), any(FailsafeElement.class));
  }

  @Test
  public void testExecuteDmlFn_transientDeadlock_retriesAndSucceeds() throws SQLException {
    SQLException deadlock = new SQLException("Deadlock found", "40001");
    when(mockStatement.execute(mockDmlInfo.getDmlSql()))
        .thenThrow(deadlock)
        .thenReturn(true);

    executeDmlFn.processElement(mockContext);

    verify(mockStatement, times(2)).execute(mockDmlInfo.getDmlSql());
    verify(mockContext, times(1)).output(eq(ExecuteDmlFn.SUCCESS_TAG), eq(element));
    verify(mockContext, never()).output(eq(ExecuteDmlFn.FAILURE_TAG), any(FailsafeElement.class));
  }

  @Test
  public void testExecuteDmlFn_retryableError_exhaustsRetries_routesToFailureTag()
      throws SQLException {
    SQLException deadlock = new SQLException("Deadlock found", "40001");
    when(mockStatement.execute(mockDmlInfo.getDmlSql())).thenThrow(deadlock);

    executeDmlFn.processElement(mockContext);

    // Initial attempt + 5 retries = 6 attempts
    verify(mockStatement, times(6)).execute(mockDmlInfo.getDmlSql());
    verify(mockContext, never()).output(eq(ExecuteDmlFn.SUCCESS_TAG), any());
    verify(mockContext, times(1)).output(eq(ExecuteDmlFn.FAILURE_TAG), any(FailsafeElement.class));
  }

  @Test
  public void testExecuteDmlFn_connectionException_reconnectsAndRetries() throws SQLException {
    SQLException connException = new SQLException("Connection closed", "08006");
    Statement mockStatement2 = mock(Statement.class);
    Connection mockConnection2 = mock(Connection.class);

    when(mockStatement.execute(mockDmlInfo.getDmlSql())).thenThrow(connException);
    when(mockConnection2.createStatement()).thenReturn(mockStatement2);
    when(mockConnection2.isClosed()).thenReturn(false);
    when(mockDataSource.getConnection()).thenReturn(mockConnection2);
    when(mockStatement2.execute(mockDmlInfo.getDmlSql())).thenReturn(true);

    executeDmlFn.processElement(mockContext);

    verify(mockStatement, times(1)).execute(mockDmlInfo.getDmlSql());
    verify(mockStatement2, times(1)).execute(mockDmlInfo.getDmlSql());
    verify(mockContext, times(1)).output(eq(ExecuteDmlFn.SUCCESS_TAG), eq(element));
    verify(mockContext, never()).output(eq(ExecuteDmlFn.FAILURE_TAG), any(FailsafeElement.class));
  }
}
