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
package com.google.cloud.teleport.v2.templates.source.oracle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.spanner.migrations.connection.IConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ConnectionException;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.TransactionalCheck;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.TransactionalCheckException;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorResponse;
import com.google.cloud.teleport.v2.templates.source.oracle.OracleJdbcDao.OracleBindParameter;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import oracle.jdbc.OraclePreparedStatement;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class OracleJdbcDaoTest {

  @Mock private IConnectionHelper<Connection> mockConnectionHelper;
  @Mock private Connection mockConnection;
  @Mock private PreparedStatement mockPreparedStatement;
  @Mock private Statement mockStatement;

  private OracleJdbcDao dao;

  @Before
  public void setUp() throws Exception {
    dao =
        new OracleJdbcDao("jdbc:oracle:thin:@//localhost:1521/xepdb1", "hr", mockConnectionHelper);
    when(mockConnectionHelper.getConnection("jdbc:oracle:thin:@//localhost:1521/xepdb1/hr"))
        .thenReturn(mockConnection);
    when(mockConnection.prepareStatement(any())).thenReturn(mockPreparedStatement);
    when(mockConnection.createStatement()).thenReturn(mockStatement);
  }

  @Test
  public void testNullConnectionThrowsConnectionException() throws Exception {
    when(mockConnectionHelper.getConnection(any())).thenReturn(null);
    assertThrows(
        ConnectionException.class, () -> dao.write(new DMLGeneratorResponse("SELECT 1"), null));
  }

  @Test
  public void testWriteWithPreparedStatementAllBindTypes() throws Exception {
    byte[] blobBytes = "binary-payload".getBytes(StandardCharsets.UTF_8);
    byte[] varbinaryBytes = "raw-bytes".getBytes(StandardCharsets.UTF_8);
    byte[] rawBytes = new byte[] {1, 2, 3};
    String clobText = "c".repeat(6000);
    String nclobText = "n".repeat(5000);

    DMLGeneratorResponse response = new DMLGeneratorResponse("MERGE INTO \"T\" ...");
    response.setPreparedStatementParameters(
        Arrays.asList(
            "101",
            null,
            new OracleBindParameter(Types.VARCHAR, null),
            new OracleBindParameter(Types.BLOB, null),
            new OracleBindParameter(Types.CLOB, null),
            new OracleBindParameter(Types.NCLOB, null),
            new OracleBindParameter(Types.VARBINARY, null),
            new OracleBindParameter(Types.NVARCHAR, null),
            new OracleBindParameter(Types.NCHAR, null),
            new OracleBindParameter(Types.BLOB, blobBytes),
            new OracleBindParameter(Types.CLOB, clobText),
            new OracleBindParameter(Types.NCLOB, nclobText),
            new OracleBindParameter(Types.NVARCHAR, "unicode-nvarchar"),
            new OracleBindParameter(Types.NCHAR, "fixed-nchar-fallback"),
            new OracleBindParameter(Types.CHAR, "fixed-char-fallback"),
            new OracleBindParameter(Types.VARBINARY, varbinaryBytes),
            new OracleBindParameter(Types.VARCHAR, "explicit-varchar"),
            rawBytes));

    TransactionalCheck check = mock(TransactionalCheck.class);
    dao.write(response, check);

    verify(mockConnection).setAutoCommit(false);
    verify(mockPreparedStatement).setObject(1, "101");
    verify(mockPreparedStatement).setNull(2, Types.VARCHAR);
    verify(mockPreparedStatement).setNull(3, Types.VARCHAR);
    verify(mockPreparedStatement).setNull(4, Types.BLOB);
    verify(mockPreparedStatement).setNull(5, Types.CLOB);
    verify(mockPreparedStatement).setNull(6, Types.NCLOB);
    verify(mockPreparedStatement).setNull(7, Types.VARBINARY);
    verify(mockPreparedStatement).setNull(8, Types.NVARCHAR);
    verify(mockPreparedStatement).setNull(9, Types.NCHAR);
    verify(mockPreparedStatement)
        .setBlob(eq(10), any(InputStream.class), eq((long) blobBytes.length));
    verify(mockPreparedStatement).setClob(eq(11), any(Reader.class), eq((long) clobText.length()));
    verify(mockPreparedStatement)
        .setNClob(eq(12), any(Reader.class), eq((long) nclobText.length()));
    verify(mockPreparedStatement).setNString(13, "unicode-nvarchar");
    verify(mockPreparedStatement).setObject(14, "fixed-nchar-fallback", Types.NCHAR);
    verify(mockPreparedStatement).setObject(15, "fixed-char-fallback", Types.CHAR);
    verify(mockPreparedStatement).setBytes(16, varbinaryBytes);
    verify(mockPreparedStatement).setObject(17, "explicit-varchar", Types.VARCHAR);
    verify(mockPreparedStatement)
        .setBlob(eq(18), any(InputStream.class), eq((long) rawBytes.length));
    verify(mockPreparedStatement).executeUpdate();
    verify(check).check();
    verify(mockConnection).commit();
    verify(mockPreparedStatement).close();
    verify(mockConnection).close();
  }

  @Test
  public void testFixedCharAndNcharUnwrapOraclePreparedStatement() throws Exception {
    OraclePreparedStatement mockOraclePstmt = mock(OraclePreparedStatement.class);
    when(mockPreparedStatement.isWrapperFor(OraclePreparedStatement.class)).thenReturn(true);
    when(mockPreparedStatement.unwrap(OraclePreparedStatement.class)).thenReturn(mockOraclePstmt);

    DMLGeneratorResponse response =
        new DMLGeneratorResponse("DELETE FROM \"CHAR_PK\" WHERE \"C\" = ? AND \"NC\" = ?");
    response.setPreparedStatementParameters(
        List.of(
            new OracleBindParameter(Types.CHAR, "padded-char"),
            new OracleBindParameter(Types.NCHAR, "padded-nchar")));

    dao.write(response, null);

    InOrder order = inOrder(mockOraclePstmt);
    order.verify(mockOraclePstmt).setFixedCHAR(1, "padded-char");
    order.verify(mockOraclePstmt).setFormOfUse(2, OraclePreparedStatement.FORM_NCHAR);
    order.verify(mockOraclePstmt).setFixedCHAR(2, "padded-nchar");
    verify(mockPreparedStatement).executeUpdate();
    verify(mockConnection).commit();
  }

  @Test
  public void testWriteWithoutParametersUsesStatement() throws Exception {
    DMLGeneratorResponse response = new DMLGeneratorResponse("DELETE FROM \"T\" WHERE \"ID\" = 1");
    response.setPreparedStatementParameters(List.of());

    dao.write(response, null);

    verify(mockStatement).executeUpdate("DELETE FROM \"T\" WHERE \"ID\" = 1");
    verify(mockConnection).commit();
    verify(mockStatement).close();
    verify(mockConnection).close();
  }

  @Test
  public void testTransactionalCheckFailureDoesNotCommitAndClosesResources() throws Exception {
    DMLGeneratorResponse response = new DMLGeneratorResponse("DELETE FROM \"T\" WHERE \"ID\" = ?");
    response.setPreparedStatementParameters(List.of("1"));

    TransactionalCheck failingCheck = mock(TransactionalCheck.class);
    doThrow(new TransactionalCheckException("shadow mismatch")).when(failingCheck).check();

    TransactionalCheckException thrown =
        assertThrows(TransactionalCheckException.class, () -> dao.write(response, failingCheck));
    assertEquals("shadow mismatch", thrown.getMessage());

    verify(mockConnection, never()).commit();
    verify(mockPreparedStatement).close();
    verify(mockConnection).close();
  }
}
