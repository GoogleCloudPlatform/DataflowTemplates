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

import com.google.cloud.teleport.v2.spanner.migrations.connection.IConnectionHelper;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.JdbcDao;
import java.io.StringReader;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import oracle.jdbc.OraclePreparedStatement;

/**
 * Oracle-specific {@link JdbcDao} subclass that overrides {@link #bindParameter} to support typed
 * {@code NULL}s, streamed {@code CLOB}/{@code NCLOB} payloads, national character ({@code
 * NVARCHAR2}/{@code NCHAR}) bindings, fixed-width {@code CHAR} bindings, and {@code RAW} byte-array
 * bindings.
 */
public class OracleJdbcDao extends JdbcDao {

  /**
   * Typed JDBC bind parameter produced by {@link OracleDMLGenerator} so {@link OracleJdbcDao} can
   * bind {@code NULL}s with their target {@link Types} code (preventing {@code ORA-00932} on LOB
   * columns) and stream large {@code CLOB}/{@code NCLOB} payloads without hitting Oracle's
   * 4,000-byte {@code VARCHAR2} bind limit ({@code ORA-01461}).
   */
  record OracleBindParameter(int sqlType, Object value) {}

  public OracleJdbcDao(String sqlUrl, String sqlUser, IConnectionHelper connectionHelper) {
    super(sqlUrl, sqlUser, connectionHelper);
  }

  @Override
  protected void bindParameter(PreparedStatement pstmt, int paramIdx, Object param)
      throws SQLException {
    if (!(param instanceof OracleBindParameter bindParam)) {
      if (param == null) {
        pstmt.setNull(paramIdx, Types.VARCHAR);
      } else {
        super.bindParameter(pstmt, paramIdx, param);
      }
      return;
    }

    int sqlType = bindParam.sqlType();
    Object value = bindParam.value();
    if (value == null) {
      pstmt.setNull(paramIdx, sqlType);
      return;
    }
    switch (sqlType) {
      case Types.CLOB -> {
        String text = requireType(value, String.class, sqlType);
        pstmt.setClob(paramIdx, new StringReader(text), text.length());
      }
      case Types.NCLOB -> {
        String text = requireType(value, String.class, sqlType);
        pstmt.setNClob(paramIdx, new StringReader(text), text.length());
      }
      case Types.NVARCHAR -> pstmt.setNString(paramIdx, requireType(value, String.class, sqlType));
      case Types.NCHAR -> setFixedChar(
          pstmt, paramIdx, requireType(value, String.class, sqlType), true);
      case Types.CHAR -> setFixedChar(
          pstmt, paramIdx, requireType(value, String.class, sqlType), false);
      case Types.VARBINARY -> pstmt.setBytes(paramIdx, requireType(value, byte[].class, sqlType));
      case Types.BLOB ->
      // Streamed with setBlob by JdbcDao.
      super.bindParameter(pstmt, paramIdx, requireType(value, byte[].class, sqlType));
      default -> pstmt.setObject(paramIdx, value, sqlType);
    }
  }

  /**
   * Binds a fixed width ({@code CHAR} / {@code NCHAR}) value with {@code setFixedCHAR}, so that
   * Oracle uses blank-padded comparison semantics (e.g. for primary keys in {@code MERGE ON} and
   * {@code DELETE WHERE}).
   */
  private static void setFixedChar(
      PreparedStatement pstmt, int paramIdx, String value, boolean national) throws SQLException {
    if (!pstmt.isWrapperFor(OraclePreparedStatement.class)) {
      pstmt.setObject(paramIdx, value, national ? Types.NCHAR : Types.CHAR);
      return;
    }
    OraclePreparedStatement oraclePstmt = pstmt.unwrap(OraclePreparedStatement.class);
    if (national) {
      oraclePstmt.setFormOfUse(paramIdx, OraclePreparedStatement.FORM_NCHAR);
    }
    oraclePstmt.setFixedCHAR(paramIdx, value);
  }

  private static <T> T requireType(Object value, Class<T> type, int sqlType) {
    if (!type.isInstance(value)) {
      throw new IllegalArgumentException(
          String.format(
              "Expected %s for JDBC type %d but got %s",
              type.getSimpleName(), sqlType, value.getClass().getName()));
    }
    return type.cast(value);
  }
}
