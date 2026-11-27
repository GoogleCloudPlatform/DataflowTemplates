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
package com.google.cloud.teleport.v2.templates.dbutils.dao.source;

import com.google.cloud.teleport.v2.spanner.migrations.connection.IConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ConnectionException;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorResponse;
import com.google.cloud.teleport.v2.templates.models.PreparedStatementValue;
import com.google.common.annotations.VisibleForTesting;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcDao implements IDao {
  private String sqlUrl;
  private String sqlUser;

  private final IConnectionHelper connectionHelper;

  private static final Logger LOG = LoggerFactory.getLogger(JdbcDao.class);

  public JdbcDao(String sqlUrl, String sqlUser, IConnectionHelper connectionHelper) {
    this.sqlUrl = sqlUrl;
    this.sqlUser = sqlUser;
    this.connectionHelper = connectionHelper;
  }

  @Override
  public void write(
      DMLGeneratorResponse dmlGeneratorResponse, TransactionalCheck transactionalCheck)
      throws Exception {
    Connection connObj = null;
    Statement statement = null;

    String dmlStatement = dmlGeneratorResponse.getDmlStatement();

    try {
      connObj = (Connection) connectionHelper.getConnection(this.sqlUrl + "/" + this.sqlUser);
      if (connObj == null) {
        throw new ConnectionException("Connection is null");
      }
      connObj.setAutoCommit(false);
      java.util.List<Object> params = dmlGeneratorResponse.getPreparedStatementParameters();
      if (params != null && !params.isEmpty()) {
        PreparedStatement pstmt = connObj.prepareStatement(dmlStatement);
        statement = pstmt;
        int paramIdx = 1;
        for (Object param : params) {
          if (param instanceof PreparedStatementValue) {
            bindValue(pstmt, paramIdx++, (PreparedStatementValue) param);
          } else if (param instanceof byte[]) {
            byte[] bytes = (byte[]) param;
            pstmt.setBlob(paramIdx++, new java.io.ByteArrayInputStream(bytes), bytes.length);
          } else {
            pstmt.setObject(paramIdx++, param);
          }
        }
        pstmt.executeUpdate();
      } else {
        statement = connObj.createStatement();
        statement.executeUpdate(dmlStatement);
      }

      if (transactionalCheck != null) {
        transactionalCheck.check();
      }
      connObj.commit();

    } finally {

      if (statement != null) {
        statement.close();
      }
      if (connObj != null) {
        connObj.close();
      }
    }
  }

  /** Binds a typed {@link PreparedStatementValue} to the given placeholder index. */
  @VisibleForTesting
  static void bindValue(PreparedStatement pstmt, int index, PreparedStatementValue param)
      throws SQLException {
    Object value = param.getValue();
    if (value == null) {
      pstmt.setNull(index, param.getSqlType());
    } else if (value instanceof byte[]) {
      pstmt.setBytes(index, (byte[]) value);
    } else if (value instanceof String) {
      pstmt.setString(index, (String) value);
    } else if (value instanceof Boolean) {
      pstmt.setBoolean(index, (Boolean) value);
    } else if (value instanceof BigDecimal) {
      pstmt.setBigDecimal(index, (BigDecimal) value);
    } else if (value instanceof Long) {
      pstmt.setLong(index, (Long) value);
    } else if (value instanceof Double) {
      pstmt.setDouble(index, (Double) value);
    } else {
      pstmt.setObject(index, value, param.getSqlType());
    }
  }
}
