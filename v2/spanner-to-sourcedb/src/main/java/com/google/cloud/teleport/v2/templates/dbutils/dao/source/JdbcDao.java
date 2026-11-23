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
import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
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
    try (Connection connObj =
        (Connection) connectionHelper.getConnection(this.sqlUrl + "/" + this.sqlUser)) {
      if (connObj == null) {
        throw new ConnectionException("Connection is null");
      }
      connObj.setAutoCommit(false);
      try {
        executeUpdate(connObj, dmlGeneratorResponse);
        if (transactionalCheck != null) {
          transactionalCheck.check();
        }
        connObj.commit();
      } catch (Throwable t) {
        try {
          connObj.rollback();
        } catch (SQLException rollbackException) {
          t.addSuppressed(rollbackException);
        }
        throw t;
      }
    }
  }

  private void executeUpdate(Connection connObj, DMLGeneratorResponse dmlGeneratorResponse)
      throws SQLException {
    String dmlStatement = dmlGeneratorResponse.getDmlStatement();
    List<Object> params = dmlGeneratorResponse.getPreparedStatementParameters();
    if (params == null || params.isEmpty()) {
      try (Statement statement = connObj.createStatement()) {
        statement.executeUpdate(dmlStatement);
      }
      return;
    }
    try (PreparedStatement pstmt = connObj.prepareStatement(dmlStatement)) {
      int paramIdx = 1;
      for (Object param : params) {
        bindParameter(pstmt, paramIdx++, param);
      }
      pstmt.executeUpdate();
    }
  }

  /** Binds a prepared statement parameter. Sources can override this for type specific binding. */
  protected void bindParameter(PreparedStatement pstmt, int paramIdx, Object param)
      throws SQLException {
    if (param instanceof byte[]) {
      byte[] bytes = (byte[]) param;
      pstmt.setBlob(paramIdx, new ByteArrayInputStream(bytes), bytes.length);
    } else {
      pstmt.setObject(paramIdx, param);
    }
  }
}
