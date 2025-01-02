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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.google.cloud.teleport.v2.templates.dbutils.connection.IConnectionHelper;
import com.google.cloud.teleport.v2.templates.dbutils.dml.CassandraTypeHandler;
import com.google.cloud.teleport.v2.templates.exceptions.ConnectionException;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorResponse;
import com.google.cloud.teleport.v2.templates.models.PreparedStatementGeneratedResponse;

public class CassandraDao implements IDao<DMLGeneratorResponse> {
  private final String cassandraUrl;
  private final String cassandraUser;
  private final IConnectionHelper connectionHelper;

  public CassandraDao(
      String cassandraUrl, String cassandraUser, IConnectionHelper connectionHelper) {
    this.cassandraUrl = cassandraUrl;
    this.cassandraUser = cassandraUser;
    this.connectionHelper = connectionHelper;
  }

  @Override
  public void write(DMLGeneratorResponse dmlGeneratorResponse) throws Exception {
    try (CqlSession session =
        (CqlSession)
            connectionHelper.getConnection(this.cassandraUrl)) { // Ensure connection is obtained
      if (session == null) {
        throw new ConnectionException("Connection is null");
      }
      PreparedStatementGeneratedResponse preparedStatementGeneratedResponse =
          (PreparedStatementGeneratedResponse) dmlGeneratorResponse;
      String dmlStatement = preparedStatementGeneratedResponse.getDmlStatement();
      PreparedStatement preparedStatement = session.prepare(dmlStatement);
      BoundStatement boundStatement =
          preparedStatement.bind(
              preparedStatementGeneratedResponse.getValues().stream()
                  .map(v -> CassandraTypeHandler.castToExpectedType(v.dataType(), v.value()))
                  .toArray());
      session.execute(boundStatement);
    }
  }

  public ResultSet readMetadata(String keyspace) throws Exception {
    if (keyspace == null || keyspace.isEmpty()) {
      throw new IllegalArgumentException("Keyspace name cannot be null or empty.");
    }

    String query =
        "SELECT table_name, column_name, type, kind FROM system_schema.columns WHERE keyspace_name = ?";

    try (CqlSession session = (CqlSession) connectionHelper.getConnection(this.cassandraUrl)) {
      if (session == null) {
        throw new ConnectionException("Failed to establish a connection.");
      }

      PreparedStatement preparedStatement = session.prepare(query);
      BoundStatement boundStatement = preparedStatement.bind(keyspace);
      return session.execute(boundStatement);
    }
  }
}
