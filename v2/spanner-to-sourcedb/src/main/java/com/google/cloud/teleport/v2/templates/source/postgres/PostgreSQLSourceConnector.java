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
package com.google.cloud.teleport.v2.templates.source.postgres;

import com.google.cloud.teleport.v2.spanner.migrations.connection.ConnectionHelperRequest;
import com.google.cloud.teleport.v2.spanner.migrations.connection.IConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.connection.JdbcConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.sourceddl.PostgreSQLInformationSchemaScanner;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.IDao;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.JdbcDao;
import com.google.cloud.teleport.v2.templates.dbutils.dml.IDMLGenerator;
import com.google.cloud.teleport.v2.templates.dbutils.processor.ISourceConnector;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class PostgreSQLSourceConnector implements ISourceConnector {

  private final IConnectionHelper connectionHelper = new JdbcConnectionHelper();

  @Override
  public IDMLGenerator getDmlGenerator() {
    return new PostgreSQLDMLGenerator();
  }

  @Override
  public IConnectionHelper getConnectionHelper() {
    return connectionHelper;
  }

  @Override
  public String getConnectionUrl(Shard shard) {
    return "jdbc:postgresql://"
        + shard.getHost()
        + ":"
        + shard.getPort()
        + "/"
        + shard.getDbName();
  }

  @Override
  public IDao getDao(Shard shard) {
    return new JdbcDao(getConnectionUrl(shard), shard.getUserName(), getConnectionHelper());
  }

  @Override
  public void initConnectionHelper(List<Shard> shards, int maxConnections) {
    if (!connectionHelper.isConnectionPoolInitialized()) {
      ConnectionHelperRequest request =
          new ConnectionHelperRequest(
              shards,
              null,
              maxConnections,
              "org.postgresql.Driver",
              null,
              "jdbc:postgresql://");
      connectionHelper.init(request);
    }
  }

  @Override
  public SourceSchema getSourceSchema(Shard shard) {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(getConnectionUrl(shard));
    config.setUsername(shard.getUserName());
    config.setPassword(shard.getPassword());
    config.setDriverClassName("org.postgresql.Driver");
    try (HikariDataSource ds = new HikariDataSource(config);
         Connection connection = ds.getConnection()) {
      return new PostgreSQLInformationSchemaScanner(
              connection, shard.getDbName(), shard.getNamespace())
          .scan();
    } catch (SQLException e) {
      throw new RuntimeException("Sql error while discovering postgres schema: ", e);
    }
  }

  @Override
  public void validateNotReadOnly(List<Shard> shards) {
    for (Shard shard : shards) {
      HikariConfig config = new HikariConfig();
      config.setJdbcUrl(getConnectionUrl(shard));
      config.setUsername(shard.getUserName());
      config.setPassword(shard.getPassword());
      config.setDriverClassName("org.postgresql.Driver");
      try (HikariDataSource ds = new HikariDataSource(config);
           Connection conn = ds.getConnection()) {
        if (conn != null) {
          try (Statement stmt = conn.createStatement();
              ResultSet rs = stmt.executeQuery("SELECT current_setting('transaction_read_only')")) {
            if (rs != null && rs.next() && "on".equalsIgnoreCase(rs.getString(1))) {
              throw new RuntimeException(
                  "PostgreSQL destination is in read-only mode for shard: " + shard.getLogicalShardId());
            }
          }
        }
      } catch (SQLException e) {
        throw new RuntimeException("Error checking PostgreSQL read-only status for shard: " + shard.getLogicalShardId(), e);
      }
    }
  }

  @Override
  public boolean isShardingSupported() {
    return true;
  }
}
