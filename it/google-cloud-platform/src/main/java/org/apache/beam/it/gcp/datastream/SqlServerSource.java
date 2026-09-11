/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.it.gcp.datastream;

import com.google.cloud.datastream.v1.SqlServerChangeTables;
import com.google.cloud.datastream.v1.SqlServerRdbms;
import com.google.cloud.datastream.v1.SqlServerSchema;
import com.google.cloud.datastream.v1.SqlServerSourceConfig;
import com.google.cloud.datastream.v1.SqlServerTable;

/**
 * Client for SQL Server resource used by Datastream.
 *
 * <p>Subclass of {@link JDBCSource}.
 */
public class SqlServerSource extends JDBCSource {

  private final String database;

  SqlServerSource(Builder builder) {
    super(builder);
    this.database = builder.database;
  }

  @Override
  public SourceType type() {
    return SourceType.SQLSERVER;
  }

  public String database() {
    return this.database;
  }

  @Override
  public SqlServerSourceConfig config() {
    SqlServerSourceConfig.Builder configBuilder = SqlServerSourceConfig.newBuilder();
    if (!this.allowedTables().isEmpty()) {
      SqlServerRdbms.Builder rdbmsBuilder = SqlServerRdbms.newBuilder();
      for (String schema : this.allowedTables().keySet()) {
        SqlServerSchema.Builder schemaBuilder = SqlServerSchema.newBuilder().setSchema(schema);
        for (String table : this.allowedTables().get(schema)) {
          schemaBuilder.addTables(SqlServerTable.newBuilder().setTable(table));
        }
        rdbmsBuilder.addSchemas(schemaBuilder);
      }
      configBuilder.setIncludeObjects(rdbmsBuilder);
    }
    configBuilder.setChangeTables(SqlServerChangeTables.getDefaultInstance());
    return configBuilder.build();
  }

  public static Builder builder(
      String hostname, String username, String password, int port, String database) {
    return new Builder(hostname, username, password, port, database);
  }

  /** Builder for {@link SqlServerSource}. */
  public static class Builder extends JDBCSource.Builder<SqlServerSource> {
    private final String database;

    public Builder(String hostname, String username, String password, int port, String database) {
      super(hostname, username, password, port);
      this.database = database;
    }

    @Override
    public SqlServerSource build() {
      return new SqlServerSource(this);
    }
  }
}
