/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.it.gcp.datastream;

import com.google.cloud.datastream.v1.MysqlDatabase;
import com.google.cloud.datastream.v1.MysqlRdbms;
import com.google.cloud.datastream.v1.MysqlSourceConfig;
import com.google.cloud.datastream.v1.MysqlTable;

/**
 * Client for MySQL resource used by Datastream.
 *
 * <p>Subclass of {@link JDBCSource}.
 */
public class MySQLSource extends JDBCSource {

  MySQLSource(Builder builder) {
    super(builder);
  }

  @Override
  public SourceType type() {
    return SourceType.MYSQL;
  }

  @Override
  public MysqlSourceConfig config() {
    MysqlSourceConfig.Builder configBuilder = MysqlSourceConfig.newBuilder();
    if (this.allowedTables().size() > 0) {
      MysqlRdbms.Builder mysqlRdmsBuilder = MysqlRdbms.newBuilder();
      for (String db : this.allowedTables().keySet()) {
        MysqlDatabase.Builder mysqlDbBuilder = MysqlDatabase.newBuilder().setDatabase(db);
        for (String table : this.allowedTables().get(db)) {
          mysqlDbBuilder.addMysqlTables(MysqlTable.newBuilder().setTable(table));
        }
        mysqlRdmsBuilder.addMysqlDatabases(mysqlDbBuilder);
      }
      configBuilder.setIncludeObjects(mysqlRdmsBuilder);
    }
    return configBuilder.build();
  }

  public static Builder builder(String hostname, String username, String password, int port) {
    return new Builder(hostname, username, password, port);
  }

  /** Builder for {@link MySQLSource}. */
  public static class Builder extends JDBCSource.Builder<MySQLSource> {
    public Builder(String hostname, String username, String password, int port) {
      super(hostname, username, password, port);
    }

    @Override
    public MySQLSource build() {
      return new MySQLSource(this);
    }
  }
}
