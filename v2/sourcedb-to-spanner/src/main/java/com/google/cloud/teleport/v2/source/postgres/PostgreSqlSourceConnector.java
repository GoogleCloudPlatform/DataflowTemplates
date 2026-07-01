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
package com.google.cloud.teleport.v2.source.postgres;

import com.google.cloud.teleport.v2.source.jdbc.JdbcSchemaReference;
import com.google.cloud.teleport.v2.source.jdbc.JdbcSourceConnector;
import com.google.cloud.teleport.v2.source.jdbc.iowrapper.config.JdbcIOWrapperConfig;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.StringUtils;

/** PostgreSQL Source Connector. */
public class PostgreSqlSourceConnector extends JdbcSourceConnector {

  private static final String DEFAULT_POSTGRESQL_NAMESPACE = "public";

  @Override
  public JdbcIOWrapperConfig.Builder getJdbcIOWrapperConfigBuilder() {
    return JdbcIOWrapperConfig.builderWithPostgreSQLDefaults();
  }

  @Override
  public SourceSchemaReference getSourceSchemaReference(String dbName, String namespace) {
    JdbcSchemaReference.Builder builder = JdbcSchemaReference.builder();
    if (StringUtils.isBlank(namespace)) {
      builder.setNamespace(DEFAULT_POSTGRESQL_NAMESPACE);
    } else {
      builder.setNamespace(namespace);
    }
    return SourceSchemaReference.ofJdbc(builder.setDbName(dbName).build());
  }

  @Override
  public void setDataSourceLoginTimeout(BasicDataSource dataSource, long timeoutMs) {
    dataSource.setMaxWaitMillis(timeoutMs);
    String connectivityTimeout = String.valueOf(timeoutMs / 1000);
    setConnectionProperty(dataSource, "loginTimeout", connectivityTimeout);
    setConnectionProperty(dataSource, "connectTimeout", connectivityTimeout);
    setConnectionProperty(dataSource, "socketTimeout", connectivityTimeout);
  }

  @Override
  public String getJdbcUrl(
      String host,
      int port,
      String dbName,
      String namespace,
      String connectionProperties,
      Integer fetchSize,
      String sourceDbURL) {
    if (sourceDbURL == null) {
      sourceDbURL = "jdbc:postgresql://" + host + ":" + port + "/" + dbName;
    }
    SourceSchemaReference sourceSchemaReference = getSourceSchemaReference(dbName, namespace);
    sourceDbURL = sourceDbURL + "?currentSchema=" + sourceSchemaReference.jdbc().namespace();
    if (StringUtils.isNotBlank(connectionProperties)) {
      sourceDbURL = sourceDbURL + "&" + connectionProperties;
    }
    return sourceDbURL;
  }
}
